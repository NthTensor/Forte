//! This module contains the api and worker logic for the Forte thread pool.

use alloc::format;
use alloc::vec::Vec;
use core::array;
use core::borrow::Borrow;
use core::cell::Cell;
use core::cmp;
use core::future::Future;
use core::hint::cold_path;
use core::marker::PhantomData;
use core::num::NonZero;
use core::pin::pin;
use core::ptr;
use core::ptr::NonNull;
use core::task::Context;
use core::task::Poll;

use async_task::Runnable;
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use st3::StealError;
use st3::lifo::Stealer;
use st3::lifo::Worker as Sharer;

use crate::FnOnceMarker;
use crate::FutureMarker;
use crate::job::HeapJob;
use crate::job::JobQueue;
use crate::job::JobRef;
use crate::job::StackJob;
use crate::latch::Latch;
use crate::latch::Semaphore;
use crate::latch::Status;
use crate::platform::*;
use crate::scope::Scope;
use crate::scope::with_scope;
use crate::time::ticks;
use crate::unwind;
use crate::util::IterBits;
use crate::util::XorShift64Star;

// -----------------------------------------------------------------------------
// Thread pool

/// A statically-allocated handle to a dynamically-sized collection of threads.
///
/// Each `ThreadPool` must be stored in a `static`, ideally defined within your
/// root binary crate rather than a library crate. You can create a new pool
/// with [`ThreadPool::new`], and will probably want to resize sometime between
/// program init and when you want to start scheduling work.
///
/// ```rust
/// # use forte::ThreadPool;
/// static POOL: ThreadPool = ThreadPool::new();
///
/// fn main() {
///     POOL.resize_to_available();
///     // … schedule work …
///     POOL.depopulate();
/// }
/// ```
///
/// A pool can accommodate at most 32 participating threads (this includes
/// managed worker threads created by the `resize` functions, but also external
/// threads that become "temporary members" when they make blocking calls to the
/// pool). All blocking methods (e.g. [`join`] and [`scope`]) work even with
/// zero managed workers, but they won't run in parallel.
pub struct ThreadPool {
    /// Shared data for each pool member.
    member_data: Lazy<MemberData>,
    /// A queue of data
    shared_queue: SegQueue<JobRef>,
    /// Manages how members leave the pool.
    ///
    /// * The 26 least significant bit count the number of ongoing broadcasts.
    ///
    /// * The 6 most significant bits count the number of ongoing resignations.
    ///
    /// Members can only resign when there are no ongoing broadcasts. Broadcasts
    /// can only begin after all current resignations complete.
    resignations: CachePadded<AtomicU32>,
    /// A bitmask that tracks which members are waiting to resign.
    wants_to_resign: CachePadded<AtomicU32>,
    /// A bitmask that tracks which member indices are claimed.
    claimed_bitmask: CachePadded<AtomicU32>,
    /// A bitmask that tracks which members are waiting on their semaphore
    /// signal.
    waiting_bitmask: CachePadded<AtomicU32>,
    /// Holds controls for threads spawned and managed by the pool. Initialized
    /// on first call to `activate`, to allow for some non-static constructors.
    managed_workers: Mutex<Vec<ManagedWorker>>,
}

/// A public interface that can be temporarily claimed and used by a thread.
/// Claiming a seat allows a thread to participate in the thread pool as a
/// worker.
pub struct MemberData {
    /// The sharing side of each seat's work-stealing queue. These should only
    /// ever be accessed by the thread that currently owns the lease for this
    /// seat (to ensure the `!Sync` bound is respected).
    sharers: [Sharer<JobRef>; 32],
    /// The stealing side of each seat's work-stealing queue.
    stealers: [Stealer<JobRef>; 32],
    /// A set of queues used for transmitting work that must be executed on a
    /// particular worker. Used for broadcasts and cross-thread nonsend worker
    /// wakeups.
    pub broadcasts: [SegQueue<JobRef>; 32],
    /// A binary semaphore for each seat, used for signaling.
    pub semaphores: [Semaphore; 32],
}

impl MemberData {
    fn new() -> MemberData {
        let sharers: [Sharer<JobRef>; 32] =
            array::from_fn(|_| Sharer::new(Worker::STEAL_QUEUE_CAPACITY));
        let stealers: [Stealer<JobRef>; 32] =
            array::from_fn(|i| sharers[i].stealer());
        let broadcasts = array::from_fn(|_| SegQueue::new());
        let semaphores = array::from_fn(|_| Semaphore::new());
        MemberData {
            sharers,
            stealers,
            broadcasts,
            semaphores,
        }
    }
}

// SAFETY: The only `!Sync` field of `MemberData` is `sharers`. This is a
// `Sharer` (aka `st3::Worker`).
//
// Each `sharers[i]` is only ever touched by the single thread that currently
// holds seat `i`, and seat ownership is exclusive. Ownership thus moves between
// threads one at a time, exactly as if the `Sharer` (which is `Send`) were
// sent.
//
// The handoff is synchronized: a resigning worker makes a `Release` store to
// the `claimed_bitmask`, and a joining worker makes an `Acquire` load of the
// same variable. So the next owner of the seat sees a consistent `Sharer`.
unsafe impl Sync for MemberData {}

/// Represents a worker thread that is managed by the pool, as opposed to
/// external threads which temporarily participate in the pool.
struct ManagedWorker {
    /// The index of this worker in the public worker info list.
    member_index: usize,
    /// Controls used to manage the lifecycle of the worker.
    control: ThreadControl,
}

/// Used to manage the lifecycle of a thread.
struct ThreadControl {
    /// Tells the thread to shut down when set to true.
    halt: Arc<AtomicBool>,
    /// The handle used to wait for the thread to complete.
    handle: JoinHandle<()>,
}

// -----------------------------------------------------------------------------
// Thread pool creation and utilities

#[allow(clippy::new_without_default)]
impl ThreadPool {
    /// Creates a new thread pool.
    pub const fn new() -> ThreadPool {
        // Create the pool itself.
        ThreadPool {
            member_data: Lazy::new(MemberData::new),
            shared_queue: SegQueue::new(),
            resignations: CachePadded::new(AtomicU32::new(0)),
            claimed_bitmask: CachePadded::new(AtomicU32::new(0)),
            waiting_bitmask: CachePadded::new(AtomicU32::new(0)),
            wants_to_resign: CachePadded::new(AtomicU32::new(0)),
            managed_workers: Mutex::new(Vec::new()),
        }
    }

    /// Returns an opaque identifier for this thread pool.
    #[inline(always)]
    pub fn id(&'static self) -> usize {
        // We can rely on `self` not to change since it's a static ref.
        ptr::from_ref(self) as usize
    }

    /// Returns the number of members participating in the pool.
    #[inline(always)]
    pub fn num_members(&'static self) -> usize {
        self.claimed_bitmask.load(Ordering::Relaxed).count_ones() as usize
    }

    /// Adds a job to the thread-pool's shared queue.
    ///
    /// This allows adding work from outside the pool (eg, without a reference
    /// to a `Worker`).
    ///
    /// Note: Workers only take work from this queue as a last resort, after all
    /// their other work has been exhausted.
    #[inline(always)]
    pub fn push_shared_job(&'static self, job_ref: JobRef) {
        self.shared_queue.push(job_ref);
        // Try to wake up a worker to execute this job. This is relatively cheap
        // if no workers are waiting.
        let waiting_bitmask = self.waiting_bitmask.load(Ordering::Relaxed);
        if waiting_bitmask != 0 {
            let i = waiting_bitmask.trailing_zeros() as usize;
            self.get_member_data().semaphores[i].signal();
        }
    }
}

// -----------------------------------------------------------------------------
// Thread pool resizing

impl ThreadPool {
    /// Resizes the thread pool to fill (almost) all available cores. After this
    /// returns, the pool will have between 1 and 32 workers. Returns the new
    /// size of the pool.
    ///
    /// This always leaves one core free, so that the main program loop can
    /// continue executing on it. If you have 8 cores, calling this function
    /// will add 7 workers to the pool (and then the main thread will become the
    /// 8th worker if it makes a blocking call like `join`).
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn resize_to_available(&'static self) -> usize {
        let mut available =
            available_parallelism().map(NonZero::get).unwrap_or(1);
        available = available.saturating_sub(1).max(1);
        self.resize_to(available)
    }

    /// Resizes the pool to the specified number of threads. Returns the new
    /// size of the thread pool. The new size may be smaller than requested if
    /// all the seats in the thread pool are occupied.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn resize_to(&'static self, new_size: usize) -> usize {
        self.resize(|_| new_size)
    }

    /// Adds the given number of threads to the thread pool. Returns the new
    /// size of the pool. The new size may be smaller than requested if all the
    /// seats in the thread pool are occupied.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn grow(&'static self, added_threads: usize) -> usize {
        self.resize(|current_size| current_size.saturating_add(added_threads))
    }

    /// Removes the given number of threads from the thread pool. Returns the new
    /// size of the pool.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn shrink(&'static self, terminated_threads: usize) -> usize {
        self.resize(|current_size| {
            current_size.saturating_sub(terminated_threads)
        })
    }

    /// Ensures that there is at least one worker thread attached to the thread
    /// pool. This is mostly used to avoid deadlocks. This should be called
    /// before blocking on a thread pool to ensure the block will eventually be
    /// released. Returns the new size of the pool, which will be either the old
    /// size or one.
    ///
    /// See [`ThreadPool::resize_to`] for more information about resizing.
    pub fn populate(&'static self) -> usize {
        self.resize(
            |current_size| {
                if current_size == 0 { 1 } else { current_size }
            },
        )
    }

    /// Removes all worker threads from the thread pool. This should only be
    /// done carefully, as blocking on an empty pool can cause a deadlock.
    ///
    /// See [`ThreadPool::resize_to`] for more information about resizing.
    pub fn depopulate(&'static self) -> usize {
        self.resize_to(0)
    }

    /// Resizes the pool, and returns the new size. The new size may be smaller
    /// than requested if all the seats in the thread pool are occupied.
    #[cold]
    pub fn resize<F>(&'static self, get_size: F) -> usize
    where
        F: Fn(usize) -> usize,
    {
        // Resizing a pool is a critical section; only one thread can resize the
        // pool at a time. This is implemented using a mutex on the thread manager.
        let mut managed_workers = self.managed_workers.lock().unwrap();

        // Compute the new size of the pool, given the current size.
        let current_size = managed_workers.len();

        // Calculate the new size of the pool (counting only managed workers).
        let new_size = get_size(current_size);

        match new_size.cmp(&current_size) {
            // The size remained the same
            cmp::Ordering::Equal => current_size,
            // The size increased
            cmp::Ordering::Greater => {
                // Spawn the new workers.
                let memberships = self.try_enroll_many(new_size - current_size);
                for membership in memberships {
                    let member_index = membership.member_index;
                    let halt = Arc::new(AtomicBool::new(false));
                    let worker_halt = halt.clone();
                    let handle = ThreadBuilder::new()
                        .name(format!("managed worker {member_index}"))
                        .spawn(move || {
                            managed_worker(membership, worker_halt);
                        })
                        .unwrap();
                    let control = ThreadControl { halt, handle };
                    managed_workers.push(ManagedWorker {
                        member_index,
                        control,
                    });
                }

                managed_workers.len()
            }
            // The size decreased
            cmp::Ordering::Less => {
                // Pull the workers we intend to halt out of the thread manager.
                let terminating_workers = managed_workers.split_off(new_size);

                // Terminate and wake the workers.
                let member_data = self.get_member_data();
                for worker in &terminating_workers {
                    // Tell the worker to halt.
                    worker.control.halt.store(true, Ordering::Relaxed);
                    // Signal the worker.
                    member_data.semaphores[worker.member_index].signal();
                }

                // Drop the lock on the state so as not to block the workers or
                // heartbeat.
                drop(managed_workers);

                // Determine our seat index.
                let own_member_index =
                    Worker::map_current(|worker| worker.member_index);

                // Wait for the other workers to fully halt.
                for worker in terminating_workers {
                    // It's possible we may be trying to terminate ourselves, in
                    // which case we can skip the thread-join.
                    if Some(worker.member_index) != own_member_index {
                        let _ = worker.control.handle.join();
                    }
                }

                new_size
            }
        }
    }
}

// -----------------------------------------------------------------------------
// Thread pool worker access

impl ThreadPool {
    /// Returns this thread's worker if it is a member of this thread pool.
    #[inline(always)]
    pub fn get_worker<F, R>(&'static self, func: F) -> R
    where
        F: FnOnce(Option<&Worker>) -> R,
    {
        Worker::with_current(|worker| match worker {
            Some(worker) if worker.thread_pool.id() == self.id() => {
                func(Some(worker))
            }
            _ => func(None),
        })
    }

    /// Returns this thread's worker. If not already a member of this thread
    /// pool, this thread will try to enroll itself as a member.
    ///
    /// Note: If the thread pool is full (it already has 32 active members) this
    /// waits for a vacancy before returning.
    #[inline(always)]
    pub fn with_worker<F, R>(&'static self, func: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        self.get_worker(|worker| match worker {
            Some(worker) => func(worker),
            None => self.with_worker_cold(func),
        })
    }

    /// The cold branch of `with_worker`.
    ///
    /// Requests membership in the thread pool, and then activates that
    /// membership to get a new local worker handle. If the thread pool is full
    /// (there are already 32 members) this blocks.
    #[cold]
    fn with_worker_cold<F, R>(&'static self, func: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        let membership = self.enroll();
        membership.activate(func)
    }
}

// -----------------------------------------------------------------------------
// Schedulers

pub trait Scheduler<'w>: Send + Sync {
    // Is passed the result of `Worker::with_current`
    fn schedule(&self, job_ref: JobRef, worker: Option<&'w Worker>);
}

impl<'w, F> Scheduler<'w> for F
where
    F: Fn(JobRef, Option<&'w Worker>) + Send + Sync,
{
    #[inline(always)]
    fn schedule(&self, job_ref: JobRef, worker: Option<&'w Worker>) {
        self(job_ref, worker)
    }
}

// -----------------------------------------------------------------------------
// Spawn Trait

pub use async_task::Task;

/// A trait for types that can be spawned onto a [`ThreadPool`].
///
/// It is implemented for:
///
/// * Closures that satisfy `for<'worker> FnOnce(&'worker Worker) + Send + 'static`.
///
/// * Futures that satisfy `Future<Output = T> + Send + 'static` where `T: Send + 'static`.
///
/// Closures return `()` when spawned, but futures return a [`Task`].
///
/// # Compile Errors
///
/// Due to a bug in rustc, you may be given errors when using closures
/// with inferred types. If you encounter the following:
///
/// ```compile_fail
/// # use forte::ThreadPool;
/// # use forte::Worker;
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// THREAD_POOL.spawn(|_| { });
/// //                ^^^^^^^ ERROR: the trait `Spawn<'_, _>` is not implemented for closure ...
/// ```
/// Try adding a type hint to the closure's parameters, like so:
/// ```
/// # use forte::ThreadPool;
/// # use forte::Worker;
/// # static THREAD_POOL: ThreadPool = ThreadPool::new();
/// THREAD_POOL.spawn(|_: &Worker| { });
/// ```
/// Hopefully rustc will fix this type inference failure eventually.
pub trait Spawn<M>: 'static {
    /// The handle returned when spawning this type.
    type Output: 'static;

    /// Turns `Self` into a `JobRef` and passes it to the `Scheduler`.
    ///
    /// # Safety
    ///
    /// If `Self` is not `Send`, the `scheduler` must only ever schedule the
    /// work to run on the current thread. For a future, this includes every
    /// reschedule triggered by a wakeup, no matter which thread the wakeup
    /// occurs on.
    unsafe fn spawn<S>(
        self,
        scheduler: S,
        worker: Option<&Worker>,
    ) -> Self::Output
    where
        S: for<'w> Scheduler<'w> + 'static;
}

impl<F> Spawn<FnOnceMarker> for F
where
    F: for<'worker> FnOnce(&'worker Worker) + 'static,
{
    type Output = ();

    #[inline]
    unsafe fn spawn<S>(self, scheduler: S, worker: Option<&Worker>)
    where
        S: for<'w> Scheduler<'w> + 'static,
    {
        // Allocate a new job on the heap to store the closure.
        let job = HeapJob::new(self);

        // Turn the job into an "owning" `JobRef` so it can be queued.
        //
        // SAFETY: `HeapJob::into_job_ref` requires:
        //
        // * The `JobRef` does not outlive any of the data closed over by the
        //   closure. `F: 'static`, so the closure captures only `'static` data,
        //   which outlives any `JobRef`.
        //
        // * If the closure is `!Send`, `JobRef::execute` is only called on the
        //   thread where the `HeapJob` was constructed.
        //
        //   The safety contract of `Spawn<M>::spawn` makes the caller
        //   responsible for ensuring this.
        let job_ref = unsafe { job.into_job_ref() };

        // Schedule the job-ref.
        scheduler.schedule(job_ref, worker);
    }
}

/// Executes a raw pointer to a runnable.
///
/// # Safety
///
/// The caller must ensure:
///
/// * `this` was produced by `Runnable::into_raw` and must not have been
///   consumed by a call to `from_raw`.
///
/// * If the `Runnable` was created for a `!Send` future, this must only be
///   called on the thread where the `Runnable` was created.
#[inline(always)]
unsafe fn execute_runnable(this: NonNull<()>, _worker: &Worker) {
    // SAFETY: `Runnable::from_raw` must be given a pointer produced by
    // `Runnable::into_raw` that has not already been consumed by a `from_raw`.
    //
    // The caller ensures this, according to the first clause of this function's
    // safety contract.
    let runnable = unsafe { Runnable::<()>::from_raw(this) };
    // Poll the task. This will drop the future if the task is
    // canceled or the future completes.
    runnable.run();
}

impl<Fut, T> Spawn<FutureMarker> for Fut
where
    Fut: Future<Output = T> + 'static,
    T: 'static,
{
    type Output = Task<T>;

    #[inline]
    unsafe fn spawn<S>(self, scheduler: S, _worker: Option<&Worker>) -> Task<T>
    where
        S: for<'w> Scheduler<'w> + 'static,
    {
        // Turn our `JobRef` scheduler into something that `async-task` knows how
        // to use.
        let schedule_task = move |runnable: Runnable| {
            // Temporarily turn the task into a raw pointer so that it can be
            // used as a job. We could also use `HeapJob` here, but since
            // `Runnable` is heap allocated this would result in a needless
            // second allocation.
            let job_pointer = runnable.into_raw();

            // SAFETY: `JobRef::new` requires us to show that `JobRef::execute`
            // will only be called on the returned `JobRef` when it would be
            // sound to call `execute_runnable` on `job_pointer`. This requires:
            //
            // * That `job_pointer` was produced by `Runnable::into_raw` and not
            //   yet consumed by `Runnable::from_raw`. We produced it with
            //   `into_raw` immediately above. The only call to `from_raw` is
            //   inside `execute_runnable`, which runs at most once because
            //   `JobRef::execute` consumes the `JobRef`.
            //
            // * If the future is `!Send`, `execute_runnable` is only called on
            //   the thread the future was spawned on.
            //
            //   The second clause of the `Spawn<M>::spawn` safety contract
            //   requires the caller to ensure this.
            let job_ref = unsafe { JobRef::new(job_pointer, execute_runnable) };

            // Schedule the job-ref, looking up the current worker so woken
            // futures tend to run on the thread that woke them.
            Worker::with_current(|worker| scheduler.schedule(job_ref, worker));
        };

        // Create a runnable for the future.
        //
        // SAFETY: `spawn_unchecked` has four obligations:
        //
        // * If `Self` is `!Send`, its `Runnable` must be used and dropped on
        //   the original thread. The `Runnable` is wrapped in a `JobRef` and
        //   handed to `scheduler`.
        //
        //   The second clause of the `Spawn<M>::spawn` safety contract
        //   requires the caller to ensure this.
        //
        // * If `Self` is `!'static`, borrowed variables must outlive its
        //   `Runnable`.
        //
        //   Vacuously true, since `Self: 'static`.
        //
        // * If `schedule_task` is `!Send`/`!Sync`, all of the `Runnable`'s
        //   `Waker`s must be used and dropped on the original thread.
        //
        //   Vacuously true, since `S: Scheduler` is `Send + Sync` so too
        //   must `schedule_task` be `Send + Sync`.
        //
        // * If `schedule_task` is `!'static`, borrowed variables must outlive
        //   all of the `Runnable`'s `Waker`s.
        //
        //   Vacuously true, since `S: 'static`.
        let (runnable, task) =
            unsafe { async_task::spawn_unchecked(self, schedule_task) };

        // Perform the initial schedule via the task's own stored schedule
        // function, pushing a `JobRef` for the future onto the current worker's
        // queue. If the future doesn't complete, it can be woken and scheduled
        // again later.
        runnable.schedule();

        // Return the task.
        task
    }
}

// -----------------------------------------------------------------------------
// Broadcasts

/// Context object for [`broadcast`](Worker::broadcast) operations.
pub struct Broadcast<'w> {
    /// The worker this part of the broadcast is running on. This will be in
    /// `[0, participants)`.
    pub worker: &'w Worker,
    /// The index of this worker within the broadcast. The return value will be
    /// stored at this index within the results vector.
    pub index: usize,
    /// The number of threads participating in the broadcast.
    pub participants: usize,
}

// -----------------------------------------------------------------------------
// Thread pool operations

impl ThreadPool {
    /// Spawns a job into the thread pool.
    ///
    /// See also: [`Worker::spawn`] and [`spawn`].
    #[inline(always)]
    pub fn spawn<M, S>(&'static self, work: S) -> S::Output
    where
        S: Spawn<M> + Send,
        S::Output: Send,
    {
        let scheduler = |job_ref, worker: Option<&Worker>| match worker {
            Some(worker) => worker.fifo_queue.push_new(job_ref),
            None => self.push_shared_job(job_ref),
        };
        Worker::with_current(|worker| {
            // SAFETY: `Spawn::spawn`'s contract requires that `!Send` work is
            // only scheduled to run on this thread. The work is `Send` here, so
            // this is vacuous.
            unsafe { work.spawn(scheduler, worker) }
        })
    }

    /// Tries to spawn a job on a specific member index. If no thread currently
    /// holds that membership, the work will not be run.
    #[inline(always)]
    pub fn spawn_on<M, S>(
        &'static self,
        member_index: usize,
        work: S,
    ) -> S::Output
    where
        S: Spawn<M> + Send,
        S::Output: Send,
    {
        let member_data = self.get_member_data();
        let scheduler = move |job_ref, _: Option<&Worker>| {
            member_data.broadcasts[member_index].push(job_ref);
            member_data.semaphores[member_index].signal();
        };
        // SAFETY: `Spawn::spawn`'s contract requires that `!Send` work is only
        // scheduled to run on this thread. The work is `Send` here, so this is
        // vacuous.
        unsafe { work.spawn(scheduler, None) }
    }

    /// Blocks the thread waiting for a future to complete.
    ///
    /// See also: [`Worker::block_on`] and [`block_on`].
    #[inline(always)]
    pub fn block_on<F, T>(&'static self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        self.get_worker(|worker| match worker {
            Some(worker) => worker.block_on(future),
            None => futures_lite::future::block_on(future),
        })
    }

    /// Executes the two closures, possibly in parallel.
    ///
    /// See also: [`Worker::join`] and [`join`].
    #[inline(always)]
    pub fn join<A, B, RA, RB>(&'static self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&Worker) -> RA + Send,
        B: FnOnce(&Worker) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        self.with_worker(|worker| worker.join(a, b))
    }

    /// Creates a scope onto which non-static work can be spawned.
    ///
    /// See also: [`Worker::scope`] and [`scope`].
    #[inline(always)]
    pub fn scope<'env, F, T>(&'static self, f: F) -> T
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
    {
        self.with_worker(|worker| worker.scope(f))
    }

    /// Runs the same closure on several threads, and returns a vector of
    /// results.
    ///
    /// See also: [`Worker::broadcast`] and [`broadcast`]. If you don't care
    /// about getting results back, you may want to use
    /// [`ThreadPool::spawn_broadcast`] instead.
    #[inline(always)]
    pub fn broadcast<F, T>(&'static self, f: F) -> Vec<T>
    where
        F: for<'w> Fn(Broadcast<'w>) -> T + Sync,
        T: Send,
    {
        self.with_worker(|worker| worker.broadcast(f))
    }

    /// Runs the same closure on sevearl threads, without waiting for them to
    /// complete.
    ///
    /// See also: [`Worker::spawn_broadcast`] and [`spawn_broadcast`]. If you
    /// care about getting results back, you may want to use
    /// [`ThreadPool::broadcast`] instead.
    #[inline(always)]
    pub fn spawn_broadcast<F>(&'static self, f: F)
    where
        F: for<'w> Fn(Broadcast<'w>) + Send + Sync + 'static,
    {
        self.with_worker(|worker| worker.spawn_broadcast(f));
    }
}

// -----------------------------------------------------------------------------
// Worker registration

/// Represents membership in a thread-pool.
///
/// Provided by [`ThreadPool::enroll`].
pub struct Membership {
    /// The thread pool the worker is registered with.
    thread_pool: &'static ThreadPool,
    /// Contains the index of a row in the `MembersData` table, if the worker
    /// has been granted membership on the thread-pool.
    pub(crate) member_index: usize,
    /// A reference to the `MemberData` table.
    pub(crate) member_data: &'static MemberData,
}

impl ThreadPool {
    /// Returns member data, initializing it on the first call.
    pub fn get_member_data(&'static self) -> &'static MemberData {
        self.member_data.get()
    }

    /// Waits for membership in the thread-pool.
    ///
    /// If the thread-pool is full (it has 32 members) this blocks.
    pub fn enroll(&'static self) -> Membership {
        loop {
            match self.try_enroll() {
                // If we receive a membership, break out of the loop
                Some(membership) => return membership,
                // If the thread-pool is full, wait for a membership to become
                // free
                None => atomic_wait::wait(&self.claimed_bitmask, u32::MAX),
            }
        }
    }

    /// Requests membership in a thread-pool.
    ///
    /// If the thread-pool is full (it has 32 members) this returns `None`.
    #[cold]
    pub fn try_enroll(&'static self) -> Option<Membership> {
        loop {
            let available_bitmask =
                !self.claimed_bitmask.load(Ordering::Acquire);
            if available_bitmask == 0 {
                return None;
            }
            let enrolled_index = available_bitmask.trailing_zeros() as usize; // TZCNT
            let enrolled_bitmask = 1 << enrolled_index;
            if self
                .claimed_bitmask
                .fetch_or(enrolled_bitmask, Ordering::Relaxed)
                & enrolled_bitmask
                == 0
            {
                return Some(Membership {
                    thread_pool: self,
                    member_index: enrolled_index,
                    member_data: self.get_member_data(),
                });
            }
        }
    }

    /// Creates multiple memberships for the thread pool.
    pub fn try_enroll_many(&'static self, n: usize) -> Vec<Membership> {
        if n == 0 {
            return Vec::new();
        }
        let member_data = self.get_member_data();
        loop {
            let claimed_bitmask = self.claimed_bitmask.load(Ordering::Acquire);
            if claimed_bitmask == u32::MAX {
                return Vec::new();
            }

            // Build a mask of up to `n` free seats by walking the complement.
            let mut enrolled_bitmask = 0;
            let mut available_bitmask = !claimed_bitmask;
            for _ in 0..n {
                if available_bitmask == 0 {
                    break;
                }
                // Isolate the lowest available bit and add it to the enrollment
                // bits
                enrolled_bitmask |=
                    available_bitmask & available_bitmask.wrapping_neg();
                // Remove that bit from the available bits
                available_bitmask &= available_bitmask - 1;
            }

            // Attempt to claim all selected seats in one atomic step.
            if self
                .claimed_bitmask
                .compare_exchange(
                    claimed_bitmask,
                    claimed_bitmask | enrolled_bitmask,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return (0..32)
                    .filter(|&i| enrolled_bitmask & (1 << i) != 0)
                    .map(|seat_number| Membership {
                        thread_pool: self,
                        member_index: seat_number as usize,
                        member_data,
                    })
                    .collect();
            }
        }
    }

    /// Blocks workers from resigning from the pool.
    ///
    /// Each call to this function must be paired with exactly one following
    /// call to `unfreeze_membership`.
    ///
    /// Returns a bitset indicating which memberships are claimed. These members
    /// are guatenteed to remain in the pool at least until the corresponding
    /// call to `unfreeze_membership`.
    pub fn freeze_membership(&'static self) -> u32 {
        // Increment the freeze counter, which will cause new resignation
        // requests to be rejected.
        //
        // Note, this will break if we exceed 67,108,863 simultaneous
        // broadcasts, because we will overflow into the bits for
        // ongoing-resignations. I expect us to always run out of memory before
        // that happens, so this case is not handled.
        let mut resignations =
            self.resignations.fetch_add(1, Ordering::Relaxed);

        // Wait for any ongoing resignations to complete.
        while (resignations >> 26) != 0 {
            atomic_wait::wait(&self.resignations, resignations);
            resignations = self.resignations.load(Ordering::Relaxed);
        }

        // Synchronizes with the `Release` store done by workers when they
        // complete their resignation, ensuring that the following load of
        // `claimed_bitmask` will properly reflect any recent resignations.
        fence(Ordering::Acquire);

        // Return the frozen membership bitmask
        self.claimed_bitmask.load(Ordering::Relaxed)
    }

    /// Unblocks workers from resigning from the pool.
    ///
    /// Each call to this function must be paired with exactly one preceding
    /// call to `unfreeze_membership`.
    pub fn unfreeze_membership(&'static self) {
        // Decrement the freeze counter, to allow new resignations to be
        // accepted.
        let resignations = self.resignations.fetch_sub(1, Ordering::Acquire);

        // If this was the last active freeze, wake up any threads that want to
        // resign.
        if resignations == 1 {
            let wants_to_resign = self.wants_to_resign.load(Ordering::Relaxed);
            for member_index in wants_to_resign.iter_bits() {
                self.get_member_data().semaphores[member_index].signal();
            }
        }
    }
}

thread_local! {
    static WORKER_PTR: Cell<*const Worker> = const { Cell::new(ptr::null()) };
}

const REJECTION_MASK: u32 = (1u32 << 26) - 1;

impl Membership {
    /// Returns this worker's index within its thread pool.
    ///
    /// The index is stable for the lifetime of the membership and is unique
    /// among concurrent members of the same pool.
    #[inline(always)]
    pub fn member_index(&self) -> usize {
        self.member_index
    }

    /// Temporarily sets the thread's worker. [`Worker::with_current`] always
    /// returns a reference to the worker set up by the most recent call to
    /// `activate`.
    ///
    /// Rust's thread locals are fairly costly, so this function is expensive.
    /// If you can avoid calling it, do so.
    #[inline(always)]
    pub fn activate<F, R>(self, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        let worker = Worker {
            migrated: Cell::new(false),
            membership: self,
            fifo_queue: JobQueue::new(),
            lifo_queue: JobQueue::new(),
            nonsend_fifo_queue: Arc::new(SegQueue::new()),
            rng: XorShift64Star::new(),
            last_promote_tick: Cell::new(0),
            _phantom: PhantomData,
        };

        // Swap the local pointer to point to the newly allocated worker.
        let outer_ptr = WORKER_PTR.with(|ptr| ptr.replace(&worker));

        // Run the function within the context created by the worker pointer,
        // and pass in a worker reference directly.
        let result = f(&worker);

        // Indicate that we want to resign.
        worker
            .thread_pool
            .wants_to_resign
            .fetch_or(1 << worker.member_index, Ordering::Relaxed);

        // Wait for all local work to complete, and our resignation to be
        // accepted.
        loop {
            if worker.yield_local() == Yield::Idle {
                // Attempt to submit our resignation
                let mut resignations =
                    worker.thread_pool.resignations.load(Ordering::Relaxed);
                if resignations & REJECTION_MASK == 0 {
                    match worker.thread_pool.resignations.compare_exchange(
                        resignations,
                        resignations + (1 << 26),
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        // Resignation accepted
                        Ok(_) => break,
                        // Resignation rejected due to ongoing broadcast
                        Err(err) if err & REJECTION_MASK != 0 => worker.wait(),
                        // Resignation conflicted with other worker, try again
                        Err(err) => resignations = err,
                    }
                }
            }
        }

        worker
            .thread_pool
            .wants_to_resign
            .fetch_and(!(1 << worker.member_index), Ordering::Relaxed);

        // Drop the worker, which will also free the claimed membership.
        let thread_pool = worker.thread_pool;
        drop(worker);

        // Complete the resignation.
        loop {
            let resignations = thread_pool.resignations.load(Ordering::Relaxed);
            // Try to decrement the resignations count.
            if thread_pool
                .resignations
                .compare_exchange_weak(
                    resignations,
                    resignations - (1 << 26),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                // If this was the last ongoing resignation, wake up any waiting
                // broadcasts.
                if (resignations >> 26) - 1 == 0 {
                    atomic_wait::wake_all(&*thread_pool.resignations);
                }
                // Exit the CAS loop.
                break;
            }
        }

        // Swap back to pointing to the previous value (possibly null).
        WORKER_PTR.with(|ptr| ptr.set(outer_ptr));

        // Return the intermediate values created while running the closure,
        // namely the result and any jobs still remaining on the local queue.
        result
    }

    /// Returns a reference to the push-side `Sharer` queue for this
    /// worker's seat.
    #[inline(always)]
    fn sharing_queue(&self) -> &'static Sharer<JobRef> {
        &self.member_data.sharers[self.member_index]
    }

    /// Returns a reference to a worker's local inbox (where !Send future
    /// wakeups and broadcasts are transmitted).
    #[inline(always)]
    fn broadcast_queue(&self) -> &'static SegQueue<JobRef> {
        &self.member_data.broadcasts[self.member_index]
    }

    /// Returns a reference to a worker's local inbox (where !Send future
    /// wakeups and broadcasts are transmitted).
    #[inline(always)]
    fn semaphore(&self) -> &'static Semaphore {
        &self.member_data.semaphores[self.member_index]
    }

    /// Waits for a signal on this member's semaphore.
    fn wait(&self) {
        let semaphore = self.semaphore();
        semaphore
            .wait(1 << self.member_index, &self.thread_pool.waiting_bitmask);
    }
}

impl Drop for Membership {
    fn drop(&mut self) {
        // Release the claim on this membership.
        self.thread_pool
            .claimed_bitmask
            .fetch_and(!(1 << self.member_index), Ordering::Release);
        // In case another thread is waiting for a membership slot to free
        // up, issue a wake on the bitmask.
        atomic_wait::wake_one(&*self.thread_pool.claimed_bitmask);
    }
}

// -----------------------------------------------------------------------------
// Worker context

/// Represents membership in a thread pool.
///
/// To get access to a worker for a given thread pool, users should call
/// [`ThreadPool::with_worker`].
///
/// Every thread has at most one worker at a time. If a worker has already been
/// set up, it may be accessed at any time by calling [`Worker::with_current`].
/// A thread's worker can also be manually set up by claiming a membership
/// ([`ThreadPool::try_enroll`]) and passing it to [`Membership::activate`]. The
/// worker returned by `with_current` always represents the membership most
/// recently activated in the call stack.
///
/// Every worker belongs to exactly one thread pool, and must hold a membership
/// in one of the shared slots within that pool.
///
/// Workers have one core memory-safety guarantee: Any jobs added to the worker
/// will eventually be executed.
pub struct Worker {
    /// Registers the worker as belonging to a specific thread pool, and
    /// potentially also grants "membership" on that thread-pool.
    pub(crate) membership: Membership,
    /// A sequence of jobs waiting to be executed. Newer jobs are executed
    /// before older ones, allowing efficient depth-first execution. During
    /// promotion, the oldest job is shared. Populated by `join()`.
    ///
    /// Jobs in this queue take precedence over those in the fifo queue.
    lifo_queue: JobQueue,
    /// A sequence of jobs waiting to be executed. Older jobs are executed
    /// before newer ones, providing reliably low latency. During promotion,
    /// this queue is partitioned into chunks and the chunks are shared.
    /// Populated by `spawn()`.
    ///
    /// Jobs in this queue are executed only when the lifo queue is empty.
    pub(crate) fifo_queue: JobQueue,
    /// A sequence of `!Sendf` jobs waitging to be executed. Older jobs are
    /// executed before newer ones.
    ///
    /// This queue does not participate in promotion. This is a `SeqQueue` so
    /// that a `Future` that is `!Send` and has been spawned onto this thread
    /// can be woken on another thread (the other thread then sends this thread
    /// a job that polls the future).
    pub(crate) nonsend_fifo_queue: Arc<SegQueue<JobRef>>,
    /// A local psudorandom number-generator. Used to spread out
    /// worker-to-worker operations evenly across the pool.
    rng: XorShift64Star,
    /// The CPU tick when work was last promoted from local to shared. This has
    /// no absolute relation to time.
    last_promote_tick: Cell<u64>,
    /// Set to true when executing a job that came from a different thread.
    migrated: Cell<bool>,
    // Make non-send. A `Worker` represents the local state of a particular
    // thread, so must be `!Send` and `!Sync`. It is already `!Sync` because of
    // `Cell`.
    _phantom: PhantomData<*const ()>,
}

use core::ops::Deref;

impl Deref for Worker {
    type Target = Membership;

    fn deref(&self) -> &Membership {
        &self.membership
    }
}

/// Describes the outcome of a call to [`Worker::yield_now`] or
/// [`Worker::yield_local`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Yield {
    /// Indicates that a job was executed.
    Executed,
    /// Indicates that no job was executed. After receiving this, do not `yield`
    /// again until you have a reasonable expectation that new work will have
    /// been shared.
    Idle,
}

impl Worker {
    /// Calls the provided closure on the thread's worker instance, if it has
    /// one. If this thread is not registered as a worker, the closure is not
    /// called.
    #[inline(always)]
    pub fn map_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&Worker) -> R,
    {
        let worker_ptr = WORKER_PTR.with(Cell::get);
        if !worker_ptr.is_null() {
            // SAFETY: `WORKER_PTR` is a thread-local `Cell` holding a raw
            // pointer to a `Worker`. It is only written to by
            // `Membership::activate`, which stores the address of a `Worker`
            // allocated within it's own stack frame. Before it returns,
            // `activate` restores the previous value of `WORKER_PTR`, so that
            // it is always either null or points to a live, immovable `Worker`
            // on the current thread's call stack (but is never left dangling).
            //
            // If the pointer is non-null, it is therefore valid to dereference
            // as a shared reference. Forming a `'static` reference is avoided
            // by passing the value into a closure, which bounds the reference's
            // lifetime to the closure body and prevents callers from retaining
            // it past the point where `activate` returns and the `Worker` is
            // freed.
            Some(f(unsafe { &*worker_ptr }))
        } else {
            None
        }
    }

    /// Calls the provided closure on the thread's worker instance, if it has
    /// one. If this thread is not registered as a worker, the closure is passed
    /// `None`.
    #[inline(always)]
    pub fn with_current<F, R>(f: F) -> R
    where
        F: FnOnce(Option<&Worker>) -> R,
    {
        let worker_ptr = WORKER_PTR.with(Cell::get);
        if !worker_ptr.is_null() {
            // SAFETY: `WORKER_PTR` is a thread-local `Cell` holding a raw
            // pointer to a `Worker`. It is only written to by
            // `Membership::activate`, which stores the address of a `Worker`
            // allocated within its own stack frame. Before it returns,
            // `activate` restores the previous value of `WORKER_PTR`, so that
            // it is always either null or points to a live, immovable `Worker`
            // on the current thread's call stack (but is never left dangling).
            //
            // If the pointer is non-null, it is therefore sound to dereference
            // as a shared reference. Forming a `'static` reference is avoided
            // by passing the value into a closure, which bounds the reference's
            // lifetime to the closure body and prevents callers from retaining
            // it past the point where `activate` returns and the `Worker` is
            // freed.
            f(Some(unsafe { &*worker_ptr }))
        } else {
            f(None)
        }
    }

    /// Returns the thread pool this worker belongs to.
    #[inline(always)]
    pub fn thread_pool(&self) -> &'static ThreadPool {
        self.thread_pool
    }

    /// Capacity of the per-worker work-stealing queue. This is the maximum
    /// amount a worker can make available for stealing at once.
    const STEAL_QUEUE_CAPACITY: usize = 32;

    /// The minimum number of CPU ticks between calls to [`Worker::promote_cold`].
    /// Approximately 5μs at 3 GHz.
    const PROMOTE_TICK_INTERVAL: u64 = 15_000;

    /// Try to promote the oldest task in the queue.
    #[inline(always)]
    fn promote(&self) {
        // Promotions are fairly costly, so we limit their frequency using the
        // cpu's instruction counter. Promote is called at a high frequency, and
        // actually doing the promotion is probably a cold path.
        let current_tick = ticks();
        if current_tick.wrapping_sub(self.last_promote_tick.get())
            >= Self::PROMOTE_TICK_INTERVAL
        {
            // This should ideally become a conditional jump.
            self.promote_cold(current_tick);
        }
    }

    /// The actual work-promotion implementation. Must be called infrequently.
    #[cold]
    fn promote_cold(&self, current_tick: u64) {
        // Update the promote tick so that `promote` won't call this again soon.
        self.last_promote_tick.set(current_tick);

        // Early out if it seems like all workers are already awake.
        let waiting_bitmask =
            self.thread_pool().waiting_bitmask.load(Ordering::Relaxed);
        if waiting_bitmask == 0 {
            return;
        }
        cold_path();

        // Track if we actually managed to share work.
        let mut shared_job = false;

        // Share work from the lifo queue. This is shared bit-by-bit, with old
        // (and therefore theoretically "large") tasks shared first.
        if let Some(job_ref) = self.lifo_queue.pop_oldest() {
            // Push into our own steal queue so siblings can steal it.
            if let Err(job_ref) = self.sharing_queue().push(job_ref) {
                // If the queue is full, that indicates that the pool is
                // probably under high-load and we should continue local-first
                // operation.
                self.lifo_queue.push_old(job_ref);
            } else {
                shared_job = true;
            }
        }

        // Share work from the fifo queue. Offload the newest jobs in a series of
        // small chunks.
        for job_refs in self.fifo_queue.split() {
            // Create a new job that will insert a chunk of jobs into the
            // runner's fifo queue when executed.
            //
            // This reduces the cost of sharing a large number of small jobs.
            let batch_job = HeapJob::new(move |worker| {
                worker.fifo_queue.append(job_refs);
            });
            // SAFETY: `HeapJob::into_job_ref` requires:
            //
            // * The data closed over by the `HeapJob` outlives the `JobRef`.
            //
            //   The closure captures `job_refs` (a `VecDeque<JobRef>`) by value,
            //   so it trivially outlives the newly created `JobRef`.
            //
            // * If the closure is `!Send`, `JobRef::execute` only runs on this
            //   thread. The closure is `Send`, so this obligation is vacuous.
            let batch_job_ref = unsafe { batch_job.into_job_ref() };
            // Push the batch job into the steal queue so siblings can steal it.
            if let Err(job_ref) = self.sharing_queue().push(batch_job_ref) {
                // If the queue is full, that indicates that the pool is
                // probably under high-load and we should continue local-first
                // operation.
                //
                // This just adds the jobs back to the local queue.
                self.execute(job_ref, false);
            } else {
                shared_job = true;
            }
        }

        // If we added work to the steal queue, wake a random sibling to steal
        // it from us, while we do other work.
        if shared_job {
            self.signal_random(waiting_bitmask);
        }
    }

    /// Tries to wake a random sleeping worker. Expects to be given a bitset of
    /// sleeping workers.
    #[inline(always)]
    fn signal_random(&self, sleeping: u32) {
        let offset = self.rng.next_usize(32) as u32;
        let mut randomized_sleeping = sleeping.rotate_right(offset);
        while randomized_sleeping != 0 {
            let index = (randomized_sleeping.trailing_zeros() + offset) % 32;
            randomized_sleeping &= randomized_sleeping - 1; // Clear the lowest bit
            let woken =
                self.membership.member_data.semaphores[index as usize].signal();
            if woken {
                return;
            }
        }
    }

    /// Create a new latch owned by the worker.
    #[inline(always)]
    pub fn new_latch(&self) -> Latch {
        Latch::new(self.semaphore())
    }

    /// Runs jobs until the provided latch is set.
    ///
    /// The thread may go to sleep if it runs out of work to do, but will wake
    /// when the latch is set or more work becomes available.
    #[inline(always)]
    pub fn wait_for(&self, latch: &Latch) -> bool {
        loop {
            match latch.check() {
                Status::Pending => {
                    if self.yield_now() == Yield::Idle {
                        let member_bitmask = 1 << self.membership.member_index;
                        let waiting_bitmask =
                            &self.membership.thread_pool.waiting_bitmask;
                        latch.wait(member_bitmask, waiting_bitmask);
                    }
                }
                Status::Ok => return false,
                Status::Error => return true,
            }
        }
    }

    /// Finds a job to work on.
    ///
    /// This function will not steal work from other workers or read from the
    /// global injector queue. It prioritizes work that _must_ be run on this
    /// thread (assuming no other workers are running).
    ///
    /// Work is prioritized as follows:
    /// 1. Pull from the LIFO queue (`join` calls)
    /// 2. Pull from the !Send FIFO queue (`spawn_local` calls)
    /// 3. Pull from the broadcast queue (`broadcast` and `spawn_on` calls)
    /// 4. Pull from the regular FIFO queue (`spawn` calls)
    /// 5. Reclaim work shared by this worker
    #[inline(always)]
    fn find_local_work(&self) -> Option<JobRef> {
        (self.lifo_queue.pop_newest())
            .or_else(|| self.nonsend_fifo_queue.pop())
            .or_else(|| self.broadcast_queue().pop())
            .or_else(|| self.fifo_queue.pop_oldest())
            .or_else(|| self.sharing_queue().pop())
    }

    /// Finds a job to work on.
    ///
    /// Work is prioritized as follows:
    /// 1. Try [`find_local_work`][Worker::find_local_work] first
    /// 2. Steal work shared from other workers
    /// 3. Read from the global queue (external calls)
    ///
    /// If work is found in the last two cases, it is treated as having been
    /// "migrated" to this thread.
    #[inline(always)]
    fn find_work(&self) -> Option<(JobRef, bool)> {
        (self.find_local_work().map(|job| (job, false)))
            .or_else(|| self.steal_from_siblings().map(|job| (job, true)))
            .or_else(|| {
                self.thread_pool.shared_queue.pop().map(|job| (job, true))
            })
    }

    /// Attempts to steal a job from another worker's work-stealing queue.
    ///
    /// Iterates over occupied seats in a random order to avoid always hitting
    /// the same victim. Because stealers are pre-allocated and permanent, no
    /// lock or atomic load is needed to access them.
    #[inline(always)]
    fn steal_from_siblings(&self) -> Option<JobRef> {
        let my_member_index = self.membership.member_index;
        let my_sharer = self.sharing_queue();
        let stealers = &self.membership.member_data.stealers;
        let claimed_bitmask =
            self.thread_pool().claimed_bitmask.load(Ordering::Relaxed);

        // Randomize the starting position so all workers get a fair shot as victims.
        let offset = self.rng.next_usize(32) as u32;
        let mut bits =
            (claimed_bitmask & !(1u32 << my_member_index)).rotate_right(offset);

        while bits != 0 {
            let shifted_idx = bits.trailing_zeros();
            let idx = (shifted_idx + offset) % 32;
            bits &= bits - 1;
            let stealer = &stealers[idx as usize];
            // `steal_and_pop` returns one job directly and moves up to half the
            // remaining items into our steal queue for later use.
            loop {
                match stealer.steal_and_pop(my_sharer, |n| n / 2) {
                    Ok((job, _)) => return Some(job),
                    Err(StealError::Busy) => {} // transient; retry
                    Err(StealError::Empty) => break,
                }
            }
        }
        None
    }

    /// Cooperatively yields execution to the thread pool, allowing it to execute
    /// some work.
    ///
    /// This function will only execute work already held locally by the worker,
    /// and does no synchronization. To claim and run shared work, use
    /// [`yield_now`][Worker::yield_now].
    ///
    /// If no work is found, this returns `Yield::Idle`. This function should
    /// not be called again (for at least a few microseconds) after an idle.
    /// Calling this repeatedly in a spin-loop should be avoided, as it's likely
    /// to significantly spike CPU usage and waste resources.
    #[inline(always)]
    pub fn yield_local(&self) -> Yield {
        // We use LIFO order here, pulling the newest work from the queue. This
        // is just for consistency with yield_now/find_work.
        match self.find_local_work() {
            Some(job_ref) => {
                self.execute(job_ref, false);
                Yield::Executed
            }
            None => Yield::Idle,
        }
    }

    /// Cooperatively yields execution to the thread pool, allowing it to execute
    /// some work.
    ///
    /// If the worker has no local work to do, it will try to steal work from
    /// coworkers or claim work from the shared injection queue. If instead the
    /// worker has a backlog of local work, the worker may make some of it
    /// accessible to other workers for stealing. This involves synchronization
    /// with the pool, and so should be called infrequently. To yield without
    /// synchronizing with the pool, use [`yield_local`][Worker::yield_local].
    ///
    /// If no work is found, this returns `Yield::Idle`. This function should
    /// not be called again (for at least a few microseconds) after an idle.
    /// Calling this repeatedly in a spin-loop should be avoided, as it's likely
    /// to significantly spike CPU usage and waste resources.
    #[inline(always)]
    pub fn yield_now(&self) -> Yield {
        self.promote();
        match self.find_work() {
            Some((job_ref, migrated)) => {
                self.execute(job_ref, migrated);
                Yield::Executed
            }
            None => Yield::Idle,
        }
    }

    /// Returns `true` if the current job is executing on a different thread
    /// from the one on which it was created. Returns `false` if not executing a
    /// job, or if the current job was created on the current thread.
    #[inline(always)]
    pub fn migrated(&self) -> bool {
        self.migrated.get()
    }

    /// Executes a job. This wrapper swaps in the correct thread-migration flag
    /// before the job runs, then swaps it back to what it was before.
    #[inline(always)]
    fn execute(&self, job_ref: JobRef, migrated: bool) {
        let outer_migrated = self.migrated.replace(migrated);
        job_ref.execute(self);
        self.migrated.set(outer_migrated);
    }
}

// -----------------------------------------------------------------------------
// Worker operations

impl Worker {
    /// Runs work (a closure or future) in the background.
    ///
    /// Just like a standard thread, this work executes concurrently (and
    /// potentially in parallel) to the place where it is spawned. It is not
    /// tied to the current stack frame, and hence it cannot hold any references
    /// other than those with `'static` lifetime. If you want to spawn a task
    /// that references stack data, use the [`scope`], [`ThreadPool::scope`] or
    /// [`Worker::scope`] functions.
    ///
    /// Since tasks spawned with this function cannot hold references into the
    /// enclosing stack frame, you almost certainly want to use a move closure
    /// as their argument (otherwise, the closure will typically hold references
    /// to any variables from the enclosing function that you happen to use).
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::spawn`] or simply [`spawn`].
    ///
    /// # Panics
    ///
    /// The panic behavior depends on the type of work being spawned:
    ///
    /// * If a closure panics, it will be caught and ignored.
    ///
    /// * If a future panics, the [`Task`] will panic when awaited.
    ///
    #[inline]
    pub fn spawn<M, S>(&self, work: S) -> S::Output
    where
        S: Spawn<M> + Send,
        S::Output: Send,
    {
        let thread_pool = self.thread_pool;
        let scheduler = |job_ref, worker: Option<&Worker>| match worker {
            Some(worker) => worker.fifo_queue.push_new(job_ref),
            None => thread_pool.push_shared_job(job_ref),
        };
        // SAFETY: `Spawn::spawn`'s contract requires that `!Send` work only be
        // scheduled to run on this thread. The work is `Send` and this is
        // vacuous.
        unsafe { work.spawn(scheduler, Some(self)) }
    }

    /// Runs work (a closure or future) on the background of a specific worker
    /// thread.
    ///
    /// This is quite similar to [`spawn`](Worker::spawn), except that the work
    /// will run on another worker instead of the current one.
    ///
    /// # Panics
    ///
    /// The panic behavior depends on the type of work being spawned:
    ///
    /// * If a closure panics, it will be caught and ignored.
    ///
    /// * If a future panics, the [`Task`] will panic when awaited.
    #[inline]
    pub fn spawn_on<M, S>(&self, member_index: usize, work: S) -> S::Output
    where
        S: Spawn<M> + Send,
        S::Output: Send,
    {
        self.thread_pool.spawn_on(member_index, work)
    }

    /// Runs work (a closure or future) in the background of this thread.
    ///
    /// This is quite similar to [`spawn`](Worker::spawn), except that the work
    /// may be `!Send` and will only run on the current thread. If your work is
    /// `Send`, consider using [`spawn`](Worker::spawn) instead.
    ///
    /// # Panics
    ///
    /// The panic behavior depends on the type of work being spawned:
    ///
    /// * If a closure panics, it will be caught and ignored.
    ///
    /// * If a future panics, the [`Task`] will panic when awaited.
    ///
    #[inline]
    pub fn spawn_local<M, S>(&self, work: S) -> S::Output
    where
        S: Spawn<M>,
    {
        let queue = self.nonsend_fifo_queue.clone();
        let semaphore = &self.member_data.semaphores[self.member_index];
        let scheduler = move |job_ref, _: Option<&Worker>| {
            queue.push(job_ref);
            semaphore.signal();
        };
        // SAFETY: `Spawn::spawn`'s contract requires that any `!Send` work be
        // scheduled to run on this thread.
        //
        // The scheduler pushes the job onto this worker's `nonsend_fifo_queue`,
        // which is only ever drained by this worker on this thread (see
        // `find_local_work`). For a future, every reschedule re-pushes onto
        // that same queue regardless of which thread issued the wakeup, so the
        // work stays on this thread.
        unsafe { work.spawn(scheduler, Some(self)) }
    }

    /// Polls a future to completion, then returns the outcome.
    ///
    /// This function will prioritize polling the future as soon as it becomes
    /// available, and while the future is not available it will try to do other
    /// meaningful work from the thread-pool. If the thread pool runs out of
    /// work, the thread is suspended until the future completes or more
    /// background work becomes available.
    ///
    /// # Async & Concurrency
    ///
    /// This is a convenient way to introduce concurrency into otherwise blocking
    /// operations. For example, it is _totally acceptable_ to use `block_on`
    /// within one of the branches of of a `join` operation (to perform I/O, for
    /// example).
    ///
    /// This should **not** be called within `async` contexts. While it will not
    /// block the execution of work on the pool, it will prevent the enclosing
    /// future's `poll` method from returning. This can potentially lead to
    /// deadlocks.
    ///
    /// Other implementation of `block_on` (like those defined by the `futures`
    /// crate) should not be called within parallel forte operations. They will
    /// block execution of work on the pool.
    ///
    /// # Alternatives
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::block_on`] instead. If you don't have a static reference
    /// to a specific thread pool (as is often the case in library code) you can
    /// use [`block_on`] instead, as long as you are sure that your code will
    /// run within a worker.
    ///
    /// # Panics
    ///
    /// If the future panics, this panics.
    #[inline(always)]
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        // Create a new latch to block the thread until the future completes.
        //
        // This is allocated on the heap, even though the worker is allocated on
        // the stack, because we can't prevent futures from keeping wakers
        // around for arbitrary amounts of time or issuing wakeups for futures
        // that have completed.
        let latch = Arc::new(self.new_latch());
        let waker = latch.clone().into();
        // Put the waker into an async context that can be used to poll futures.
        let mut ctx = Context::from_waker(&waker);
        // Pin the future, promising not to move it while it's being polled.
        let mut future = pin!(future);
        // Execute other jobs while we wait for the future to complete.
        loop {
            match future.as_mut().poll(&mut ctx) {
                // While the future is incomplete, run other tasks or sleep.
                Poll::Pending => {
                    // This will not return until the latch is set.
                    self.wait_for(latch.borrow());
                    // We want to keep using the same latch every time we wait
                    // for the future to become ready, so we have to reset it
                    // here.
                    //
                    // The latch must be in the set state because we just waited
                    // for it.
                    latch.reset();
                }
                // When it is complete, pull out the result and return it.
                Poll::Ready(res) => return res,
            }
        }
    }

    /// Executes the two closures, possibly in parallel.
    ///
    /// This is conceptually similar to spawning two threads to execute each
    /// closure, and then joining both (although the implementation is quite
    /// different). It is intended for implementing recursive,
    /// divide-and-conquer algorithms where each branch may itself call `join`.
    ///
    /// # Examples
    ///
    /// This example (taken wholesale from `rayon`) uses `join` to perform a
    /// quick-sort.
    ///
    /// ```rust
    /// # use forte::*;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.resize_to_available();
    ///
    /// let mut v = vec![5, 1, 8, 22, 0, 44];
    /// THREAD_POOL.with_worker(|worker| quick_sort(worker, &mut v));
    /// assert_eq!(v, vec![0, 1, 5, 8, 22, 44]);
    ///
    /// fn quick_sort<T: PartialOrd + Send>(worker: &Worker, v: &mut [T]) {
    ///     if v.len() > 1 {
    ///         let mid = partition(v);
    ///         let (lo, hi) = v.split_at_mut(mid);
    ///         worker.join(|w| quick_sort(w, lo),
    ///                     |w| quick_sort(w, hi));
    ///     }
    /// }
    ///
    /// // Partition rearranges all items `<=` to the pivot
    /// // item (arbitrary selected to be the last item in the slice)
    /// // to the first half of the slice. It then returns the
    /// // "dividing point" where the pivot is placed.
    /// fn partition<T: PartialOrd + Send>(v: &mut [T]) -> usize {
    ///     let pivot = v.len() - 1;
    ///     let mut i = 0;
    ///     for j in 0..pivot {
    ///         if v[j] <= v[pivot] {
    ///             v.swap(i, j);
    ///             i += 1;
    ///         }
    ///     }
    ///     v.swap(i, pivot);
    ///     i
    /// }
    /// ```
    ///
    /// This example (taken from `chili`) shows how to use `join` to sum the
    /// nodes of a binary tree.
    ///
    /// ```rust
    /// # use forte::*;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.resize_to_available();
    ///
    /// let tree = gen_tree(8);
    /// let result = THREAD_POOL.with_worker(|worker| sum(worker, &tree));
    /// assert_eq!(result, 255);
    ///
    /// struct Node {
    ///     val: u64,
    ///     left: Option<Box<Node>>,
    ///     right: Option<Box<Node>>,
    /// }
    ///
    /// fn gen_tree(layers: usize) -> Box<Node> {
    ///     Box::new(Node {
    ///         val: 1,
    ///         left: (layers != 1).then(|| gen_tree(layers - 1)),
    ///         right: (layers != 1).then(|| gen_tree(layers - 1)),
    ///     })
    /// }
    ///
    /// fn sum(worker: &Worker, node: &Node) -> u64 {
    ///     let (left, right) = worker.join(
    ///         |w| node.left.as_deref().map(|n| sum(w, n)).unwrap_or_default(),
    ///         |w| node.right.as_deref().map(|n| sum(w, n)).unwrap_or_default(),
    ///     );
    ///     node.val + left + right
    /// }
    /// ```
    ///
    /// # Alternatives
    ///
    /// If you do not have a `Worker`, you can use [`ThreadPool::join`]
    /// instead. If you don't have a static reference to a specific thread pool
    /// (as is often the case in library code) you can use [`join`] instead, as
    /// long as you are sure that your code will run within a worker.
    ///
    /// If your workload isn't amenable to the divide-and-conquer approach or is
    /// async, but you still want to borrow local data in your computations, you
    /// may want to use a [`scope`][`Worker::scope`] instead.
    ///
    /// # Warning about blocking I/O
    ///
    /// The assumption is that the closures given to `join()` are CPU-bound
    /// tasks that do not perform blocking operations. If you do perform I/O,
    /// and that I/O should block (e.g., waiting for a network request), the
    /// overall performance may be poor. Moreover, if you cause one closure to
    /// be blocked waiting on another (for example, using a channel), that could
    /// lead to a deadlock.
    ///
    /// You can use [`block_on`][Worker::block_on] to do async I/O within a
    /// `join` branch, as long as different branches are not made to depend on
    /// each other.
    ///
    /// # Panics
    ///
    /// Both closures are always executed to completion. If either panics,
    /// `join` will propagate that panic after both complete. When both panic,
    /// only the panic from the first argument is propagated and the panic from
    /// the other argument is dropped (this may cause program aborts in some
    /// situations).
    #[inline(always)]
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&Worker) -> RA + Send,
        B: FnOnce(&Worker) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        // Allocate a job to run the closure `a` on the stack. It is vital to
        // the correctness of this function that this stack-job never move until
        // it is freed.
        let stack_job = StackJob::new(a, self.new_latch());

        // SAFETY: We are only allowed to create a `JobRef` to this `StackJob`
        // if we can show that...
        //
        // * `as_job_ref` is called at most once for this `StackJob`.
        //
        //   The `StackJob` is only accessible in this function (it is
        //   created here, dropped here, and no direct references escape
        //   this scope), and within this function we only call `as_job_ref`
        //   once.
        //
        // * The `StackJob` will not be moved or dropped until either:
        //
        //   A. A call to `check` on the enclosed `Latch` returns something
        //      other than `Pending`.
        //
        //   B. The `JobRef` is dropped without `execute` being called.
        //
        //   If `recover_newest` returns `true`, then the `JobRef` must have
        //   been dropped without `execute` being called (satisfying B).
        //
        //   If `recover_newest` returns `false`, then we call `wait_for`, which
        //   will not allow the function to progress until `check` returns
        //   something other than `Pending` (satisfying A).
        //
        //   In either case, we cannot move or drop the `StackJob` until we pass
        //   the branch marked with "(*)". We clearly do not.
        let job_ref = unsafe { stack_job.as_job_ref() };

        // Store the id of the `JobRef` for later, when we will need it to
        // safely recover the closure `a` for inline execution.
        let job_ref_id = job_ref.id();

        // Push the job onto the queue.
        self.lifo_queue.push_new(job_ref);

        // If we have received a heartbeat, we remove the oldest item in the
        // local queue and push it into the shared queue. This causes work to be
        // shared in "breadth-first" order (as opposed to the "depth-first"
        // order we use when executing).
        self.promote();

        // Run the second closure directly.
        let result_a;
        let result_b = unwind::halt_unwinding(|| b(self));

        // Attempt to recover the job from the queue. It should still be there
        // if we didn't share it.
        if self.lifo_queue.recover_newest(job_ref_id) {
            // (*)
            // SAFETY: Because the ids match, the JobRef we just popped from the
            // queue must point to `stack_job`, implying that `stack_job` cannot
            // have been executed yet, and `JobRef::execute` will never be
            // called.
            let a = unsafe { stack_job.unwrap_func() };
            // Execute the closure directly and return the results. This is
            // allows the compiler to inline and optimize `a`.
            result_a = unwind::halt_unwinding(|| a(self));
        } else {
            // Wait for the job to complete.
            if self.wait_for(stack_job.completion_latch()) {
                // SAFETY: Since `wait_for` returned `true`, a `check` must have
                // returned `Error`.
                let error = unsafe { stack_job.unwrap_error() };
                result_a = Err(error);
            } else {
                // SAFETY: Since `wait_for` returned `false`, a `check` must have
                // returned `Ok`.
                let output = unsafe { stack_job.unwrap_output() };
                result_a = Ok(output);
            }
        }

        // Resume unwinding if either job panicked.
        match (result_a, result_b) {
            (Err(error), _) | (_, Err(error)) => {
                unwind::resume_unwinding(error)
            }
            (Ok(value_a), Ok(value_b)) => (value_a, value_b),
        }
    }

    /// Creates a new scope for spawning non-static work.
    ///
    /// Work spawned onto the new scope does not have to have a `'static`
    /// lifetime, and can borrow local variables. Local borrowing is possible
    /// because this function will not return until all work spawned on the
    /// scope has completed, this ensuring the stack frame is kept alive for the
    /// duration.
    ///
    /// # Accessing stack data
    ///
    /// In general, spawned tasks may borrow any stack data that lives outside
    /// the scope closure.
    ///
    /// ```
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn(|_: &Worker| {
    ///         // Transfer ownership of `bad` into a local variable (also named `bad`).
    ///         // This will force the closure to take ownership of `bad` from the environment.
    ///         let bad = bad;
    ///         println!("ok: {:?}", ok); // `ok` is only borrowed.
    ///         println!("bad: {:?}", bad); // refers to our local variable, above.
    ///     });
    ///
    ///     scope.spawn(|_: &Worker| println!("ok: {:?}", ok)); // we too can borrow `ok`
    /// });
    /// # });
    /// ```
    /// As the comments example above suggest, to reference `bad` we must
    /// take ownership of it. One way to do this is to detach the closure
    /// from the surrounding stack frame, using the `move` keyword. This
    /// will cause it to take ownership of *all* the variables it touches,
    /// in this case including both `ok` *and* `bad`:
    ///
    /// ```rust
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn(move |_: &Worker| {
    ///         println!("ok: {:?}", ok);
    ///         println!("bad: {:?}", bad);
    ///     });
    ///
    ///     // That closure is fine, but now we can't use `ok` anywhere else,
    ///     // since it is owned by the previous task:
    ///     // scope.spawn(|_: &Worker| println!("ok: {:?}", ok));
    /// });
    /// # });
    /// ```
    ///
    /// While this works, it could be a problem if we want to use `ok`
    /// elsewhere. There are two choices. We can keep the closure as a `move`
    /// closure, but instead of referencing the variable `ok`, we create a
    /// shadowed variable that is a borrow of `ok` and capture *that*:
    ///
    /// ```rust
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     let ok: &Vec<i32> = &ok; // shadow the original `ok`
    ///     scope.spawn(move |_: &Worker| {
    ///         println!("ok: {:?}", ok); // captures the shadowed version
    ///         println!("bad: {:?}", bad);
    ///     });
    ///
    ///     // Now we too can use the shadowed `ok`, since `&Vec<i32>` references
    ///     // can be shared freely. Note that we need a `move` closure here though,
    ///     // because otherwise we'd be trying to borrow the shadowed `ok`,
    ///     // and that doesn't outlive `scope`.
    ///     scope.spawn(move |_: &Worker| println!("ok: {:?}", ok));
    /// });
    /// # });
    /// ```
    ///
    /// Another option is not to use the `move` keyword but instead to take
    /// ownership of individual variables:
    ///
    /// ```rust
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn(|_: &Worker| {
    ///         // Transfer ownership of `bad` into a local variable (also named `bad`).
    ///         // This will force the closure to take ownership of `bad` from the environment.
    ///         let bad = bad;
    ///         println!("ok: {:?}", ok); // `ok` is only borrowed.
    ///         println!("bad: {:?}", bad); // refers to our local variable, above.
    ///     });
    ///
    ///     scope.spawn(|_: &Worker| println!("ok: {:?}", ok)); // we too can borrow `ok`
    /// });
    /// # });
    /// ```
    ///
    /// # Referencing the scope
    ///
    /// The scope passed into the closure is not allowed to leak out of this
    /// call. In other words, this will fail to compile:
    ///
    /// ```compile_fail
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let mut leak = None;
    /// forte::scope(|scope| {
    ///     leak = Some(scope); // <-- ERROR: scope would be leaked here
    /// });
    /// drop(leak);
    /// # });
    /// ```
    ///
    /// Anything spawned onto the scope can capture a reference to it.
    /// This allows scoped work to spawn other scoped work.
    ///
    /// ```
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.with_worker(|worker| {
    /// let mut counter = 0;
    /// let counter_ref = &mut counter;
    /// forte::scope(|scope| {
    ///     scope.spawn(|worker: &Worker| {
    ///         *counter_ref += 1;
    ///         // Note: we borrow the scope again here.
    ///         scope.spawn(move |_: &Worker| {
    ///             *counter_ref += 1;
    ///         });
    ///     });
    /// });
    /// assert_eq!(counter, 2);
    /// # });
    /// ```
    ///
    /// It's possible to spawn non-scoped work within the closure, but these
    /// generally can't hold references to the scope. So for example, the
    /// following also fails to compile:
    ///
    /// ```compile_fail,E0521
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// THREAD_POOL.with_worker(|worker| {
    ///     worker.scope(|scope| {
    ///         worker.spawn(|worker: &Worker| {
    ///             // ^^^^^ ERROR: This creates a *static* job on the worker,
    ///             //       which may outlive the scope.
    ///             
    ///             scope.spawn(|_: &Worker| { });
    ///             // ^^^^^ ERROR: This requires borrowing the scope within the
    ///             //       unscoped job, which isn't allowed by the compiler
    ///             //       because 'scope would have to to outlive 'static.
    ///         });
    ///     });
    /// });
    /// ```
    ///
    /// # Alternatives
    ///
    /// If you do not have a `Worker`, you can use [`ThreadPool::scope`]
    /// instead. If you don't have a static reference to a specific thread pool
    /// (as is often the case in library code) you can use [`scope`] instead, as
    /// long as you are sure that your code will run within a worker.
    ///
    /// Scopes are a more flexible building block compared to
    /// [`join`][Worker::join], since a loop can be used to spawn any number of
    /// tasks without recursing. However, that flexibility comes at a
    /// performance price: tasks spawned using `scope` must be allocated onto
    /// the heap, whereas [`join`][Worker::join] can make exclusive use of the
    /// stack. Prefer [`join`][Worker::join]) where possible.
    ///
    /// # Panics
    ///
    /// If a panic occurs, either in the closure given to `scope` or in job
    /// spawned on the scope, that panic is caught and stored. When all the work
    /// on the scope is complete, `scope` will then re-emit that panic. If
    /// multiple panics occurs, the first will propagate and the others will be
    /// caught and dropped (which may result in program aborts).
    #[inline(always)]
    pub fn scope<'env, F, T>(&self, f: F) -> T
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
    {
        with_scope(self, f)
    }

    /// Runs the same operation across multiple threads, and returns a vector of
    /// results.
    ///
    /// Each worker receives a [`Broadcast`] struct, telling it how many threads
    /// are participating in the broadcast, and it's index among those
    /// participants. The operation may return a different result on each
    /// thread, and those results are collected into a vector, ordered according
    /// to broadcast index. If you don't care about getting results back,
    /// consider using [`spawn_broadcast`](Worker::spawn_broadcast) instead.
    ///
    /// Broadcasts execute after they have completed their local work queues,
    /// but before they attempt to steal work from other threads. Forte will
    /// generally try to run the broadcast on as many threads as possible, but
    /// is not guaranteed to actually use all of them.
    ///
    /// While a broadcast is running, workers are temporarally forbidden from
    /// leaving the pool.
    ///
    /// # Panics
    ///
    /// If the operation panics on one or more threads, exactly one panic will
    /// be propagated, only after all threads have completed (or themselves
    /// panicked).
    #[inline(always)]
    pub fn broadcast<F, T>(&self, f: F) -> Vec<T>
    where
        F: for<'w> Fn(Broadcast<'w>) -> T + Sync,
        T: Send,
    {
        // Prevent workers from leaving the pool, and read the membership bitset
        // once it's frozen.
        let members = self.thread_pool.freeze_membership();
        let participants = members.count_ones() as usize;

        // Create a new stack job for every member.
        let jobs: Vec<_> = members
            .iter_bits()
            .enumerate()
            .map(|(i, member_index)| {
                let func = &f;
                let op = move |worker: &Worker| {
                    func(Broadcast {
                        worker,
                        index: i,
                        participants,
                    })
                };
                (member_index, StackJob::new(op, self.new_latch()))
            })
            .collect();

        // Send the broadcast to each member, and wake them up.
        for (member_index, job) in &jobs {
            // SAFETY: We are only allowed to create a `JobRef` for this
            // `StackJob` if we can show that...
            //
            // * `as_job_ref` is called at most once for this `StackJob`.
            //
            //   The `StackJob` is only accessible in this function (it is
            //   created here, dropped here, and no direct references escape
            //   this scope), and within this function we only call `as_job_ref`
            //   once.
            //
            // * The `StackJob` will not be moved or dropped until a call to
            //   `check` on the enclosed `Latch` returns something other than
            //   `Pending`.
            //
            //   We call `wait_for` on each job's latch (marked with a *). This
            //   does not allow the function to progress while `check` returns
            //   `Pending`. No `StackJob` is moved or dropped until after this
            //   function has been called on every `StackJob` and returned.
            let job_ref = unsafe { job.as_job_ref() };
            self.member_data.broadcasts[*member_index].push(job_ref);
            self.member_data.semaphores[*member_index].signal();
        }

        // Wait for each job to finish.
        let error_flags: Vec<_> = jobs
            .iter()
            .map(|(_, job)| self.wait_for(job.completion_latch())) // (*)
            .collect();

        // Allow workers to leave the pool again.
        self.thread_pool.unfreeze_membership();

        // Collect and return results or propagate panics.
        jobs.into_iter()
            .zip(error_flags)
            .map(|((_, job), error_flag)| {
                if error_flag {
                    // SAFETY: If `error_flag` is `true` then `check` has
                    // returned `Error`.
                    let error = unsafe { job.unwrap_error() };
                    unwind::resume_unwinding(error);
                } else {
                    // SAFETY: If `error_flag` is `false` then `check` has
                    // returned `Ok`.
                    unsafe { job.unwrap_output() }
                }
            })
            .collect()
    }

    /// Runs the same operation across multiple threads, without waiting for
    /// results.
    ///
    /// Like [`broadcast`](Worker::broadcast), except it does not allow the
    /// operation to return a result, and does not wait for the operation to
    /// complete before continuing.
    ///
    /// # Panics
    ///
    /// Panics are not propagated.
    #[inline(always)]
    pub fn spawn_broadcast<F>(&self, f: F)
    where
        F: for<'w> Fn(Broadcast<'w>) + Send + Sync + 'static,
    {
        // Prevent workers from leaving the pool, and read the membership bitset
        // once it's frozen.
        let members = self.thread_pool.freeze_membership();
        let participants = members.count_ones() as usize;

        // Prevent a deadlock if there are no workers. This should be
        // impossible, but we will be defensive.
        if participants == 0 {
            cold_path();
            self.thread_pool.unfreeze_membership();
            return;
        }

        // Wrap the operation in an `Arc` so each per-member job can own an
        // independent, `'static` reference to it. We must *not* let a job
        // capture a borrow of `f`: `spawn_broadcast` does not wait, so `f`
        // would be dropped while jobs are still queued or running on other
        // threads.
        let f = Arc::new(f);

        // Send the broadcast to each member, and wake them up.
        for (i, member_index) in members.iter_bits().enumerate() {
            let func = Arc::clone(&f);
            let op = move |worker: &Worker| {
                // Run the job
                (*func)(Broadcast {
                    worker,
                    index: i,
                    participants,
                });
            };

            let job = HeapJob::new(op);

            // SAFETY: `HeapJob::into_job_ref` has two preconditions:
            //
            // * The `JobRef` must not outlive any of the items closed over by
            //   `op`.
            //
            //   `op` owns an `Arc<F>` clone (`func`). Since `F: 'static`, that
            //   `Arc` — and everything it keeps alive — is itself `'static`,
            //   so it outlives the `JobRef` regardless of when the job runs.
            //
            // * If `F: !Send` then the `JobRef` must only be executed on this
            //   thread.
            //
            //   `op` is `Send` (`Arc<F>: Send` because `F: Send + Sync`), so
            //   this does not apply.
            let job_ref = unsafe { job.into_job_ref() };
            self.member_data.broadcasts[member_index].push(job_ref);
            self.member_data.semaphores[member_index].signal();
        }

        // Once we have finished pushing jobs out to workers who we know are not
        // in the middle of resginging, we can allow resignations again.
        self.thread_pool.unfreeze_membership();
    }
}

// -----------------------------------------------------------------------------
// Implicit worker registration api

/// A [`ThreadPool`] wrapper, used by the [`DEFAULT_POOL`].
///
/// This dereferences to a [`ThreadPool`]. The first time it is dereferenced, it
/// resizes itself to fill all available cores.
pub struct DefaultThreadPool {
    thread_pool: &'static ThreadPool,
    initialized: AtomicU32,
}

impl Deref for DefaultThreadPool {
    type Target = ThreadPool;

    fn deref(&self) -> &'static ThreadPool {
        if self.initialized.swap(1, Ordering::Relaxed) == 0 {
            self.thread_pool.resize_to_available();
        };
        self.thread_pool
    }
}

static DEFAULT_POOL_INNER: ThreadPool = ThreadPool::new();

/// The default thread pool.
///
/// Unless you set up your own thread pool, this is where your operations run.
/// The first time this is dereferenced, it resizes itself to fill all available
/// cores.
pub static DEFAULT_POOL: DefaultThreadPool = DefaultThreadPool {
    thread_pool: &DEFAULT_POOL_INNER,
    initialized: AtomicU32::new(0),
};

/// Runs work in the background.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use [`Worker::spawn`]
/// instead. If you don't have a worker, but know which thread pool you want to
/// use, [`ThreadPool::spawn`] is more appropriate.
pub fn spawn<M, S>(work: S) -> S::Output
where
    S: Spawn<M> + Send,
    S::Output: Send,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.spawn(work),
        None => DEFAULT_POOL.spawn(work),
    })
}

/// Runs work on the background on a specific worker thread.
///
/// This is quite similar to [`spawn`], except that the work will run on another
/// worker instead of the current one.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::spawn_on`] instead. If you don't have a worker, but know which
/// thread pool you want to use, [`ThreadPool::spawn_on`] is more appropriate.
pub fn spawn_on<M, S>(member_index: usize, work: S) -> S::Output
where
    S: Spawn<M> + Send,
    S::Output: Send,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.spawn_on(member_index, work),
        None => DEFAULT_POOL.spawn_on(member_index, work),
    })
}

/// Waits for a future to complete.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::block_on`] instead. If you don't have a worker, but know which
/// thread pool you want to use, [`ThreadPool::block_on`] is more appropriate.
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.block_on(future),
        None => DEFAULT_POOL.block_on(future),
    })
}

/// Executes the two closures, possibly in parallel.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use [`Worker::join`]
/// instead. If you don't have a worker, but know which thread pool you want to
/// use, [`ThreadPool::join`] is more appropriate.
pub fn join<A, B, RA, RB>(a: A, b: B) -> (RA, RB)
where
    A: FnOnce(&Worker) -> RA + Send,
    B: FnOnce(&Worker) -> RB + Send,
    RA: Send,
    RB: Send,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.join(a, b),
        None => DEFAULT_POOL.join(a, b),
    })
}

/// Creates a new scope for spawning non-static work.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::scope`] instead. If you don't have a worker, but know which thread
/// pool you want to use, [`ThreadPool::scope`] is more appropriate.
pub fn scope<'env, F, T>(f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.scope(f),
        None => DEFAULT_POOL.scope(f),
    })
}

/// Runs an operation on multiple threads and returns a vector of results.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::broadcast`] instead. If you don't have a worker, but know which
/// thread pool you want to use, [`ThreadPool::spawn_broadcast`] is more
/// appropriate.
pub fn broadcast<F, T>(f: F) -> Vec<T>
where
    F: for<'w> Fn(Broadcast<'w>) -> T + Sync,
    T: Send,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.broadcast(f),
        None => DEFAULT_POOL.broadcast(f),
    })
}

/// Runs an operation on multiple threads without waiting for results.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::spawn_broadcast`] instead. If you don't have a worker, but know
/// which thread pool you want to use, [`ThreadPool::spawn_broadcast`] is more
/// appropriate.
pub fn spawn_broadcast<F>(f: F)
where
    F: for<'w> Fn(Broadcast<'w>) + Send + Sync + 'static,
{
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.spawn_broadcast(f),
        None => DEFAULT_POOL.spawn_broadcast(f),
    });
}

/// Returns the number of members participating in a thread-pool.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Membership::activate`], [`ThreadPool::with_worker`], or
/// similar) this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If not called within a thread pool, this uses the [`DEFAULT_POOL`].
pub fn num_members() -> usize {
    Worker::with_current(|worker| match worker {
        Some(worker) => worker.thread_pool().num_members(),
        None => DEFAULT_POOL.num_members(),
    })
}

// -----------------------------------------------------------------------------
// Main worker loop

/// This is the main loop for a worker thread. It's in charge of executing jobs.
/// Operating on the principle that you should finish what you start before
/// starting something new, workers will first execute their queue, then execute
/// shared jobs, then pull new jobs from the injector.
fn managed_worker(membership: Membership, halt: Arc<AtomicBool>) {
    // Register as the indicated worker, and work until we are told to halt.
    membership.activate(|worker| {
        while !halt.load(Ordering::Relaxed) {
            if worker.yield_now() == Yield::Idle {
                worker.wait();
            }
        }
    });
}

// -----------------------------------------------------------------------------
// Tests

#[cfg(test)]
mod tests {

    use std::sync::mpsc::channel;
    use std::vec;

    use super::*;

    #[test]
    fn spawn_then_join_in_worker() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let (tx, rx) = channel();
        THREAD_POOL.scope(move |_| {
            spawn(move |_: &Worker| tx.send(22).unwrap());
        });
        assert_eq!(22, rx.recv().unwrap());

        THREAD_POOL.depopulate();
    }

    #[test]
    fn spawn_then_join_outside_worker() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let (tx, rx) = channel();
        THREAD_POOL.spawn(move |_: &Worker| tx.send(22).unwrap());
        assert_eq!(22, rx.recv().unwrap());

        THREAD_POOL.depopulate();
    }

    #[test]
    fn spawn_cancel_safety() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        THREAD_POOL.with_worker(|worker| {
            let _ = worker.spawn(core::future::pending::<()>());
        });

        THREAD_POOL.depopulate();
    }

    #[test]
    fn join_basic() {
        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut a = 0;
        let mut b = 0;
        THREAD_POOL.join(|_| a += 1, |_| b += 1);

        assert_eq!(a, 1);
        assert_eq!(b, 1);

        THREAD_POOL.depopulate();
    }

    #[test]
    fn join_deep() {
        fn increment(worker: &Worker, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (head, tail) = slice.split_at_mut(1);

                    worker.join(
                        |_| head[0] += 1,
                        |worker| increment(worker, tail),
                    );
                }
            }
        }

        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut vals = [0; 800];
        THREAD_POOL.with_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, [1; 800]);

        THREAD_POOL.depopulate();
    }

    #[test]
    fn join_wide() {
        fn increment(worker: &Worker, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let mid = slice.len() / 2;
                    let (left, right) = slice.split_at_mut(mid);

                    worker.join(
                        |worker| increment(worker, left),
                        |worker| increment(worker, right),
                    );
                }
            }
        }

        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut vals = vec![0; 65_536];
        THREAD_POOL.with_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, vec![1; 65_536]);

        THREAD_POOL.depopulate();
    }
}
