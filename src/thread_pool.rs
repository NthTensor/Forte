//! This module contains the api and worker logic for the Forte thread pool.

use alloc::boxed::Box;
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
use tracing::debug;
use tracing::trace;
use tracing::trace_span;

use crate::FnOnceMarker;
use crate::FutureMarker;
use crate::job::ExternalJob;
use crate::job::HeapJob;
use crate::job::JobQueue;
use crate::job::JobRef;
use crate::job::StackJob;
use crate::latch::Latch;
use crate::latch::SleepController;
use crate::platform::*;
use crate::scope::Scope;
use crate::scope::with_scope;
use crate::unwind;
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
    /// A bit-set that tracks which seats are occupied.
    occupied: CachePadded<AtomicU32>,
    /// A bit-set that tracks which seats are sleeping.
    sleeping: CachePadded<AtomicU32>,
    /// Holds shared data for each thread participating in the pool.
    seats: OnceLock<Box<Seats>>,
    /// Holds controls for threads spawned and managed by the pool. Initialized
    /// on first call to `occupy`, to allow for some non-static constructors.
    managed_threads: Mutex<ManagedThreads>,
    /// Used to inject external work into the thread pool. This is generally
    /// treated as a fallback, for when the thread-pool is at capacity and
    /// threads can't register themselves as workers.
    shared_jobs: SegQueue<JobRef>,
}

/// A public interface that can be temporarily claimed and used by a thread.
/// Claiming a seat allows a thread to participate in the thread pool as a
/// worker.
pub(crate) struct Seats {
    /// The sharing side of each seat's work-stealing queue. These should only
    /// ever be accessed by the thread that currently owns the lease for this
    /// seat (to ensure the `!Sync` bound is respected).
    sharers: [Sharer<JobRef>; 32],
    /// The stealing side of each seat's work-stealing queue.
    stealers: [Stealer<JobRef>; 32],
    /// The sleep/wake controller for each seat.
    sleep_controllers: [SleepController; 32],
}

// SAFETY: `stealers` are `Send + Sync` by their own bounds. `workers[i]` is
// only ever accessed by the single thread holding seat `i`'s occupancy lease;
// the `occupied` bitmask in `ThreadPool` enforces that exclusivity.
unsafe impl Sync for Seats {}

/// A lease represents ownership of one of a "seats" in a thread pool, and
/// allows the owning thread to participate in that pool as a worker.
pub struct Lease {
    /// The thread pool against which this lease is held.
    thread_pool: &'static ThreadPool,
    /// The index of the seat in the data list
    seat_number: usize,
    /// A reference to the pre-initialized seat data (to avoid repeated hits of
    /// the `OnceLock`).
    seats: &'static Seats,
}

impl Drop for Lease {
    fn drop(&mut self) {
        // Unset the occupied bit for this seat
        self.thread_pool
            .occupied
            .fetch_and(!(1 << self.seat_number), Ordering::Relaxed);
    }
}

/// Manages threads spawned by the pool.
struct ManagedThreads {
    /// Stores thread controls for workers spawned by the pool.
    workers: Vec<ManagedWorker>,
}

/// Represents a worker thread that is managed by the pool, as opposed to
/// external threads which temporarily participate in the pool.
struct ManagedWorker {
    /// The index of this worker in the public worker info list.
    seat_number: usize,
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
// Thread pool creation and maintenance

#[allow(clippy::new_without_default)]
impl ThreadPool {
    /// Creates a new thread pool.
    pub const fn new() -> ThreadPool {
        // Create the pool itself.
        ThreadPool {
            seats: OnceLock::new(),
            occupied: CachePadded::new(AtomicU32::new(0)),
            sleeping: CachePadded::new(AtomicU32::new(0)),
            managed_threads: Mutex::new(ManagedThreads {
                workers: Vec::new(),
            }),
            shared_jobs: SegQueue::new(),
        }
    }

    /// Returns the pre-allocated steal queues, initializing them on the first call.
    fn get_seats(&'static self) -> &'static Seats {
        self.seats.get_or_init(|| {
            let sharers: [Sharer<JobRef>; 32] =
                array::from_fn(|_| Sharer::new(Worker::STEAL_QUEUE_CAPACITY));
            let stealers: [Stealer<JobRef>; 32] = array::from_fn(|i| sharers[i].stealer());
            let sleep_controllers = array::from_fn(|_| SleepController::new());
            Box::new(Seats {
                sharers,
                stealers,
                sleep_controllers,
            })
        })
    }

    /// Adds a job ref to the shared queue.
    pub fn queue_shared_job(&'static self, job_ref: JobRef) {
        self.shared_jobs.push(job_ref);
    }

    /// Claims a lease on the thread pool which can be occupied by a worker
    /// (using [`Worker::occupy`]), allowing a thread to participate in the pool.
    ///
    /// Returns `None` if all seats are occupied.
    #[cold]
    pub fn claim_lease(&'static self) -> Option<Lease> {
        loop {
            let occupied = self.occupied.load(Ordering::Relaxed);
            if occupied == u32::MAX {
                return None;
            }
            let seat_number = occupied.trailing_ones() as usize;
            let mask = 1 << seat_number;
            if self.occupied.fetch_or(mask, Ordering::Relaxed) & mask == 0 {
                // At this point we have acquired the lease on the seat
                return Some(Lease {
                    thread_pool: self,
                    seat_number,
                    seats: self.get_seats(),
                });
            }
        }
    }

    /// Claims up to `n` leases at once in a single atomic transaction.
    ///
    /// Finds up to `n` free seats, then atomically claims all of them with a
    /// single `compare_exchange`. Either every selected seat is claimed together
    /// or none are (and the loop retries). Returns between 0 and `n` leases;
    /// returns an empty `Vec` when `n` is 0 or the pool is full.
    #[cold]
    pub fn claim_leases(&'static self, n: usize) -> Vec<Lease> {
        if n == 0 {
            return Vec::new();
        }
        let seats = self.get_seats();
        loop {
            let occupied = self.occupied.load(Ordering::Relaxed);
            if occupied == u32::MAX {
                return Vec::new();
            }

            // Build a mask of up to `n` free seats by walking the complement.
            let mut claimed_seats = 0;
            let mut free_seats = !occupied;
            for _ in 0..n {
                if free_seats == 0 {
                    break;
                }
                let seat_bit = free_seats & free_seats.wrapping_neg(); // isolate lowest set bit
                claimed_seats |= seat_bit;
                free_seats &= !seat_bit;
            }

            // Attempt to claim all selected seats in one atomic step.
            match self.occupied.compare_exchange(
                occupied,
                occupied | claimed_seats,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return (0..32)
                        .filter(|&i| claimed_seats & (1 << i) != 0)
                        .map(|seat_number| Lease {
                            thread_pool: self,
                            seat_number: seat_number as usize,
                            seats,
                        })
                        .collect();
                }
                Err(_) => {
                    // Another thread modified `occupied`; retry.
                }
            }
        }
    }

    /// Returns an opaque identifier for this thread pool.
    #[inline(always)]
    pub fn id(&self) -> usize {
        // We can rely on `self` not to change since it's a static ref.
        ptr::from_ref(self) as usize
    }

    /// Returns the number of workers participating in this thread pool.
    #[inline(always)]
    pub fn num_workers(&self) -> usize {
        self.occupied.load(Ordering::Relaxed).count_ones() as usize
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
        let mut available = available_parallelism().map(NonZero::get).unwrap_or(1);
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
        self.resize(|current_size| current_size.saturating_sub(terminated_threads))
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
        debug!("starting thread pool resize");

        // Resizing a pool is a critical section; only one thread can resize the
        // pool at a time. This is implemented using a mutex on the thread manager.
        trace!("locking state");
        let mut managed_threads = self.managed_threads.lock().unwrap();

        // Compute the new size of the pool, given the current size.
        let current_size = managed_threads.workers.len();

        // Calculate the new size of the pool (counting only managed workers).
        let new_size = get_size(current_size);

        trace!(
            "attempting to resize thread pool from {} to {} thread(s)",
            current_size, new_size
        );
        match new_size.cmp(&current_size) {
            // The size remained the same
            cmp::Ordering::Equal => {
                debug!("completed thread pool resize, size unchanged");
                return current_size;
            }
            // The size increased
            cmp::Ordering::Greater => {
                // Spawn the new workers.
                let leases = self.claim_leases(new_size - current_size);
                for lease in leases {
                    let seat_number = lease.seat_number;
                    debug!("spawning managed worker for seat number {}", seat_number);
                    let halt = Arc::new(AtomicBool::new(false));
                    let worker_halt = halt.clone();
                    let handle = ThreadBuilder::new()
                        .name(format!("worker {seat_number}"))
                        .spawn(move || {
                            managed_worker(lease, worker_halt);
                        })
                        .unwrap();
                    let control = ThreadControl { halt, handle };
                    managed_threads.workers.push(ManagedWorker {
                        seat_number,
                        control,
                    });
                }

                drop(managed_threads);
            }
            // The size decreased
            cmp::Ordering::Less => {
                // Pull the workers we intend to halt out of the thread manager.
                let terminating_workers = managed_threads.workers.split_off(new_size);

                // Terminate and wake the workers.
                let seats = self.get_seats();
                for worker in &terminating_workers {
                    // Tell the worker to halt.
                    worker.control.halt.store(true, Ordering::Relaxed);
                    // Wake the worker up.
                    seats.sleep_controllers[worker.seat_number].wake();
                }

                // Drop the lock on the state so as not to block the workers or heartbeat.
                drop(managed_threads);

                // Determine our seat index.
                let own_seat_number = Worker::map_current(|worker| worker.lease.seat_number);

                // Wait for the other workers to fully halt.
                for worker in terminating_workers {
                    // It's possible we may be trying to terminate ourselves, in
                    // which case we can skip the thread-join.
                    if Some(worker.seat_number) != own_seat_number {
                        let _ = worker.control.handle.join();
                    }
                }
            }
        }

        // Return the new size of the thread pool
        new_size
    }
}

// -----------------------------------------------------------------------------
// Thread pool worker access

impl ThreadPool {
    /// Runs the closure on a thread-pool worker.
    ///
    /// If this thread is not a worker, it will try to register itself as one.
    /// If the thread pool is full, the closure is sent to another worker as a
    /// job, and this thread is parked.
    ///
    /// If your closure is `!Send`, use [`with_worker`][ThreadPool::with_worker]
    /// instead.
    #[inline(always)]
    pub fn on_worker<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&Worker) -> R + Send,
        R: Send,
    {
        self.with_worker(|worker| match worker {
            Some(worker) => f(worker),
            None => {
                let mut job = ExternalJob::new(f);
                // SAFETY: `ExternalJob::as_job_ref` requires:
                //
                // * The `ExternalJob` must not move or be deallocated until the
                //   `JobRef` is executed.
                //
                // * The `JobRef` does not outlive any data the `ExternalJob` closes over.
                //
                // * `as_job_ref` is not called again while `JobRef` lives.
                //
                // The `ExternalJob` is a stack-allocated variable. After
                // calling `as_job_ref`, we never move `job`, and we wait for
                // the job to execute by calling `job.wait_for_value`. Only
                // after that returns do we allow the `job` to be dropped. This
                // also means that any data closed over by the `ExternalJob`
                // must outlive the `JobRef`.
                //
                // Also, `as_job_ref` is plainly called only once.
                let job_ref = unsafe { job.as_job_ref() };
                self.queue_shared_job(job_ref);
                // SAFETY: `wait_for_value` must be called at most once. This is
                // the only call site for this particular `job`, which is a
                // stack-local variable.
                let result = unsafe { job.wait_for_value() };
                match result {
                    Ok(value) => value,
                    Err(error) => unwind::resume_unwinding(error),
                }
            }
        })
    }

    /// Runs the closure on a thread-pool worker.
    ///
    /// If this thread is not a worker, it will try to register itself as one.
    /// If the thread pool is full, this panics.
    ///
    /// If you don't want to panic, use [`on_worker`][ThreadPool::on_worker] or
    /// [`with_worker`][ThreadPool::with_worker] instead.
    #[inline(always)]
    #[track_caller]
    pub fn expect_worker<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        self.with_worker(|worker| match worker {
            Some(worker) => f(worker),
            None => panic!("thread pool full; not able to access worker"),
        })
    }

    /// Runs the closure on a thread-pool worker.
    ///
    /// If this thread is currently acting as a worker for the thread-pool, this
    /// just looks that worker up. If this thread is not registered as a worker,
    /// or if the thread's worker is registered with different thread pool, the
    /// thread will try to register itself with the correct pool. If the thread
    /// pool is full, it passes the closure `None`.
    ///
    /// The provided closure is never sent to another thread. If your closure is
    /// `Send`, consider using [`on_worker`][ThreadPool::on_worker] instead.
    #[inline(always)]
    pub fn with_worker<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(Option<&Worker>) -> R,
    {
        Worker::with_current(|worker| match worker {
            Some(worker) if worker.lease.thread_pool.id() == self.id() => f(Some(worker)),
            _ => self.with_worker_cold(f),
        })
    }

    /// Tries to register the calling thread on the thread pool, and pass a
    /// worker instance to the provided closure.
    ///
    /// This is the slow fallback for `with_worker` covering "external calls"
    /// from outside the pool. Never call this directly.
    #[cold]
    fn with_worker_cold<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(Option<&Worker>) -> R,
    {
        match self.claim_lease() {
            Some(lease) => Worker::occupy(lease, |worker| f(Some(worker))),
            None => f(None),
        }
    }
}

// -----------------------------------------------------------------------------
// Generalized spawn trait

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
pub trait Spawn<M>: Send + 'static {
    /// The handle returned when spawning this type.
    type Output: Send + 'static;

    /// Spawns work onto the thread pool.
    fn spawn(self, thread_pool: &'static ThreadPool, worker: Option<&Worker>) -> Self::Output;
}

impl<F> Spawn<FnOnceMarker> for F
where
    F: for<'worker> FnOnce(&'worker Worker) + Send + 'static,
{
    type Output = ();

    #[inline]
    fn spawn(self, thread_pool: &'static ThreadPool, worker: Option<&Worker>) {
        // Allocate a new job on the heap to store the closure.
        let job = HeapJob::new(self);

        // Turn the job into an "owning" `JobRef` so it can be queued.
        //
        // SAFETY: All jobs added to the queue are guaranteed to be executed
        // eventually, this is one of the core invariants of the thread pool.
        // The closure `f` has a static lifetime, meaning it only closes over
        // data that lasts for the duration of the program, so it's not possible
        // for this job to outlive the data `f` closes over.
        let job_ref = unsafe { job.into_job_ref() };

        // Queue the job for evaluation
        if let Some(worker) = worker {
            worker.fifo_queue.push_new(job_ref);
        } else {
            // Push the work into the share queue and wake a worker
            thread_pool.shared_jobs.push(job_ref);
        }
    }
}

/// An alias for [`async_task::Task`] that includes a reference to the pool on
/// which the future is executing.
pub type Task<T> = async_task::Task<T, &'static ThreadPool>;

/// Schedules a runnable future as a job.
///
/// Async-task prefers that this is a static function, rather than a closure,
/// which is why this is a separate function that pulls the thread pool from the
/// runnable metadata.
fn schedule_runnable(runnable: Runnable<&'static ThreadPool>) {
    // Get a ref to the thread pool from the runnable.
    let thread_pool = *runnable.metadata();

    // Temporarily turn the task into a raw pointer so that it can be
    // used as a job. We could also use `HeapJob` here, but since
    // `Runnable` is heap allocated this would result in a needless
    // second allocation.
    let job_pointer = runnable.into_raw();

    // SAFETY: The raw runnable pointer will remain valid until it is
    // used by `execute_runnable`, after which it will be dropped.
    let job_ref = unsafe { JobRef::new_raw(job_pointer, execute_runnable) };

    // Send this job off to be executed.
    thread_pool.with_worker(|worker| match worker {
        Some(worker) => worker.fifo_queue.push_new(job_ref),
        None => thread_pool.shared_jobs.push(job_ref),
    });
}

/// Executes a raw pointer to a runnable future.
#[inline(always)]
fn execute_runnable(this: NonNull<()>, _worker: &Worker) {
    // SAFETY: This pointer was created by `Runnable::into_raw` in
    // `schedule_runnable` with type parameter `&'static ThreadPool`, and
    // `from_raw` is called at most once.
    let runnable = unsafe { Runnable::<&'static ThreadPool>::from_raw(this) };
    // Poll the task. This will drop the future if the task is
    // canceled or the future completes.
    runnable.run();
}

impl<Fut, T> Spawn<FutureMarker> for Fut
where
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    type Output = Task<T>;

    #[inline]
    fn spawn(self, thread_pool: &'static ThreadPool, _worker: Option<&Worker>) -> Task<T> {
        // Create a runnable and add the thread pool as metadata.
        let (runnable, task) = async_task::Builder::new()
            .metadata(thread_pool)
            .spawn(|_| self, schedule_runnable);

        // Call the schedule function, pushing a `JobRef` for the future onto
        // the local work queue. If the future doesn't complete, it can be
        // woken and scheduled at a later point.
        //
        // Because we always look up the local worker within the schedule
        // function, woken futures will tend to run on the thread that wakes
        // them. This is a desirable property, as typically the next thing a
        // future is going to do after being woken up is read some data from the
        // thread/task that woke it.
        //
        // This is potentially more efficient than `Runnable::schedule`.
        schedule_runnable(runnable);

        // Return the task.
        task
    }
}

// -----------------------------------------------------------------------------
// Thread pool operations

impl ThreadPool {
    /// Spawns a job into the thread pool.
    ///
    /// See also: [`Worker::spawn`] and [`spawn`].
    #[inline(always)]
    pub fn spawn<M, S: Spawn<M>>(&'static self, work: S) -> S::Output {
        work.spawn(self, None)
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
        self.on_worker(|worker| worker.block_on(future))
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
        self.on_worker(|worker| worker.join(a, b))
    }

    /// Creates a scope onto which non-static work can be spawned.
    ///
    /// For more complete docs, see [`scope`]. If you have a reference to a
    /// worker, you should call [`Worker::scope`] instead.
    #[inline(always)]
    pub fn scope<'env, F, T>(&'static self, f: F) -> T
    where
        F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T + Send,
        T: Send,
    {
        self.on_worker(|worker| worker.scope(f))
    }
}

// -----------------------------------------------------------------------------
// Worker context

thread_local! {
    static WORKER_PTR: Cell<*const Worker> = const { Cell::new(ptr::null()) };
}

/// Represents membership in a thread pool.
///
/// To get access to worker for a given thread pool, users should call
/// [`ThreadPool::with_worker`], [`ThreadPool::on_worker`], [`ThreadPool::expect_worker`]
///
/// Every thread has at most one worker at a time. If a worker has already been
/// set up, it may be accessed at any time by calling [`Worker::with_current`].
/// A thread's worker can also be manually overridden by claiming a lease
/// ([`ThreadPool::claim_lease`]) and passing it to [`Worker::occupy`]. The
/// worker returned by `with_current` always represents the lease most recently
/// occupied in the call stack.
///
/// Every worker belongs to exactly one thread pool, and must hold a "lease" on
/// one of the shared slots within that pool.
///
/// Workers have one core memory-safety guarantee: Any jobs added to the worker
/// will eventually be executed.
pub struct Worker {
    migrated: Cell<bool>,
    lease: Lease,
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
    rng: XorShift64Star,
    last_promote_tick: Cell<u64>,
    // Make non-send.
    _phantom: PhantomData<*const ()>,
}

/// Describes the outcome of a call to [`Worker::yield_now`] or [`Worker::yield_local`].
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
    /// Temporarily sets the thread's worker. [`Worker::with_current`] always
    /// returns a reference to the worker set up by the most recent call to
    /// `occupy`.
    ///
    /// Rust's thread locals are fairly costly, so this function is expensive.
    /// If you can avoid calling it, do so.
    #[inline(always)]
    pub fn occupy<F, R>(lease: Lease, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        trace!("occupying lease");

        let span = trace_span!("occupy", seat_number = lease.seat_number);
        let _enter = span.enter();

        // Create a new worker to occupy the lease. Note: It's potentially a
        // problem that the same thread can occupy multiple workers on the same
        // thread. We may eventually need to design something to prevent this.
        let worker = Worker {
            migrated: Cell::new(false),
            lease,
            fifo_queue: JobQueue::new(),
            lifo_queue: JobQueue::new(),
            rng: XorShift64Star::new(),
            last_promote_tick: Cell::new(0),
            _phantom: PhantomData,
        };

        // Swap the local pointer to point to the newly allocated worker.
        let outer_ptr = WORKER_PTR.with(|ptr| ptr.replace(&worker));

        // Run the function within the context created by the worker pointer,
        // and pass in a worker reference directly.
        let result = f(&worker);

        // Finish executing local work before shutting down.
        while let Some(job_ref) = worker.find_local_work() {
            worker.execute(job_ref, false);
        }

        // Swap back to pointing to the previous value (possibly null).
        WORKER_PTR.with(|ptr| ptr.set(outer_ptr));

        trace!("vacating lease");

        // Return the intermediate values created while running the closure,
        // namely the result and any jobs still remaining on the local queue.
        result
    }

    /// Returns a reference to the push-side `Sharer` queue for this
    /// worker's seat.
    #[inline(always)]
    fn sharer(&self) -> &Sharer<JobRef> {
        &self.lease.seats.sharers[self.lease.seat_number]
    }

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
            // pointer to a `Worker`. It is only written to by `Worker::occupy`,
            // which stores the address of a `Worker` allocated within it's own
            // stack frame. Before it returns, `occupy` restores the previous
            // value of `WORKER_PTR`, so that it is always either null or points
            // to a live, immovable `Worker` on the current thread's call stack
            // (but is never left dangling).
            //
            // If the pointer is non-null, it is therefore valid to dereference
            // as a shared reference. Forming a `'static` reference is avoided
            // by passing the value into a closure, which bounds the reference's
            // lifetime to the closure body and prevents callers from retaining
            // it past the point where `occupy` returns and the `Worker` is
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
            // SAFETY: The `WORKER` static is only set by `occupy`, and it's
            // always set to a stack-allocated `Worker` which is never moved and
            // is only accessed through shared references. Therefore, if the
            // pointer is non-null, it must be safe to dereference.
            //
            // This creates a reference with an unbounded lifetime. To avoid
            // turning it into a `'static`, we pass it in to a closure. This
            // restricts its lifetime to the closure body, and prevents callers
            // from keeping around references to Workers that will be
            // deallocated when `occupy` returns.
            f(Some(unsafe { &*worker_ptr }))
        } else {
            f(None)
        }
    }

    /// Returns this worker's seat index within the pool (0–31).
    ///
    /// Seat numbers may be re-used by different workers at different times, and
    /// may not be contiguous or ordered.
    #[inline(always)]
    pub fn seat_number(&self) -> usize {
        self.lease.seat_number
    }

    /// Returns the thread pool this worker belongs to.
    #[inline(always)]
    pub fn thread_pool(&self) -> &'static ThreadPool {
        self.lease.thread_pool
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
        let current_tick = tick_counter::start();
        if current_tick.wrapping_sub(self.last_promote_tick.get()) >= Self::PROMOTE_TICK_INTERVAL {
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
        let sleeping = self.lease.thread_pool.sleeping.load(Ordering::Relaxed);
        if sleeping == 0 {
            return;
        }
        cold_path();

        // Track if we actually managed to share work.
        let mut shared_job = false;

        // Share work from the lifo queue. This is shared bit-by-bit, with old
        // (and therefore theoretically "large") tasks shared first.
        if let Some(job_ref) = self.lifo_queue.pop_oldest() {
            // Push into our own steal queue so siblings can steal it.
            if let Err(job_ref) = self.sharer().push(job_ref) {
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
            // SAFETY: `into_job_ref` requires that the data closed over by the
            // `HeapJob` outlive the `JobRef`.
            //
            // Here, the closure captures `job_refs` (a `VecDequeue<JobRef>`) by
            // value, and so trivially outlives the newly created `JobRef`.
            let batch_job_ref = unsafe { batch_job.into_job_ref() };
            // Push the batch job into the steal queue so siblings can steal it.
            if let Err(job_ref) = self.sharer().push(batch_job_ref) {
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
            self.wake_random(sleeping);
        }
    }

    /// Tries to wake a random sleeping worker. Expects to be given a bitset of
    /// sleeping workers.
    #[inline(always)]
    fn wake_random(&self, sleeping: u32) {
        let offset = self.rng.next_usize(32) as u32;
        let mut randomized_sleeping = sleeping.rotate_right(offset);
        while randomized_sleeping != 0 {
            let index = (randomized_sleeping.trailing_zeros() + offset) % 32;
            randomized_sleeping &= randomized_sleeping - 1; // Clear the lowest bit
            let woken = self.lease.seats.sleep_controllers[index as usize].wake();
            if woken {
                return;
            }
        }
    }

    /// Create a new latch owned by the worker.
    #[inline(always)]
    pub fn new_latch(&self) -> Latch {
        Latch::new(
            self.lease.seat_number,
            &self.lease.thread_pool.sleeping,
            &self.lease.seats.sleep_controllers[self.lease.seat_number],
        )
    }

    /// Runs jobs until the provided latch is set.
    ///
    /// The thread may go to sleep if it runs out of work to do, but will wake
    /// when the latch is set or more work becomes available.
    #[inline(always)]
    pub fn wait_for(&self, latch: &Latch) {
        while !latch.check() {
            if self.yield_now() == Yield::Idle {
                latch.wait();
            }
        }
    }

    /// Finds a job to work on. This function is entirely local, and does no
    /// synchronization with the queue.
    #[inline(always)]
    fn find_local_work(&self) -> Option<JobRef> {
        self.lifo_queue
            .pop_newest()
            .or_else(|| self.fifo_queue.pop_oldest())
    }

    /// Finds a job to work on. This tries
    /// [`find_local_work`][Worker::find_local_work] first, then falls back to
    /// pulling shared work from the thread pool.
    #[inline(always)]
    fn find_work(&self) -> Option<(JobRef, bool)> {
        self.find_local_work()
            .map(|job| (job, false))
            .or_else(|| self.sharer().pop().map(|job| (job, false)))
            .or_else(|| self.steal_from_siblings().map(|job| (job, true)))
            .or_else(|| self.claim_shared_job().map(|job| (job, true)))
    }

    /// Attempts to steal a job from another worker's work-stealing queue.
    ///
    /// Iterates over occupied seats in a random order to avoid always hitting
    /// the same victim. Because stealers are pre-allocated and permanent, no
    /// lock or atomic load is needed to access them.
    fn steal_from_siblings(&self) -> Option<JobRef> {
        let stealers = &self.lease.seats.stealers;
        let occupied = self.lease.thread_pool.occupied.load(Ordering::Relaxed);
        let my_seat = self.lease.seat_number as u32;

        // Randomise the starting position so all workers get a fair shot as victims.
        let offset = self.rng.next_usize(32) as u32;
        let mut bits = (occupied & !(1u32 << my_seat)).rotate_right(offset);

        while bits != 0 {
            let shifted_idx = bits.trailing_zeros();
            let idx = (shifted_idx + offset) % 32;
            bits &= bits - 1;
            // The stealer is a permanent reference — no lock or atomic load needed.
            let stealer = &stealers[idx as usize];
            // `steal_and_pop` returns one job directly and moves up to half the
            // remaining items into our steal queue for later use.
            loop {
                match stealer.steal_and_pop(self.sharer(), |n| n / 2) {
                    Ok((job, _)) => return Some(job),
                    Err(StealError::Busy) => {} // transient; retry
                    Err(StealError::Empty) => break,
                }
            }
        }
        None
    }

    /// Claims a job from the global injector queue.
    #[inline(always)]
    fn claim_shared_job(&self) -> Option<JobRef> {
        self.lease.thread_pool.shared_jobs.pop()
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
    /// Spawns work (a closure or future) onto the thread pool. Just like a
    /// standard thread, this work executes concurrently (and potentially in
    /// parallel) to the place where it is spawned. It is not tied to the
    /// current stack frame, and hence it cannot hold any references other than
    /// those with `'static` lifetime. If you want to spawn a task that
    /// references stack data, use the [`scope`], [`ThreadPool::scope`] or
    /// [`Worker::scope`] functions.
    ///
    /// Since tasks spawned with this function cannot hold references into the
    /// enclosing stack frame, you almost certainly want to use a move closure
    /// as their argument (otherwise, the closure will typically hold references
    /// to any variables from the enclosing function that you happen to use).
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::spawn`] or simply [`spawn`].
    #[inline]
    pub fn spawn<M, S: Spawn<M>>(&self, work: S) -> S::Output {
        work.spawn(self.lease.thread_pool, Some(self))
    }

    /// Polls a future to completion, then returns the outcome. This function
    /// will prioritize polling the future as soon as it becomes available, and
    /// while the future is not available it will try to do other meaningful
    /// work from the thread-pool. If the thread pool runs out of work, the
    /// thread is suspended until the future completes or more background work
    /// becomes available.
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
    /// If the future panics, this immediately panics.
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
    /// THREAD_POOL.on_worker(|worker| quick_sort(worker, &mut v));
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
    /// let result = THREAD_POOL.on_worker(|worker| sum(worker, &tree));
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
        let mut stack_job = StackJob::new(a, self);

        // SAFETY: The `StackJob` is allocated on the stack just above, is never
        // moved, and so will live for the entirety of this function in the same
        // memory location. If closure `a` closes over data, that must be valid
        // for the lifetime of this function as well. The `JobRef` cannot
        // outlive either, because it is guaranteed to be executed before the
        // function returns. We also clearly never create more than one `JobRef`
        // using the `stack_job`.
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
            // SAFETY: Because the ids match, the JobRef we just popped from
            // the queue must point to `stack_job`, implying that
            // `stack_job` cannot have been executed yet.
            let a = unsafe { stack_job.unwrap() };
            // Execute the closure directly and return the results. This is
            // allows the compiler to inline and optimize `a`.
            result_a = unwind::halt_unwinding(|| a(self));
        } else {
            // Wait for the job to complete.
            self.wait_for(stack_job.completion_latch());
            // SAFETY: The job must be complete, because we just waited on the latch.
            result_a = unsafe { stack_job.return_value() };
        }

        // Resume unwinding if either job panicked.
        match (result_a, result_b) {
            (Err(error), _) | (_, Err(error)) => unwind::resume_unwinding(error),
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
    /// # THREAD_POOL.expect_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn_on(worker, |_: &Worker| {
    ///         // Transfer ownership of `bad` into a local variable (also named `bad`).
    ///         // This will force the closure to take ownership of `bad` from the environment.
    ///         let bad = bad;
    ///         println!("ok: {:?}", ok); // `ok` is only borrowed.
    ///         println!("bad: {:?}", bad); // refers to our local variable, above.
    ///     });
    ///
    ///     scope.spawn_on(worker, |_: &Worker| println!("ok: {:?}", ok)); // we too can borrow `ok`
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
    /// # THREAD_POOL.expect_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn_on(worker, move |_: &Worker| {
    ///         println!("ok: {:?}", ok);
    ///         println!("bad: {:?}", bad);
    ///     });
    ///
    ///     // That closure is fine, but now we can't use `ok` anywhere else,
    ///     // since it is owned by the previous task:
    ///     // scope.spawn_on(worker, |_: &Worker| println!("ok: {:?}", ok));
    /// });
    /// # });
    /// ```
    ///
    /// While this works, it could be a problem if we want to use `ok` elsewhere.
    /// There are two choices. We can keep the closure as a `move` closure, but
    /// instead of referencing the variable `ok`, we create a shadowed variable that
    /// is a borrow of `ok` and capture *that*:
    ///
    /// ```rust
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.expect_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     let ok: &Vec<i32> = &ok; // shadow the original `ok`
    ///     scope.spawn_on(worker, move |_: &Worker| {
    ///         println!("ok: {:?}", ok); // captures the shadowed version
    ///         println!("bad: {:?}", bad);
    ///     });
    ///
    ///     // Now we too can use the shadowed `ok`, since `&Vec<i32>` references
    ///     // can be shared freely. Note that we need a `move` closure here though,
    ///     // because otherwise we'd be trying to borrow the shadowed `ok`,
    ///     // and that doesn't outlive `scope`.
    ///     scope.spawn_on(worker, move |_: &Worker| println!("ok: {:?}", ok));
    /// });
    /// # });
    /// ```
    ///
    /// Another option is not to use the `move` keyword but instead to take ownership
    /// of individual variables:
    ///
    /// ```rust
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.expect_worker(|worker| {
    /// let ok: Vec<i32> = vec![1, 2, 3];
    /// forte::scope(|scope| {
    ///     let bad: Vec<i32> = vec![4, 5, 6];
    ///     scope.spawn_on(worker, |_: &Worker| {
    ///         // Transfer ownership of `bad` into a local variable (also named `bad`).
    ///         // This will force the closure to take ownership of `bad` from the environment.
    ///         let bad = bad;
    ///         println!("ok: {:?}", ok); // `ok` is only borrowed.
    ///         println!("bad: {:?}", bad); // refers to our local variable, above.
    ///     });
    ///
    ///     scope.spawn_on(worker, |_: &Worker| println!("ok: {:?}", ok)); // we too can borrow `ok`
    /// });
    /// # });
    /// ```
    ///
    /// # Referencing the scope
    ///
    /// The scope passed into the closure is not allowed to leak out of this call.
    /// In other words, this will fail to compile:
    ///
    /// ```compile_fail
    /// # use forte::ThreadPool;
    /// # use forte::Worker;
    /// # static THREAD_POOL: ThreadPool = ThreadPool::new();
    /// # THREAD_POOL.populate();
    /// # THREAD_POOL.expect_worker(|worker| {
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
    /// # THREAD_POOL.expect_worker(|worker| {
    /// let mut counter = 0;
    /// let counter_ref = &mut counter;
    /// forte::scope(|scope| {
    ///     scope.spawn_on(worker, |worker: &Worker| {
    ///         *counter_ref += 1;
    ///         // Note: we borrow the scope again here.
    ///         scope.spawn_on(worker, move |_: &Worker| {
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
    ///             scope.spawn_on(worker, |_: &Worker| { });
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
}

// -----------------------------------------------------------------------------
// Implicit worker registration api

/// Runs the provided closure in the background.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Worker::occupy`], [`ThreadPool::with_worker`], or similar)
/// this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If you have a reference to a [`Worker`], it's better to use [`Worker::spawn`]
/// instead. If you don't have a worker, but know which thread pool you want to
/// use, [`ThreadPool::spawn`] is more appropriate.
///
/// <div class="example-wrap" style="display:inline-block"><pre class="compile_fail" style="white-space:normal;font:inherit;">
///
/// **Warning:** This function panics if the current thread is not registered as a worker.
///
/// </pre></div>
pub fn spawn<M, S: Spawn<M>>(work: S) -> S::Output {
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::spawn` from outside a thread pool")
            .spawn(work)
    })
}

/// Waits for a future to complete.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Worker::occupy`], [`ThreadPool::with_worker`], or similar)
/// this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::block_on`] instead. If you don't have a worker, but know which
/// thread pool you want to use, [`ThreadPool::block_on`] is more appropriate.
///
/// <div class="example-wrap" style="display:inline-block"><pre class="compile_fail" style="white-space:normal;font:inherit;">
///
/// **Warning:** This function panics if the current thread is not registered as a worker.
///
/// </pre></div>
pub fn block_on<F, T>(future: F) -> T
where
    F: Future<Output = T> + Send,
    T: Send,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::block_on` from outside a thread pool")
            .block_on(future)
    })
}

/// Executes the two closures, possibly in parallel.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Worker::occupy`], [`ThreadPool::with_worker`], or similar)
/// this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If you have a reference to a [`Worker`], it's better to use [`Worker::join`]
/// instead. If you don't have a worker, but know which thread pool you want to
/// use, [`ThreadPool::join`] is more appropriate.
///
/// <div class="example-wrap" style="display:inline-block"><pre class="compile_fail" style="white-space:normal;font:inherit;">
///
/// **Warning:** This function panics if the current thread is not registered as a worker.
///
/// </pre></div>
pub fn join<A, B, RA, RB>(a: A, b: B) -> (RA, RB)
where
    A: FnOnce(&Worker) -> RA + Send,
    B: FnOnce(&Worker) -> RB + Send,
    RA: Send,
    RB: Send,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::join` from outside a thread pool")
            .join(a, b)
    })
}

/// Creates a new scope for spawning non-static work.
///
/// When executed on a thread that is currently registered as a worker (i.e. the
/// closure inside [`Worker::occupy`], [`ThreadPool::with_worker`], or similar)
/// this is able to look up that registration and find the worker and
/// thread-pool implicitly.
///
/// If you have a reference to a [`Worker`], it's better to use
/// [`Worker::scope`] instead. If you don't have a worker, but know which thread
/// pool you want to use, [`ThreadPool::scope`] is more appropriate.
///
/// <div class="example-wrap" style="display:inline-block"><pre class="compile_fail" style="white-space:normal;font:inherit;">
///
/// **Warning:** This function panics if the current thread is not registered as a worker.
///
/// </pre></div>
pub fn scope<'env, F, T>(f: F) -> T
where
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env>) -> T,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::scope` from outside a thread pool")
            .scope(f)
    })
}

// -----------------------------------------------------------------------------
// Main worker loop

/// This is the main loop for a worker thread. It's in charge of executing jobs.
/// Operating on the principle that you should finish what you start before
/// starting something new, workers will first execute their queue, then execute
/// shared jobs, then pull new jobs from the injector.
fn managed_worker(lease: Lease, halt: Arc<AtomicBool>) {
    trace!("starting managed worker");

    // Register as the indicated worker, and work until we are told to halt.
    Worker::occupy(lease, |worker| {
        while !halt.load(Ordering::Relaxed) {
            #[cfg(feature = "shuttle")]
            shuttle::hint::spin_loop();

            if let Some((job, migrated)) = worker.find_work() {
                worker.execute(job, migrated);
            } else {
                worker.lease.seats.sleep_controllers[worker.lease.seat_number]
                    .sleep(worker.lease.seat_number, &worker.lease.thread_pool.sleeping);
            }
        }
    });

    trace!("exiting managed worker");
}

// -----------------------------------------------------------------------------
// Tests

#[cfg(all(test, not(feature = "shuttle")))]
mod tests {

    use alloc::vec;
    use std::sync::mpsc::channel;

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
    #[cfg(not(miri))] // This is too much for miri to handle
    fn join_long() {
        fn increment(worker: &Worker, slice: &mut [u32]) {
            match slice.len() {
                0 => (),
                1 => slice[0] += 1,
                _ => {
                    let (head, tail) = slice.split_at_mut(1);

                    worker.join(|_| head[0] += 1, |worker| increment(worker, tail));
                }
            }
        }

        static THREAD_POOL: ThreadPool = ThreadPool::new();
        THREAD_POOL.resize_to_available();

        let mut vals = [0; 1_024];
        THREAD_POOL.on_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, [1; 1_024]);

        THREAD_POOL.depopulate();
    }

    #[test]
    #[cfg(not(miri))] // This is too much for miri to handle
    fn join_very_long() {
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

        let mut vals = vec![0; 512 * 512];
        THREAD_POOL.on_worker(|worker| increment(worker, &mut vals));
        assert_eq!(vals, vec![1; 512 * 512]);

        THREAD_POOL.depopulate();
    }
}
