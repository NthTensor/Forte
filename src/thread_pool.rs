//! This module contains the api and worker logic for the Forte thread pool.

use alloc::collections::BTreeMap;
use alloc::collections::btree_map::Entry;
use alloc::format;
use alloc::string::ToString;
use alloc::vec::Vec;
use core::cmp;
use core::future::Future;
use core::num::NonZero;
use core::pin::Pin;
use core::pin::pin;
use core::ptr;
use core::ptr::NonNull;
use core::task::Context;
use core::task::Poll;
use core::time::Duration;
use std::time::Instant;

use async_task::Runnable;
use async_task::Task;
use tracing::debug;
use tracing::trace;
use tracing::trace_span;

use crate::blocker::Blocker;
use crate::job::HeapJob;
use crate::job::JobQueue;
use crate::job::JobRef;
use crate::job::StackJob;
use crate::platform::*;
use crate::scope::Scope;
use crate::signal::Signal;

// -----------------------------------------------------------------------------
// Thread pool worker leases

/// A lease is a capability that the thread pool hands out to threads, allowing
/// them to act as a worker on that pool.
pub struct Lease {
    thread_pool: &'static ThreadPool,
    index: usize,
    heartbeat: Arc<AtomicBool>,
}

// -----------------------------------------------------------------------------
// Thread pool types

/// The "heartbeat interval" controls the frequency at which workers share work.
pub const HEARTBEAT_INTERVAL: Duration = Duration::from_micros(500);

/// The `ThreadPool` object is used to orchestrate and distribute work to a pool
/// of threads, and is generally the main entry point to using `Forte`.
///
/// # Creating Thread Pools
///
/// Thread pools must be static and const constructed. You don't have to worry
/// about `LazyStatic` or anything else; to create a new thread pool, just call
/// [`ThreadPool::new`].
///
/// ```
/// # #![cfg(not(loom))]
/// # use forte::prelude::*;
/// // Allocate a new thread pool.
/// static THREAD_POOL: ThreadPool = ThreadPool::new();
///
/// fn main() {
///     // Resize the pool to fill the available number of cores.
///     THREAD_POOL.resize_to_available();
///     // Access one of the pool's worker threads.
///     THREAD_POOL.install(|w| {
///         // Spawn a compute job.
///         w.spawn(|w| {
///             // Then spawn another one using the provided worker.
///             w.spawn(|_| 1);
///             // Spawn one using the thread pool directly.
///             THREAD_POOL.spawn(|_| 2);
///             // Spawn one using the current thread pool, without the worker.
///             forte::spawn(|_| 3);
///         });
///         // Spawn an async job.
///         let task = w.spawn_async(async || { "Hello World" });
///         // Do two operations in parallel.
///         let (a, b) = w.join(|_| 1, |_| 2);
///         // Wait for a task to complete.
///         let result = w.block_on(task);
///     });
///     // Halt all the threads in the pool.
///     THREAD_POOL.resize_to(0);
/// }
/// ```
///
/// This attaches a new thread pool to your program named `THREAD_POOL`, which you
/// can begin to schedule work on immediately. The thread pool will exist for
/// the entire duration of your program, and will shut down when your program
/// completes.
///
/// # Resizing Thread Pools
///
/// Thread pools are dynamically sized; When your program starts they have size
/// zero (meaning no threads are running), and you will have to add threads by
/// resizing it. The simplest way to resize a pool is via
/// [`ThreadPool::resize_to_available`] which will simply fill all the available
/// space. More granular control is possible through other methods such as
/// [`ThreadPool::grow`], [`ThreadPool::shrink`], or [`ThreadPool::resize_to`].
///
pub struct ThreadPool {
    state: Mutex<ThreadPoolState>,
    job_is_ready: Condvar,
}

struct ThreadPoolState {
    shared_jobs: BTreeMap<usize, JobRef>,
    tenants: Vec<Option<Tenant>>,
    managed_threads: ManagedThreads,
}

impl ThreadPoolState {
    pub fn claim_shared_job(&mut self) -> Option<JobRef> {
        self.shared_jobs.pop_first().map(|(_, job_ref)| job_ref)
    }

    /// Claims a lease on the thread pool. A lease can be passed to
    /// [`Worker::occupy`] to enter a worker context for the thread pool.
    ///
    /// There are a finite number of leases available on each pool. If they are
    /// already claimed, this returns `None`.
    pub fn claim_lease(&mut self, thread_pool: &'static ThreadPool) -> Lease {
        let heartbeat = Arc::new(AtomicBool::new(false));
        let tenant = Tenant {
            heartbeat: Arc::downgrade(&heartbeat),
            last_heartbeat: Instant::now(),
        };

        for (index, occupant) in self.tenants.iter_mut().enumerate() {
            if occupant.is_none() {
                *occupant = Some(tenant);
                return Lease {
                    thread_pool,
                    index,
                    heartbeat,
                };
            }
        }

        self.tenants.push(Some(tenant));
        Lease {
            thread_pool,
            index: self.tenants.len(),
            heartbeat,
        }
    }

    /// Attempts to claim several leases at once. See
    /// [`ThreadPool::claim_lease`] for more information. If no leases are
    /// available, this returns an empty vector.
    pub fn claim_leases(&mut self, thread_pool: &'static ThreadPool, num: usize) -> Vec<Lease> {
        let mut leases = Vec::with_capacity(num);

        let now = Instant::now();

        for (index, occupant) in self.tenants.iter_mut().enumerate() {
            if leases.len() == num {
                return leases;
            }

            if occupant.is_none() {
                let heartbeat = Arc::new(AtomicBool::new(false));
                let tenant = Tenant {
                    heartbeat: Arc::downgrade(&heartbeat),
                    last_heartbeat: now,
                };
                *occupant = Some(tenant);
                leases.push(Lease {
                    thread_pool,
                    index,
                    heartbeat,
                });
            }
        }

        while leases.len() != num {
            let heartbeat = Arc::new(AtomicBool::new(false));
            let tenant = Tenant {
                heartbeat: Arc::downgrade(&heartbeat),
                last_heartbeat: now,
            };
            self.tenants.push(Some(tenant));
            leases.push(Lease {
                thread_pool,
                index: self.tenants.len(),
                heartbeat,
            });
        }

        leases
    }
}

struct Tenant {
    heartbeat: Weak<AtomicBool>,
    last_heartbeat: Instant,
}

/// Manages thread spawned by the pool.
struct ManagedThreads {
    /// Stores thread controls for workers spawned by the pool.
    workers: Vec<ManagedWorker>,
    /// Stores thread controls for the heartbeat thread.
    heartbeat: Option<ThreadControl>,
}

/// Represents a worker thread that is managed by the pool, as opposed to an
/// external threads which temporarally participate in the pool.
struct ManagedWorker {
    /// The index of this worker in the public worker info list.
    index: usize,
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
    #[cfg(not(loom))]
    pub const fn new() -> ThreadPool {
        ThreadPool {
            state: Mutex::new(ThreadPoolState {
                shared_jobs: BTreeMap::new(),
                tenants: Vec::new(),
                managed_threads: ManagedThreads {
                    workers: Vec::new(),
                    heartbeat: None,
                },
            }),
            job_is_ready: Condvar::new(),
        }
    }

    /// Non-const constructor variant for loom.
    #[cfg(loom)]
    pub fn new() -> ThreadPool {
        let managed_threads = ManagedThreads {
            workers: Vec::new(),
            heartbeat: None,
        };

        let worker_info = [(); MAX_WORKERS].map(|_| WorkerInfo {
            wake: Latch::new(),
            queue: Queue::with_recycle(2, RecycleJobRef),
        });

        ThreadPool {
            queue: UnboundedQueue::new(),
            registry: Mutex::new([false; MAX_WORKERS]),
            worker_info,
            heartbeat: AtomicU32::new(100),
            managed_threads: Mutex::new(managed_threads),
        }
    }

    /// Resizes the thread pool to fill all available space. After this returns,
    /// the pool will have at least one worker thread and at most `MAX_THREADS`.
    /// Returns the new size of the pool.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn resize_to_available(&'static self) -> usize {
        let available = available_parallelism().map(NonZero::get).unwrap_or(1);
        let available = available.saturating_sub(2);
        self.resize_to(available)
    }

    /// Resizes the pool to the specified number of threads. Returns the new
    /// size of the thread pool, which may be smaller than requested.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn resize_to(&'static self, new_size: usize) -> usize {
        self.resize(|_| new_size)
    }

    /// Adds the given number of threads to the thread pool. Returns the new
    /// size of the pool, which may be smaller than requested.
    ///
    /// See [`ThreadPool::resize_to`] for more information about resizing.
    pub fn grow(&'static self, added_threads: usize) -> usize {
        self.resize(|current_size| current_size + added_threads)
    }

    /// Removes the given number of thread from the thread pool. Returns the new
    /// size of the pool.
    ///
    /// See [`ThreadPool::resize_to`] for more information about resizing.
    pub fn shrink(&'static self, terminated_threads: usize) -> usize {
        self.resize(|current_size| current_size - terminated_threads)
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

    /// Resizes the pool, and returns the new size.
    ///
    /// Not that the new size may be different from the size requested.
    #[cold]
    pub fn resize<F>(&'static self, get_size: F) -> usize
    where
        F: Fn(usize) -> usize,
    {
        debug!("starting threadpool resize");

        // Resizing a pool is a critical section; only one thread can resize the
        // pool at a time. This is implemented using a mutex on the thread manager.
        trace!("locking state");
        let mut state = self.state.lock().unwrap();

        // Compute the new size of the pool, given the current size.
        let current_size = state.managed_threads.workers.len();

        // You are only allowed to spawn managed threads for up to half the total number of workers,
        // to leave room for non-managed threads. By default, this means at most 16 workers can be managed.
        let mut new_size = get_size(current_size);

        trace!(
            "attempting to resize thread pool from {} to {} thread(s)",
            current_size, new_size
        );

        match new_size.cmp(&current_size) {
            // The size remained the same
            cmp::Ordering::Equal => {
                debug!("completed threadpool resize, size unchanged");
                return current_size;
            }
            // The size increased
            cmp::Ordering::Greater => {
                // Acquire leases for the new threads.
                trace!("locking worker leases");
                let new_leases = state.claim_leases(self, new_size - current_size);
                new_size = current_size + new_leases.len(); // Scale back the new size to what we can actually spawn.
                trace!("acquired leases for {} new threads", new_size);

                // When not in loom, start the heartbeat thread if scaling up from zero.
                #[cfg(not(loom))]
                if new_size > 0 && current_size == 0 {
                    debug!("spawning heartbeat runner");
                    let halt = Arc::new(AtomicBool::new(false));
                    let heartbeat_halt = halt.clone();
                    let handle = ThreadBuilder::new()
                        .name("heartbeat".to_string())
                        .spawn(move || {
                            heartbeat_loop(self, heartbeat_halt);
                        })
                        .unwrap();
                    let control = ThreadControl { halt, handle };
                    state.managed_threads.heartbeat = Some(control);
                }

                // Loom dosn't support barriers.
                #[cfg(not(loom))]
                let barrier = Arc::new(Barrier::new(new_leases.len() + 1));

                // Spawn the new workers.
                for lease in new_leases {
                    let index = lease.index;
                    debug!("spawning managed worker with index {}", index);
                    let halt = Arc::new(AtomicBool::new(false));
                    let worker_halt = halt.clone();
                    #[cfg(not(loom))]
                    let worker_barrier = barrier.clone();
                    let handle = ThreadBuilder::new()
                        .name(format!("worker {index}"))
                        .spawn(move || {
                            managed_worker(
                                lease,
                                worker_halt,
                                #[cfg(not(loom))]
                                worker_barrier,
                            );
                        })
                        .unwrap();
                    let control = ThreadControl { halt, handle };
                    state
                        .managed_threads
                        .workers
                        .push(ManagedWorker { index, control });
                }

                drop(state);

                // When not in loom, wait for the threads to start.
                #[cfg(not(loom))]
                barrier.wait();
            }
            // The size decreased
            cmp::Ordering::Less => {
                // Halt the heartbeat thread when scaling to zero.
                if let Some(control) = state.managed_threads.heartbeat.take() {
                    control.halt.store(true, Ordering::Relaxed);
                    let _ = control.handle.join();
                }

                // Pull the workers we intend to hault out of the thread manager.
                let terminating_workers = state.managed_threads.workers.split_off(new_size);

                // Terminate the workers.
                for worker in &terminating_workers {
                    // Tell the worker to halt.
                    worker.control.halt.store(true, Ordering::Relaxed);
                }

                // Wake any sleeping workers to ensure they will eventually see the termination notice.
                self.job_is_ready.notify_all();

                let own_lease = Worker::map_current(|worker| worker.lease.index);

                // Wait for the workers to fully halt.
                for worker in terminating_workers {
                    // It's possible we may be trying to terminate ourselves, in
                    // which case we can skip the thread-join.
                    if Some(worker.index) != own_lease {
                        let _ = worker.control.handle.join();
                    }
                }
            }
        }

        debug!("completed thread pool resize");

        // Return the new size of the threadpool
        new_size
    }

    /// Returns an opaque identifier for this thread pool.
    #[inline(always)]
    pub fn id(&self) -> usize {
        // We can rely on `self` not to change since it's a static ref.
        ptr::from_ref(self) as usize
    }

    /// Tries to ensure the calling thread is a member of the thread pool, and
    /// then executes the provided closure. If the thread is already a member of
    /// the pool, the closure is called directly. Otherwise, the thread will
    /// attempt to temporarally register itself with the pool (which can be
    /// slightly slower). If registration fails (because the pool is full to
    /// capacity) the closure is passed `None` instead of a worker instance.
    ///
    /// The provided closure is never sent to another thread.
    #[inline(always)]
    pub fn as_worker<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        Worker::with_current(|worker| match worker {
            Some(worker) if worker.lease.thread_pool.id() == self.id() => f(worker),
            _ => self.as_worker_cold(f),
        })
    }

    /// Tries to register the calling thread on the thread pool, and pass a
    /// worker instance to the provided closure.
    ///
    /// This is the slow fallback for `as_worker` covering "external calls"
    /// from outside the pool. Never call this directly.
    #[cold]
    fn as_worker_cold<F, R>(&'static self, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        let lease = self.state.lock().unwrap().claim_lease(self);
        Worker::occupy(lease, f)
    }
}

// -----------------------------------------------------------------------------
// Thread pool scheduling api

impl ThreadPool {
    /// Spawns a job into the thread pool.
    ///
    /// See also: [`Worker::spawn`] and [`spawn`].
    #[inline]
    pub fn spawn<F>(&'static self, f: F)
    where
        F: FnOnce(&Worker) + Send + 'static,
    {
        self.as_worker(|worker| worker.spawn(f));
    }

    /// Spawns a future onto the thread pool.
    ///
    /// See also: [`Worker::spawn_future`] and [`spawn_future`].
    #[inline]
    pub fn spawn_future<F, T>(&'static self, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // This function "schedules" work on the future, which in this case
        // pushing a `JobRef` that knows how to run it onto the local work queue.
        let schedule = move |runnable: Runnable| {
            // Temporarally turn the task into a raw pointer so that it can be
            // used as a job. We could also use `HeapJob` here, but since
            // `Runnable` is heap allocated this would result in a needless
            // second allocation.
            let job_pointer = runnable.into_raw();

            // Define a function to run the runnable that will be comparable with `JobRef`.
            #[inline]
            fn execute_runnable(this: NonNull<()>, _worker: &Worker) {
                // SAFETY: This pointer was created by the call to `Runnable::into_raw` just above.
                let runnable = unsafe { Runnable::<()>::from_raw(this) };
                // Poll the task. This will drop the future if the task is
                // canceled or the future completes.2
                runnable.run();
            }

            // SAFETY: The raw runnable pointer will remain valid until it is
            // used by `execute_runnable`, after which it will be dropped.
            let job_ref = unsafe { JobRef::new_raw(job_pointer, execute_runnable) };

            // Send this job off to be executed.
            self.as_worker(|worker| {
                worker.queue.push_back(job_ref);
            });
        };

        // Creates a task from the future and schedule.
        let (runnable, task) = async_task::spawn(future, schedule);

        // This calls the schedule function, pushing a `JobRef` for the future
        // onto the local work queue. If the future dosn't complete, it will
        // schedule a waker that will call this schedule again, which will then
        // add create a new `JobRef`.
        //
        // Because we always look up the local worker within the schedule
        // function, woken futures will tend to run on the thread that wakes
        // them. This is a desirable property, as typically the next thing a
        // future is going to do after being woken up is read some data from the
        // thread that woke it.
        runnable.schedule();

        // Return the task, which acts as a handle for this series of jobs.
        task
    }

    /// Spawns an async closure onto the thread pool.
    ///
    /// See also: [`Worker::spawn_async`] and [`spawn_async`].
    #[inline]
    pub fn spawn_async<Fn, Fut, T>(&'static self, f: Fn) -> Task<T>
    where
        Fn: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Wrap the function into a future using an async block.
        let future = async move { f().await };
        // We just pass this future to `spawn_future`.
        self.spawn_future(future)
    }

    /// Blocks the thread waiting for a future to complete.
    ///
    /// See also: [`Worker::block_on`] and [`block_on`].
    #[inline]
    pub fn block_on<F, T>(&'static self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        self.as_worker(|worker| worker.block_on(future))
    }

    /// Executes the two closures, possibly in parallel, and returns the
    /// results.
    ///
    /// See also: [`Worker::join`] and [`join`].
    #[inline]
    pub fn join<A, B, RA, RB>(&'static self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&Worker) -> RA + Send,
        B: FnOnce(&Worker) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        self.as_worker(|worker| worker.join(a, b))
    }

    /// Create a scope for spawning non-static work.
    ///
    /// See also: [`Worker::scope`] and [`scope`].
    #[inline]
    pub fn scope<'scope, F, T>(&'static self, f: F) -> T
    where
        F: FnOnce(Pin<&Scope<'scope>>) -> T + Send,
        T: Send,
    {
        self.as_worker(|worker| worker.scope(f))
    }
}

// -----------------------------------------------------------------------------
// Worker thread data

#[cfg(not(loom))]
thread_local! {
    static WORKER_PTR: Cell<*const Worker> = const { Cell::new(ptr::null()) };
}

#[cfg(loom)]
thread_local! {
    static RNG: XorShift64Star = XorShift64Star::new();
    static WORKER_PTR: Cell<*const Worker> = Cell::new(ptr::null());
}

/// Holds the local context for a thread pool member, which allows queuing,
/// executing and sharing jobs on the pool.
///
/// Workers are the recommended way to interface with a thread pool. To get
/// access to worker for a given thread pool, users should call
/// [`ThreadPool::as_worker`] (which keeps work in the same thread, but
/// sometimes may fail to acquire a worker) or [`ThreadPool::in_worker`] (which
/// may send work to other threads, but will always acquire a worker).
///
/// Every thread has at most one worker at a time. If a worker has already been
/// set up, it may be accessed at any time by calling [`Worker::with_current`].
/// A thread's worker can also manually overridden by claiming a lease
/// ([`ThreadPool::claim_lease`]) and passing it to [`Worker::occupy`]. The
/// worker returned by `with_current` always represents the lease most recently
/// occupied in the call stack.
///
/// Every worker belongs to exactly one thread pool, and must hold a "lease" on
/// one of the shared slots within that pool.
///
/// Workers have one core memory-safety grantee: Any jobs added to the worker
/// will eventually be executed.
pub struct Worker {
    pub(crate) lease: Lease,
    pub(crate) queue: JobQueue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Yield {
    Executed,
    Idle,
}

impl Worker {
    /// Calls the provided closure on the thread's worker instance, if it has one.
    ///
    /// Rust's thread locals are fairly costly, so this function is expensive.
    /// If you can avoid calling it, do so.
    #[inline]
    pub fn map_current<F, R>(f: F) -> Option<R>
    where
        F: FnOnce(&Worker) -> R,
    {
        let worker_ptr = WORKER_PTR.with(Cell::get);
        if !worker_ptr.is_null() {
            // SAFETY: The `WORKER` static is only set by `occupy`, and it's
            // always set to a stack-allocated `Worker` which is never moved and
            // is only accseed through shared references. Therefore, if the
            // pointer is non-null, it must be safe to dereference.
            //
            // This creates a reference with an unbounded lifetime. To avoid
            // turning it into a `'static`, we pass it in to a closure. This
            // restricts it's lifetime to the closure body, and prevents callers
            // from keeping around references to Workers that will be
            // deallocated when `occupy` returns.
            Some(f(unsafe { &*worker_ptr }))
        } else {
            None
        }
    }

    /// Looks up the current `Worker` instance from the thread local.
    ///
    /// Rust's thread locals are fairly costly, so this function is expensive.
    /// If you can avoid calling it, do so.
    #[inline]
    pub fn with_current<F, R>(f: F) -> R
    where
        F: FnOnce(Option<&Worker>) -> R,
    {
        let worker_ptr = WORKER_PTR.with(Cell::get);
        if !worker_ptr.is_null() {
            // SAFETY: The `WORKER` static is only set by `occupy`, and it's
            // always set to a stack-allocated `Worker` which is never moved and
            // is only accseed through shared references. Therefore, if the
            // pointer is non-null, it must be safe to dereference.
            //
            // This creates a reference with an unbounded lifetime. To avoid
            // turning it into a `'static`, we pass it in to a closure. This
            // restricts it's lifetime to the closure body, and prevents callers
            // from keeping around references to Workers that will be
            // deallocated when `occupy` returns.
            f(Some(unsafe { &*worker_ptr }))
        } else {
            f(None)
        }
    }

    /// Temporarally sets the thread's worker. [`Worker::with_current`] always
    /// returns a reference to the worker set up by the most recent call to this
    /// worker.
    ///
    /// Rust's thread locals are fairly costly, so this function is expensive.
    /// If you can avoid calling it, do so.
    #[inline]
    pub fn occupy<F, R>(lease: Lease, f: F) -> R
    where
        F: FnOnce(&Worker) -> R,
    {
        trace!("occupying lease");

        let span = trace_span!("occupy", lease = lease.index);
        let _enter = span.enter();

        // Create a new worker to occupy the lease. Note: It's potentially a
        // problem that the same thread can occupy multiple workers on the same
        // thread. We many eventually need to design something to prevent this.
        let worker = Worker {
            lease,
            queue: JobQueue::new(),
        };

        // Swap the local pointer to point to the newly allocated worker.
        let outer_ptr = WORKER_PTR.with(|ptr| ptr.replace(&worker));

        // Run the function within the context created by the worker pointer,
        // and pass in a worker reference directly.
        let result = f(&worker);

        // Execute the work queue until it's empty
        while let Some(job_ref) = worker.queue.pop_front() {
            job_ref.execute(&worker);
        }

        // Swap back to pointing to the previous value (possibly null).
        WORKER_PTR.with(|ptr| ptr.set(outer_ptr));

        trace!("vacating lease");

        // Return the intermediate values created while running the closure,
        // namely the result and any jobs still remaining on the local queue.
        result
    }

    /// Tries to promote the oldest job in the local stack to a shared job. If
    /// the local job queue is empty, or if the shared queue is full, this does
    /// nothing. If the promotion is successful, it tries to wake another
    /// thread to accept the shared work. This is lock free.
    #[cold]
    fn promote(&self) {
        let mut state = self.lease.thread_pool.state.lock().unwrap();
        if let Entry::Vacant(e) = state.shared_jobs.entry(self.lease.index) {
            if let Some(job) = self.queue.pop_front() {
                e.insert(job);
                self.lease.thread_pool.job_is_ready.notify_one();
            }
        }
    }

    /// Runs jobs until the provided signal is received. When this thread runs
    /// out of local or shared work and the signal is still yet to be received,
    /// this puts the thread to sleep, and the thread will not wake again until
    /// the signal is received.
    ///
    /// # Panics
    ///
    /// This panics if a value has already been received over this signal. The
    /// caller must ensure this won't be the case.
    #[inline]
    pub fn wait_for_signal<T>(&self, signal: &Signal<T>) -> T
    where
        T: Send,
    {
        loop {
            // Shot-circuit if the signal has already been sent.
            //
            // Panics if a value has already been received over this signal.
            //
            // SAFETY: The `try_recv` and `recv` functions are only called in
            // this functionm, and are therefore only called on the current thread.
            if let Some(value) = unsafe { signal.try_recv() } {
                return value;
            }

            if self.yield_now() == Yield::Idle {
                // If we run out of jobs, just sleep until the signal is received.
                //
                // SAFETY: The `try_recv` and `recv` functions are only called in
                // this functionm, and are therefore only called on the current thread.
                return unsafe { signal.recv() };
            }
        }
    }

    /// Tries to find a job to execute, either in the local queue or shared on
    /// the threadpool.
    #[inline]
    pub fn find_work(&self) -> Option<JobRef> {
        // We give preference first to things in our local deque, then in other
        // workers deques, and finally to injected jobs from the outside. The
        // idea is to finish what we started before we take on something new.
        self.queue.pop_back().or_else(|| self.claim_shared_job())
    }

    /// Claims a shared job from the thread pool.
    #[cold]
    pub fn claim_shared_job(&self) -> Option<JobRef> {
        self.lease
            .thread_pool
            .state
            .lock()
            .unwrap()
            .claim_shared_job()
    }

    /// Cooperatively yields execution to the threadpool, allowing it to execute
    /// some work.
    #[inline]
    pub fn yield_local(&self) -> Yield {
        match self.queue.pop_back() {
            Some(job_ref) => {
                job_ref.execute(self);
                Yield::Executed
            }
            None => Yield::Idle,
        }
    }

    /// Cooperatively yields execution to the threadpool, allowing it to execute
    /// some work.
    #[inline]
    pub fn yield_now(&self) -> Yield {
        match self.find_work() {
            Some(job_ref) => {
                job_ref.execute(self);
                Yield::Executed
            }
            None => Yield::Idle,
        }
    }
}

// -----------------------------------------------------------------------------
// Worker scheduling api

impl Worker {
    /// Spawns a new closure onto the thread pool. Just like a standard thread,
    /// this task is not tied to the current stack frame, and hence it cannot
    /// hold any references other than those with 'static lifetime. If you want
    /// to spawn a task that references stack data, use the
    /// [`Worker::scope()`] function to create a scope.
    ///
    /// Since tasks spawned with this function cannot hold references into the
    /// enclosing stack frame, you almost certainly want to use a move closure
    /// as their argument (otherwise, the closure will typically hold references
    /// to any variables from the enclosing function that you happen to use).
    ///
    /// To spawn an async closure or future, use [`Worker::spawn_async`] or
    /// [`Worker::spawn_future`]. To spawn a non-static closure, use
    /// [`ThreadPool::scope`].
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::spawn`] or simply [`spawn`].
    #[inline]
    pub fn spawn<F>(&self, f: F)
    where
        F: FnOnce(&Worker) + Send + 'static,
    {
        // Allocate a new job on the heap to store the closure.
        let job = HeapJob::new(f);

        // Turn the job into an "owning" `JobRef` so it can be queued.
        //
        // SAFETY: All jobs added to the queue are garneteed to be executed
        // eventually, this is one of the core invariants of the thread pool.
        // The closure `f` has a static lifetime, meaning it only closes over
        // data that lasts for the duration of the program, so it's not possible
        // for this job to outlive the data `f` closes over.
        let job_ref = unsafe { job.into_job_ref() };

        // Queue the `JobRef` on the worker so that it will be evaluated.
        self.queue.push_back(job_ref);
    }

    /// Spawns a future onto the thread pool. See [`Worker::spawn`] for more
    /// information about spawning jobs. Only static futures are supported
    /// through this function, but you can use [`Worker::scope`] to get a scope
    /// on which non-static futures and async tasks can be spawned.
    ///
    /// # Returns
    ///
    /// Spawning a future returns a [`Task`], which represents a handle to the async
    /// computation and is itself a future that can be awaited to receive the
    /// return value. There's four ways to interact with a task:
    ///
    /// 1. Await the task. This will eventually produce the output of the
    ///    provided future.
    ///
    /// 2. Drop the task. This will stop execution of the future.
    ///
    /// 3. Cancel the task. This has the same effect as dropping the task, but
    ///    waits until the future stops running (which can take a while).
    ///
    /// 4. Detach the task. This will allow the future to continue executing
    ///    even after the task itself is dropped.
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::spawn_future`] or simply [`spawn_future`].
    #[inline]
    pub fn spawn_future<F, T>(&self, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.lease.thread_pool.spawn_future(future)
    }

    /// Spawns an async closure onto the task pool. This is a simple wrapper
    /// around [`Worker::spawn_future`].
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::spawn_async`] or simply [`spawn_async`].
    #[inline]
    pub fn spawn_async<Fn, Fut, T>(&self, f: Fn) -> Task<T>
    where
        Fn: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        self.lease.thread_pool.spawn_async(f)
    }

    /// Polls a future to completion, then returns the outcome. This function
    /// will prioritize polling the future as soon as it becomes available, and
    /// while the future is not available it will try to do other meaningfully
    /// work.
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::block_on`] or simply [`block_on`].
    #[inline]
    pub fn block_on<F, T>(&self, future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        // Create a new blocker, which will be used to block the thread until
        // the future completes.
        let blocker = Blocker::new();
        // Convert the blocker into an async waker.
        //
        // SAFETY: The blocker lasts for the duration of this function, and
        // since the waker is only used within this function, it must outlive
        // the waker.
        let waker = unsafe { blocker.as_waker() };
        // Put the waker into an async context that can be used to poll futures.
        let mut ctx = Context::from_waker(&waker);
        // Pin the future, promising not to move it while it's being polled.
        let mut future = pin!(future);
        // Execute other jobs while we wait for the future to complete.
        loop {
            match future.as_mut().poll(&mut ctx) {
                // While the future is incomplete, run other tasks or sleep.
                Poll::Pending => {
                    while blocker.would_block() {
                        if self.yield_now() == Yield::Idle {
                            blocker.block();
                            break;
                        }
                    }
                }
                // When it is complete, pull out the result and return it.
                Poll::Ready(res) => return res,
            }
        }
    }

    /// Takes two closures and *potentially* runs them in parallel, then returns
    /// the results.
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::join`] or simply [`join`].
    #[inline]
    pub fn join<A, B, RA, RB>(&self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce(&Worker) -> RA + Send,
        B: FnOnce(&Worker) -> RB + Send,
        RA: Send,
        RB: Send,
    {
        // Allocate a job to run the closure `a` on the stack.
        let stack_job = StackJob::new(a);

        // SAFETY: The `StackJob` is allocated on the stack just above, is never
        // moved, and so will live for the entirety of this function in the same
        // memory location. It closure `a` closes over data, that must be valid
        // for the lifetime of this function as well. The `JobRef` cannot
        // outlive either, because it is garneteed to be executed before the
        // function returns. We also clearly never create more than one `JobRef`
        // using the `stack_job`.
        let job_ref = unsafe { stack_job.as_job_ref() };

        // Store the id of the `JobRef` for later, when we will need it to
        // safely recover the closure `a` for inline execution.
        let job_ref_id = job_ref.id();

        // Push the job onto the queue.
        self.queue.push_back(job_ref);

        // Check for a heartbeat, potentially promoting the job we just pushed
        // to a shared job.
        if self.lease.heartbeat.load(Ordering::Relaxed) {
            self.promote();
            self.lease.heartbeat.store(false, Ordering::Relaxed);
        }

        // Run the second closure directly.
        let result_b = b(self);

        // Attempt to recover the job from the queue. It should still be there
        // if we didn't share it.
        if let Some(job) = self.queue.pop_back() {

            // If the shoe fits, this is our original `JobRef`, and we can
            // unwrap it to recover the closure `a` to execute it directly.
            if job.id() == job_ref_id {
                // SAFETY: Because the ids match, the JobRef we just popped from
                // the queue must point to `stack_job`, implying that
                // `stack_job` cannot have been executed yet.
                let a = unsafe { stack_job.unwrap() };
                // Execute the closure directly and return the results. This is
                // allows the compiler to inline and optimize `a`.
                let result_a = a(self);
                return (result_a, result_b);
            }
            
            // Even if it's not the droid we were looking for, we must still
            // execute the job.
            job.execute(self);
        }

        // Wait for the job to complete, then return the result.
        let result_a = self.wait_for_signal(stack_job.signal());
        (result_a, result_b)
    }

    /// Creates a scope on which non-static work can be spawned. Spawned jobs
    /// may run asynchronously with respect to the closure; they may themselves
    /// spawn additional tasks into the scope. When the closure returns, it will
    /// block until all tasks that have been spawned into onto the scope complete.
    ///
    /// It is also possible to crtate a new scope from a worker using
    /// [`Scope::new`], but it must be pinned before it can be used. This
    /// function mostly just does the pinning for you.
    ///
    /// If you do not have access to a [`Worker`], you may call
    /// [`ThreadPool::scope`] or simply [`scope`].
    #[inline]
    pub fn scope<'scope, F, T>(&self, f: F) -> T
    where
        F: FnOnce(Pin<&Scope<'scope>>) -> T + Send,
        T: Send,
    {
        let scope = pin!(Scope::new());
        let scope = scope.into_ref();
        f(scope)
    }
}

// -----------------------------------------------------------------------------
// Thread local scheduling api

/// Spawns a thread onto the current thread pool.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::spawn`] and [`ThreadPool::spawn`].
pub fn spawn<F>(f: F)
where
    F: FnOnce(&Worker) + Send + 'static,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::spawn` from outside a thread pool")
            .spawn(f);
    });
}

/// Spawns a future onto the current thread pool.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::spawn_future`] and [`ThreadPool::spawn_future`].
pub fn spawn_future<F, T>(future: F) -> Task<T>
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::spawn_future` from outside a thread pool")
            .spawn_future(future)
    })
}

/// Spawns an async closure onto the current thread pool.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::spawn_async`] and [`ThreadPool::spawn_async`].
pub fn spawn_async<Fn, Fut, T>(f: Fn) -> Task<T>
where
    Fn: FnOnce() -> Fut + Send + 'static,
    Fut: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    Worker::with_current(|worker| {
        worker
            .expect("attempt to call `forte::spawn_async` from outside a thread pool")
            .spawn_async(f)
    })
}

/// Blocks the thread waiting for a future to complete.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::spawn_future`] and [`ThreadPool::spawn_future`].
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

/// Executes two closures on the current thread pool and returns the results.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::join`] and [`ThreadPool::join`].
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

/// Creates a scope that allows spawning non-static jobs.
///
/// If there is no current thread pool, this panics.
///
/// See also: [`Worker::scope`] and [`Threadpool::scope`].
pub fn scope<'scope, F, T>(f: F) -> T
where
    F: FnOnce(Pin<&Scope<'scope>>) -> T + Send,
    T: Send,
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
fn managed_worker(lease: Lease, halt: Arc<AtomicBool>, #[cfg(not(loom))] barrier: Arc<Barrier>) {
    trace!("starting managed worker");

    barrier.wait();

    // Register as the indicated worker, and work until we are told to halt.
    Worker::occupy(lease, |worker| {
        loop {
            if let Some(job) = worker.queue.pop_back() {
                job.execute(worker);
                continue;
            }

            let mut state = worker.lease.thread_pool.state.lock().unwrap();

            while !halt.load(Ordering::Relaxed) {
                if let Some(job) = state.claim_shared_job() {
                    drop(state);
                    job.execute(worker);
                    break;
                }

                state = worker.lease.thread_pool.job_is_ready.wait(state).unwrap();
            }
        }
    });

    trace!("exiting managed worker");
}

// -----------------------------------------------------------------------------
// Heartbeat sender loop

/// This is the main loop for the heartbeat thread. It's in charge of
/// periodically sending a "heartbeat" signal to each worker. By default, each
/// worker receives a heartbeat about once every 100 μs.
///
/// Workers use the heartbeat signal to amortize the cost of promoting local
/// jobs to shared jobs (which allows other works to claim them) and to reduce
/// lock contention.
///
/// This is never run on loom,
#[cfg(not(loom))]
fn heartbeat_loop(thread_pool: &'static ThreadPool, halt: Arc<AtomicBool>) {
    use std::thread;
    use std::time::Instant;

    trace!("starting managed heartbeat thread");

    loop {
        if halt.load(Ordering::Relaxed) {
            break;
        }

        let mut state = thread_pool.state.lock().unwrap();

        let mut num_tenants = 0;
        let now = Instant::now();
        for tenancy in state.tenants.iter_mut() {
            if let Some(tenant) = tenancy {
                let Some(heartbeat) = tenant.heartbeat.upgrade() else {
                    *tenancy = None;
                    continue;
                };

                if now.duration_since(tenant.last_heartbeat) >= HEARTBEAT_INTERVAL {
                    heartbeat.store(true, Ordering::Relaxed);
                    tenant.last_heartbeat = now;
                }

                num_tenants += 1;
            }
        }

        drop(state);

        if num_tenants > 0 {
            let sleep_interval = HEARTBEAT_INTERVAL / num_tenants;
            thread::sleep(sleep_interval);
        }
    }
}
