use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    future::Future,
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use async_task::{Runnable, Task};
use crossbeam_queue::SegQueue;
use crossbeam_utils::CachePadded;
use parking_lot::{Condvar, Mutex};

use crate::{
    job::{HeapJob, JobRef, StackJob},
    latch::{AtomicLatch, Latch, LockLatch, Probe, SetOnWake, WakeLatch},
    scope::*,
    util::{CallOnDrop, Slot, XorShift64Star},
};

// -----------------------------------------------------------------------------
// Thread pool types

/// This crate puts a hard upper-bound on the maximum size of a threadpool and
/// pre-allocates space for that number of threads.
///
/// I've chosen 32 as a reasonable default for development, as that's generally
/// the maximum number of threads avalible on flagship consumer hardware.
///
/// Note that this is only the hard upper bound on thread pool size. Thread
/// pools can be dynamically resized at runtime.
pub const MAX_THREADS: usize = 32;

/// The `ThreadPool` object is used to orchistrate and distribute work to a pool
/// of threads, and is generally the main entrypoint to using `Forte`.
///
/// # Creating Thread Pools
/// 
/// Thread pools should be static and const constructed. You don't have to worry
/// about `LazyStatic` or anything else; to create a new thread pool, just call
/// [`ThreadPool::new`].
///
/// ```
/// # use forte::prelude::*;
/// // Allocate a new thread pool.
/// static COMPUTE: ThreadPool = ThreadPool::new();
///
/// fn main() {
///     // Spawn a task onto the pool.
///     COMPUTE.spawn(|| {
///         println!("Do your work here");
///     });
/// }
/// ```
///
/// This attaches a new thread pool to your program named `COMPUTE`, which you
/// can begin to schedule work on immediately. The thread pool will exist for
/// the entire duration of your program, and will shut down when your program
/// completes.
///
/// # Resizing Thread Pools
///
/// Thread pools are dynamically sized; When your program starts they have size
/// zero (meaning no threads are running), and you will have to add threads by
/// resizing it. The simplest way to resize a pool is via
/// [`ThreadPool::resize_to_avalible`] which will simply fill all the avalible
/// space. More granular control is possible through other methods such as
/// [`ThreadPool::grow`], [`ThreadPool::shink`], or [`ThreadPool::resize_to`].
///
pub struct ThreadPool {
    /// This contains the shared state for each worker thread.
    threads: [CachePadded<ThreadInfo>; MAX_THREADS],
    /// A queue of pending jobs that can be taken by any thread. It uses the
    /// lock-free queue from crossbeam.
    queue: SegQueue<JobRef>,
    /// The thread pool state is a collection of infrequently modified shared
    /// data. It's bundled together into a cache line so that atomic writes
    /// don't cause unrelated cache-misses.
    state: CachePadded<ThreadPoolState>,
    /// The activity tally is used to determine if the thread pool is "active"
    /// which means basically that not everything spawned on the thread pool has
    /// run to completion.
    ///
    /// This is implemented as an atomic counter. The counter is incremented
    /// when work is sent to the thread pool and decremented when the work is
    /// completed.
    ///
    /// Closely related to `ThreadPoolState::is_active`.
    active_tally: CachePadded<AtomicUsize>,
}

/// Core information about the thread pool. This data may be read from
/// frequently and should only be written to infrequently.
struct ThreadPoolState {
    /// Tracks the number of currently runing threads, included currently
    /// sleeping threads. This should only be written to when the `is_resizing`
    /// mutex is held. It is not placed within the mutex because it can be
    /// safely read at any time.
    running_threads: AtomicUsize,
    /// A mutex used to guard the resizing critical section. 
    is_resizing: Mutex<bool>,
    /// Controlls for the thread that sends out heartbeat notifications.
    heartbeat_control: ThreadControl,
    /// This is set to `true` when `active_tally > 0`, and is used to wait for
    /// either activity or inactivity.
    is_active: Mutex<bool>,
    /// Used to send notifications to sleeping threads when `is_active` changes.
    activity_changed: Condvar,
}

/// Information for a specific worker thread.
struct ThreadInfo {
    /// This is the thread's "heartbeat": an atomic bool which is set
    /// periodically by a coordination thread. The heartbeat is used to
    /// "promote" local jobs to shared jobs.
    heartbeat: AtomicBool,
    /// Each worker may "share" one job, allowing other workers to claim it if
    /// they are busy. This is typically the last (oldest) job on their queue.
    shared_job: Slot<JobRef>,
    /// Information used to control the thread's lifecycle.
    control: ThreadControl,
}

/// This is a generalized control mechanism for a thread, implementing sleeping,
/// wakeups and a termination procedure. This is used by all the worker threads
/// and also the heartbeat-sender thread.
struct ThreadControl {
    /// Set to true when the worker is sleeping. Allows the thread to sleep
    /// until awakened by another thread.
    is_sleeping: Mutex<bool>,
    /// Used to wake a sleeping thread.
    awakened: Condvar,
    /// Set to true when the thread is running. Allows other threads to sleep
    /// until this thread stops or starts.
    is_running: Mutex<bool>,
    /// Used to wake other threads when the thread stops or starts.
    synchronized: Condvar,
    /// A latch that terminates the thread when set.
    should_terminate: AtomicLatch,
}

// -----------------------------------------------------------------------------
// Thread pool creation and mantinence

/// The inital value of `ThreadControl`. We have this instead of
/// `ThreadControl::new()` so that it can be used with the const array
/// initalization syntax.
const THREAD_CONTROL: ThreadControl = ThreadControl {
    is_sleeping: Mutex::new(false),
    awakened: Condvar::new(),
    is_running: Mutex::new(false),
    synchronized: Condvar::new(),
    should_terminate: AtomicLatch::new(),
};

/// The inital value of `ThreadInfo`, padded to fill a cache line. Again, we
/// have this instead of `ThreadInfo::new()` so that it can be used with the
/// const array initialization syntax.
const THREAD_INFO: CachePadded<ThreadInfo> = CachePadded::new(ThreadInfo {
    heartbeat: AtomicBool::new(false),
    shared_job: Slot::empty(),
    control: THREAD_CONTROL,
});

impl ThreadPool {
    /// Creates a new thread pool. This function should be used to define a
    /// `static` variable rather than to allocate something on the stack during
    /// runtime.
    pub const fn new() -> ThreadPool {
        ThreadPool {
            threads: [THREAD_INFO; MAX_THREADS],
            queue: SegQueue::new(),
            state: CachePadded::new(ThreadPoolState {
                running_threads: AtomicUsize::new(0),
                heartbeat_control: THREAD_CONTROL,
                is_resizing: Mutex::new(false),
                is_active: Mutex::new(false),
                activity_changed: Condvar::new(),
            }),
            active_tally: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Resizes the thread pool to fill all avalible space. After this returns,
    /// the pool will have at least one worker thread and at most `MAX_THREADS`.
    /// Returns the new size of the pool.
    ///
    /// See [`ThreadPool::resize`] for more information about resizing.
    pub fn resize_to_avalible(&'static self) -> usize {
        let available = thread::available_parallelism()
            .map(|num_threads| num_threads.get())
            .unwrap_or(1);
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
                if current_size == 0 {
                    1
                } else {
                    current_size
                }
            },
        )
    }

    /// Removes all worker threads from the thread pool. This should only be
    /// done cairfully, as blocking on an empty pool can cause a deadlock.
    ///
    /// See [`ThreadPool::resize_to`] for more information about resizing.
    pub fn depopulate(&'static self) -> usize {
        self.resize_to(0)
    }

    /// Resizes the pool.
    ///
    /// When called from within the pool, this does nothing. Pools can only be
    /// resized by other threads.
    pub fn resize<F>(&'static self, get_size: F) -> usize
    where
        F: Fn(usize) -> usize,
    {
        // We cannot shrink the pool from within the pool, so we simply refuse
        // the request and return the same size.
        if !WorkerThread::current().is_null() {
            return self.state.running_threads.load(Ordering::Acquire);
        }

        // Resizing is a critical section; only one thread is allowed to resize
        // the thread pool at a time. To ensure this exclusivity, we lock a
        // boolean mutex.
        let mut is_resizing = self.state.is_resizing.lock();
        *is_resizing = true;

        // Use the provided callback to determine the new size of the pool,
        // clamping it to the max size. We don't have to worry about race
        // conditions here because it's a critical section. The entire section
        // is effectively atomic.
        let current_size = self.state.running_threads.load(Ordering::Acquire);
        let new_size = usize::min(get_size(current_size), MAX_THREADS);

        // If the size is unchanged we can return early.
        if new_size == current_size {
            *is_resizing = false;
            return current_size;
        }

        // Otherwise we can store the new size. We still don't have to worry
        // about data races between this and the atomic load above because is
        // this the only place it's set, and we are in a critical section
        // guarded by the `is_resizing` mutex.
        //
        // At this point, other threads will begin to thread the thread pool as
        // having been resized, even though we have not actually created new or
        // cleaned up old threads yet.
        self.state
            .running_threads
            .store(new_size, Ordering::Release);

        // Now we spawn or terminate workers as required.
        if current_size < new_size {
            // Spawn each new thread.
            for i in current_size..new_size {
                // SAFETY: The main loop is the first thing called on the new
                // thread.
                self.threads[i]
                    .control
                    .run(move || unsafe { main_loop(self, i) });
            }

            // Wait for each thread to become ready.
            for i in new_size..current_size {
                self.threads[i].control.await_ready();
            }

            // Spawn the heartbeat thread if it's not running.
            if current_size == 0 {
                self.state
                    .heartbeat_control
                    .run(move || heartbeat_loop(self));
            }
            
        } else if current_size > new_size {
            // Ask each thread to terminate.
            for i in new_size..current_size {
                self.threads[i].control.halt();
            }

            // Wait for each thread to terminate.
            for i in new_size..current_size {
                self.threads[i].control.await_termination();
            }

            // Terminate the heartbeat thread if the pool is empty.
            if new_size == 0 {
                self.state.heartbeat_control.halt();
                self.state.heartbeat_control.await_termination();
            }
        }

        // Release the lock and return the new size.
        *is_resizing = false;
        new_size
    }

    /// Returns an opaque identifier for this thread pool.
    pub fn id(&'static self) -> usize {
        // We can rely on `self` not to change since it's a static ref.
        self as *const Self as usize
    }

    /// When called on a worker thread, this pushes the job directly into the
    /// local queue. Otherwise it injects it into the thread pool queue.
    pub fn inject_or_push(&'static self, job_ref: JobRef) {
        let worker_thread = WorkerThread::current();
        // SAFETY: We check if the worker thread is null and only dereference it
        // if we find that it is not.
        unsafe {
            if !worker_thread.is_null() && (*worker_thread).thread_pool().id() == self.id() {
                (*worker_thread).push(job_ref);
            } else {
                self.inject(job_ref);
            }
        }
    }

    /// Injects a job into the thread pool.
    pub fn inject(&'static self, job_ref: JobRef) {
        self.queue.push(job_ref);

        // If the pool is inactive, we have to wake a thread to work on this.
        if self.active_tally.load(Ordering::Relaxed) == 0 {
            self.wake_any(1);
        }
    }

    /// Pops a job from the thread pool's injector queue.
    pub fn pop(&'static self) -> Option<JobRef> {
        self.queue.pop()
    }

    /// Runs the provided function in one of this thread pool's workers. If
    /// called by a worker, the function is immediately run on that worker.
    /// Otherwise (if called from a thread belonging to a different thread pool
    /// or not belonging to a thread pool) the function is queued on the pool and
    /// executed as a job.
    ///
    /// This function blocks until the function is complete, possibly putting
    /// the current thread to sleep.
    pub fn in_worker<F, T>(&'static self, f: F) -> T
    where
        F: FnOnce(&WorkerThread, bool) -> T + Send,
        T: Send,
    {
        // If we are not in a worker, pack the function into a job and send it
        // to the global injector queue. This will block until the job completes.
        let worker_thread = WorkerThread::current();
        if worker_thread.is_null() {
            return self.in_worker_cold(f);
        }

        // SAFETY: We just checked that this pointer wasn't null.
        let worker_thread = unsafe { &*worker_thread };
        if worker_thread.thread_pool.id() != self.id() {
            // We are in a worker thread, but not in the same thread pool.
            // Package the job into a thread but then do idle work until it
            // completes.
            self.in_worker_cross(worker_thread, f)
        } else {
            // We are in a worker thread for the correct thread pool, so we can
            // just execute the function directly.
            f(worker_thread, false)
        }
    }

    /// Queues the provided closure for execution on a worker and then blocks
    /// the thread (with a mutex) until it completes.
    ///
    /// This is intended for situations where you want to run something in a
    /// worker from a non-worker thread. It's used to implement the public
    /// `in_worker` function just above.
    #[cold]
    fn in_worker_cold<F, T>(&'static self, f: F) -> T
    where
        F: FnOnce(&WorkerThread, bool) -> T + Send,
        T: Send,
    {
        thread_local!(static LOCK_LATCH: LockLatch = const { LockLatch::new() });

        // Ensure there is at least one worker in the pool to run on.
        let _ = self.populate();

        LOCK_LATCH.with(|latch| {
            let mut result = None;
            let job = StackJob::new(|| {
                // SAFETY: Since this is within a job, and jobs only execute on
                // worker threads, this must be non-null.
                let worker_thread = unsafe { &*WorkerThread::current() };

                // Run the user-provided function and write the output directly
                // to the result.
                result = Some(f(worker_thread, true));

                // SAFETY: This latch is static, so the pointer is always valid.
                unsafe { Latch::set(latch) };
            });

            // Inject the job into the thread pool for execution.

            // SAFETY: The job will remain valid until the end of this scope.
            // This scope will only end when the latch is set, and the latch
            // will only be set when the job executes, so this scope is valid
            // until the job executes.
            let job_ref = unsafe { job.as_job_ref() };
            self.inject(job_ref);

            // Block the thread until the job completes, then reset the latch.
            latch.wait_and_reset();

            // Return the result
            result.unwrap()
        })
    }

    /// Queues the provided closure for execution on a different worker, but
    /// keeps running tasks for the current worker.
    ///
    /// The `current_thread` is a worker from a different pool, which is queuing
    /// work into this pool, across thread pool boundaries.
    fn in_worker_cross<F, T>(&'static self, current_thread: &WorkerThread, f: F) -> T
    where
        F: FnOnce(&WorkerThread, bool) -> T + Send,
        T: Send,
    {
        // Ensure there is at least one worker in the pool to run on.
        let _ = self.populate();

        // Create a latch with a reference to the current thread.
        let latch = WakeLatch::new(current_thread);
        let mut result = None;
        let job = StackJob::new(|| {
            // SAFETY: Jobs are only executed on worker threads, so this must be
            // non-null.
            let worker_thread = unsafe { &*WorkerThread::current() };

            result = Some(f(worker_thread, true));

            // SAFETY: This latch is valid until this function returns, and it
            // does not return until the latch is set.
            unsafe { Latch::set(&latch) };
        });

        // SAFETY: This job is valid for this entire scope. The scope does not
        // exit until the function returns, the job does not return until the
        // latch is set, and the latch cannot be set until the job runs.
        let job_ref = unsafe { job.as_job_ref() };
        self.inject(job_ref);

        // Run tasks on the current thread until the job completes, possibly
        // putting the thread to sleep.
        current_thread.run_until(&latch);

        // Return the result.
        result.unwrap()
    }

    /// Tries to wake a number of threads. Returns the number of threads
    /// actually woken.
    pub fn wake_any(&'static self, num_to_wake: usize) -> usize {
        if num_to_wake > 0 {
            // Iterate through the threads, trying to wake each one until we run
            // out or have reached our target number.
            let mut num_woken = 0;
            let num_threads = self.state.running_threads.load(Ordering::Relaxed);
            for index in 0..num_threads {
                if self.wake_thread(index) {
                    num_woken += 1;
                    if num_to_wake == num_woken {
                        return num_woken;
                    }
                }
            }
            num_woken
        } else {
            0
        }
    }

    /// Wakes a worker that has gone to sleep. Returns true if the worker was
    /// woken up, false if it was already awake.
    pub fn wake_thread(&'static self, index: usize) -> bool {
        self.threads[index].control.wake()
    }

    /// Marks mew activity on the thread pool. This is what powers
    /// `wait_until_inactive`. A call to this should always be paired with a
    /// corresponding call to `mark_inactive` when the activity completes.
    ///
    /// What exactly counts as "activity" isn't strictly defined. It's anything
    /// we would want to wait for completion of in `wait_until_inactive`. It's
    /// mostly used for benchmarking purposes.
    pub fn mark_active(&'static self) {
        if self.active_tally.fetch_add(1, Ordering::AcqRel) == 0 {
            let mut is_active = self.state.is_active.lock();
            *is_active = true;
            self.state.activity_changed.notify_all();
        };
    }

    /// Marks the end of some thread pool activity. See `mark_active` for more
    /// information.
    pub fn mark_inactive(&'static self) {
        if self.active_tally.fetch_sub(1, Ordering::AcqRel) == 1 {
            let mut is_active = self.state.is_active.lock();
            *is_active = false;
            self.state.activity_changed.notify_all();
        }
    }

    /// Waits for the threadpool is inactive. Mostly used for benchmarking.
    pub fn wait_until_inactive(&'static self) {
        let mut is_active = self.state.is_active.lock();
        while *is_active {
            self.state.activity_changed.wait(&mut is_active);
        }
    }
}

// -----------------------------------------------------------------------------
// Thread control

impl ThreadControl {
    /// Spawns a thread with the provided closure. It's expected that this
    /// thread is given a reference to this `ThreadControl`.
    ///
    /// The `ThreadControl` api is split into two halves: one half is tended to
    /// be called by the "controller" (the thread that calls this) and the other
    /// half is intended to be called by the "worker" (the thread this spawns).
    fn run<F>(&'static self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(f);
    }

    /// The controller may call this to wait until the client calls
    /// `post_ready_status`.
    fn await_ready(&'static self) {
        let mut is_running = self.is_running.lock();
        while !*is_running {
            self.synchronized.wait(&mut is_running);
        }
    }

    /// The worker should call this to indicate that it is now entering it's
    /// main loop.
    fn post_ready_status(&'static self) {
        let mut is_running = self.is_running.lock();
        *is_running = true;
        self.synchronized.notify_all();
    }

    /// The controller should call this whenever it wishes to wake the worker.
    ///
    /// This assumes the worker has set `is_sleeping` to true and is waiting for
    /// `awakened`. There is no `sleep` function because sleeping behavior tends
    /// to be implementation specific.
    fn wake(&'static self) -> bool {
        let mut is_sleeping = self.is_sleeping.lock();
        if *is_sleeping {
            *is_sleeping = false;
            self.awakened.notify_one();
            true
        } else {
            false
        }
    }

    /// The controller should call this to tell the worker to exit it's main
    /// thread.
    ///
    /// This assumes the worker is looping waiting for `should_terminate` to be
    /// set.
    fn halt(&'static self) {
        unsafe { Latch::set(&self.should_terminate) }
        self.wake();
    }

    /// The controller may call this to wait until the client calls
    /// `post_termination_status`.
    fn await_termination(&'static self) {
        let mut is_running = self.is_running.lock();
        while *is_running {
            self.synchronized.wait(&mut is_running);
        }
    }

    /// The worker should call this right before it terminates.
    fn post_termination_status(&'static self) {
        self.should_terminate.reset();
        let mut is_running = self.is_running.lock();
        *is_running = false;
        self.synchronized.notify_all();
    }
}

// -----------------------------------------------------------------------------
// Core API

impl ThreadPool {
    /// Spawns a new closure onto the thread pool. Just like a standard thread,
    /// this task is not tied to the current stack frame, and hence it cannot
    /// hold any references other than those with 'static lifetime. If you want
    /// to spawn a task that references stack data, use the
    /// [`ThreadPool::scope()`] function to create a scope.
    ///
    /// Since tasks spawned with this function cannot hold references into the
    /// enclosing stack frame, you almost certainly want to use a move closure
    /// as their argument (otherwise, the closure will typically hold references
    /// to any variables from the enclosing function that you happen to use).
    ///
    /// To spawn an async task or future, use [`ThreadPool::spawn_async`] or
    /// [`ThreadPool::spawn_scope`].
    pub fn spawn<F>(&'static self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        // Marks some async thread pool activity. This is decremented at the (*).
        self.mark_active();
        let job = HeapJob::new(|| {
            f();
            self.mark_inactive(); // (*) Mark the activity complete.
        });
        let job_ref = job.into_static_job_ref();
        self.inject_or_push(job_ref);
    }

    /// Spawns a future onto the scope. See [`ThreadPool::spawn`] for more
    /// information about spawning jobs. Only static futures are supported
    /// through this function, but you can use `ThreadPool::scope` to get a scope
    /// on which non-static futures and async tasks can be spawned.
    ///
    /// # Returns
    ///
    /// This returns a task, which represents a handle to the async computation
    /// and is itself a future that can be awaited to receive the output of the
    /// future. There's four ways to interact with a task:
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
    pub fn spawn_future<F, T>(&'static self, future: F) -> Task<T>
    where
        F: Future<Output = T> + Send + 'static,
        T: Send + 'static,
    {
        // Marks some async thread pool activity. This is decremented at the (*).
        self.mark_active();
        let future = async move {
            // (*) Mark the activity complete when the future is dropped.
            let _guard = CallOnDrop(|| self.mark_inactive());
            future.await
        };

        // The schedule function will turn the future into a job when woken.
        let schedule = move |runnable: Runnable| {
            // Now we turn the runnable into a job-ref that we can send to a
            // worker.

            // SAFETY: We provide a pointer to a non-null runnable, and we turn
            // it back into a non-null runnable. The runnable will remain valid
            // until the task is run.
            let job_ref = unsafe {
                JobRef::new_raw(runnable.into_raw().as_ptr(), |this| {
                    let this = NonNull::new_unchecked(this as *mut ());
                    let runnable = Runnable::<()>::from_raw(this);
                    // Poll the task. This will drop the future if the task is
                    // canceled or the future completes.
                    runnable.run();
                })
            };

            // Send this job off to be executed. When this schedule function is
            // called on a worker thread this re-schedules it onto the worker's
            // local queue, which will generally cause tasks to stick to the
            // same thread instead of jumping around randomly. This is also
            // faster than injecting into the global queue.
            self.inject_or_push(job_ref);
        };

        // Creates a task from the future and schedule.
        let (runnable, task) = async_task::spawn(future, schedule);
        // Call the schedule function once to create the initial job.
        runnable.schedule();
        task
    }

    /// Like [`ThreadPool::spawn_future`] but accepts an async closure instead of
    /// a future. Here again everything must be static (but there is a
    /// non-static equivalent on [`Scope`]).
    ///
    /// Internally this wraps the closure into a new future and passes it along
    /// over to `spawn_future`.
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

    /// Polls a future to completion, then returns the outcome. This function
    /// will prioritize polling the future as soon as it becomes available, and
    /// while the future is not available it will try to do other meaningfully
    /// work (if executed on a thread pool) or block (if not executed on a thread
    /// pool).
    pub fn block_on<F, T>(&'static self, mut future: F) -> T
    where
        F: Future<Output = T> + Send,
        T: Send,
    {
        // We pin the future so that we can poll it.
        // SAFETY: This future is dropped at the end of this scope and is not
        // moved before then, so it is effectively pinned.
        let mut future = unsafe { Pin::new_unchecked(&mut future) };
        self.in_worker(|worker_thread, _| {
            // Create a callback that will wake this thread when the future is
            // ready to be polled again.
            let wake = SetOnWake::new(WakeLatch::new(worker_thread));
            let ctx_waker = Arc::clone(&wake).into();
            let mut ctx = Context::from_waker(&ctx_waker);
            // Keep polling the future, running work until it is woken up again.
            loop {
                match future.as_mut().poll(&mut ctx) {
                    Poll::Ready(res) => return res,
                    Poll::Pending => {
                        worker_thread.run_until(wake.latch());
                        wake.latch().reset();
                    }
                }
            }
        })
    }

    /// Takes two closures and *potentially* runs them in parallel. It returns a
    /// pair of the results from those closures. It is conceptually similar to
    /// spawning to threads, but it can be significantly faster due to
    /// optimizations in the thread pool.
    ///
    /// When called from outside the thread pool this will block until both
    /// closures are executed. When called within the thread pool, the worker
    /// thread will attempt to do other work while it's waiting.
    pub fn join<A, B, RA, RB>(&'static self, a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        self.in_worker(|worker_thread, _| {
            // We will execute `a` and create a job to run `b` in parallel.
            let mut status_b = None;
            // Create a new latch that can wake this thread when it completes.
            let latch_b = WakeLatch::new(worker_thread);
            // Create a job which runs b, returns the outcome back to the stack,
            // and trips the latch.
            let job_b = StackJob::new(|| {
                status_b = Some(b());
                // SAFETY: This job is valid until the end of the scope and is
                // not dropped until this function returns. The function does
                // not return until after the latch is set.
                unsafe { Latch::set(&latch_b) };
            });
            // SAFETY: This job is valid until the end of this scope, and is not
            // dropped until this function returns. The function does not return
            // until the latch is set, which can only happen when this job is
            // executed.
            let job_b_ref = unsafe { job_b.as_job_ref() };
            let job_b_ref_id = job_b_ref.id();
            worker_thread.push(job_b_ref);

            // Execute task A.
            let status_a = a();

            // We wait for `job_b` to complete. At this point we don't know if
            // `job_b` has been shared or is still somewhere on the local stack,
            // so we go hunting through the stack for it.
            while !latch_b.probe() {
                if let Some(job) = worker_thread.pop() {
                    if job.id() == job_b_ref_id {
                        // We found `job_b`, now we have to execute it. First we
                        // will try to share a job by calling `tick`. Normally
                        // this is done by `execute` but we have to call it
                        // manually here.
                        worker_thread.tick();
                        // Since we still are holding the original `job_b` we
                        // can run it without the indirection from the job-ref,
                        // allowing the compiler to optimize to closure.
                        job_b.run_inline();
                        // Having run the job we can break, since we know
                        // `latch_b` should now be set.
                        break;
                    } else {
                        // If it wasn't `job_b` we execute the job-ref normally.
                        worker_thread.execute(job);
                    }
                } else {
                    // We executed all our local jobs, so `job_b` must have been
                    // shared. We wait until it completes. This will put the
                    // thread to sleep at first, but it may wake up and do more
                    // work before this returns.
                    worker_thread.run_until(&latch_b);
                }
            }

            // Return the outcome of the two closures.
            (status_a, status_b.unwrap())
        })
    }

    /// Creates a scope on which new work can be spawned. Spawned jobs may run
    /// asynchronously with respect to the closure; they may themselves spawn
    /// additional tasks into the scope. When the closure returns, it will block
    /// until all tasks that have been spawned into `s` complete.
    ///
    /// This function allows spawning closures, futures and async closures with
    /// non-static lifetimes.
    pub fn scope<'scope, F, T>(&'static self, f: F) -> T
    where
        F: FnOnce(&Scope<'scope>) -> T + Send,
        T: Send,
    {
        self.in_worker(|owner_thread, _| {
            // SAFETY: The scope is completed before it is dropped.
            unsafe {
                let scope = Scope::<'scope>::new(owner_thread);
                let outcome = f(&scope);
                scope.complete(owner_thread);
                outcome
            }
        })
    }
}

// -----------------------------------------------------------------------------
// Worker threads

/// Data for a local worker thread, typically stored in a thread-local static.
pub struct WorkerThread {
    queue: UnsafeCell<VecDeque<JobRef>>,
    thread_pool: &'static ThreadPool,
    index: usize,
    rng: XorShift64Star,
}

thread_local! {
    static WORKER_THREAD_STATE: Cell<*const WorkerThread> = const { Cell::new(ptr::null()) };
}

impl WorkerThread {
    /// Returns access to the shared portion of this thread's data.
    #[inline]
    fn thread_info(&self) -> &ThreadInfo {
        &self.thread_pool.threads[self.index]
    }

    /// Returns a mutable reference to this worker's job queue.
    ///
    /// # Safety
    ///
    /// The caller must not call this function again until the returned
    /// reference is dropped. The simplest way to satisfy this requirement is by
    /// 1. ensuring the reference not returned from the calling function and,
    /// 2. not calling anything that calls this function while the reference is held.
    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_queue(&self) -> &mut VecDeque<JobRef> {
        &mut *self.queue.get()
    }

    /// Sets `self` as the worker thread index for the current thread.
    /// This is done during worker thread startup.
    ///
    /// # Safety
    ///
    /// This must be called only once per thread.
    unsafe fn set_current(&self) {
        WORKER_THREAD_STATE.with(|t| {
            assert!(t.get().is_null());
            t.set(self);
        });
    }

    /// Gets the `WorkerThread` for the current thread; returns NULL if this is
    /// not a worker thread. This pointer is valid anywhere on the current
    /// thread.
    #[inline]
    pub fn current() -> *const WorkerThread {
        WORKER_THREAD_STATE.with(Cell::get)
    }

    /// Returns the thread pool to which the worker belongs.
    #[inline]
    pub fn thread_pool(&self) -> &'static ThreadPool {
        &self.thread_pool
    }

    /// Returns the unique index of the thread within the thread pool.
    #[inline]
    pub fn index(&self) -> usize {
        self.index
    }

    /// Pushes a job onto the local queue. This operation is cheap and local,
    /// with no atomics or locks.
    #[inline]
    pub fn push(&self, job: JobRef) {
        // SAFETY: The job queue reference is not returned, and `job_queue` is
        // not called again within this scope.
        let local_queue = unsafe { self.get_queue() };
        // We treat the queue as a stack, with the newest jobs on the front and
        // the oldest on the back.
        local_queue.push_front(job);
    }

    /// Pops a job from the local queue. This operation is cheap and local, with
    /// no atomics or locks.
    #[inline]
    pub fn pop(&self) -> Option<JobRef> {
        // SAFETY: The job queue reference is not returned, and `job_queue` is
        // not called again within this scope.
        let local_queue = unsafe { self.get_queue() };
        // Pop a job from the front of the stack, where the jobs are newest.
        local_queue.pop_front()
    }

    /// Claims a shared job. Claiming jobs is lock free. This will do at most
    /// `MAX_THREADS` atomic read-modify-write operations and at at most one
    /// actual write. The worker will try to claim it's own shared job first.
    /// Otherwise it will try to claim shared jobs in sequence starting from a
    /// random other node.
    #[inline]
    pub fn claim_shared(&self) -> Option<JobRef> {
        // Try to reclaim this worker's shared job first.
        if let Some(job) = self.thread_info().shared_job.take() {
            return Some(job);
        }

        // Otherwise try to claim shared jobs from random other workers.
        let threads = self.thread_pool.threads.as_slice();
        let num_threads = self
            .thread_pool
            .state
            .running_threads
            .load(Ordering::Relaxed);
        let start = self.rng.next_usize(num_threads);
        (start..num_threads)
            .chain(0..start)
            .filter(move |&i| i != self.index())
            .find_map(|i| threads[i].shared_job.take())
    }

    /// Tries to poromote the oldest job in the local stack to a shared job. If
    /// the local job queue is empty, this does nothing. If the worker thread
    /// already has a shared job, this will instead try to wake one of the other
    /// thread to claim it.
    #[cold]
    fn promote(&self) {
        // SAFETY: The job queue reference is not returned, and `job_queue` is
        // not called again within this scope.
        let local_queue = unsafe { self.get_queue() };
        if let Some(job) = local_queue.pop_back() {
            // If there's work in the queue, pop it and try to share it
            if let Some(job) = self.thread_info().shared_job.put(job) {
                // If the shared slot is already occupied, put the job back on
                // the queue where it was.
                local_queue.push_back(job);
            } else {
                // Attempt to wake one other thread to claim this shared job.
                self.thread_pool.wake_any(1);
            }
        }
    }

    /// Promotes the oldest local job into a shared job which can be claimed and
    /// executed by other workers in the thread pool.
    ///
    /// This function is amortized. Promotion is somewhat expensive, so this
    /// function will only perform a promotion once in a fixed interval of time
    /// (the heartbeat interval).
    ///
    /// Many parts of the core thread pool api call this function automatically,
    /// but it can also be called manually by users.
    #[inline]
    pub fn tick(&self) {
        // Only runs the promotion if we have received the heartbeat signal. This
        // will happen infrequently so the promotion itself is marked cold.
        if self
            .thread_info()
            .heartbeat
            .compare_exchange(true, false, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            self.promote();
        }
    }

    /// Executes a job in the main loop.
    ///
    /// This call calls `tick`. Every so often, when the heartbeat signal is received, it will
    /// try to promote a local job to a shared job.
    #[inline]
    pub fn execute(&self, job: JobRef) {
        // Possibly promote a local job.
        self.tick();
        // Run the job.
        unsafe { job.execute() }
    }

    /// Runs until the provided latch is set. This will put the thread to sleep
    /// if no work can be found and the latch is still unset.
    #[inline]
    pub fn run_until<L: Probe>(&self, latch: &L) {
        if !latch.probe() {
            self.run_until_cold(latch);
        }
    }

    /// Runs until the provided latch is set. This will put the thread to sleep
    /// if no work can be found and the latch is still unset. Setting the latch
    /// will wake the thread.
    #[cold]
    fn run_until_cold<L: Probe>(&self, latch: &L) {
        while !latch.probe() {
            // Try to find work, either on the local queue, the shared jobs
            // vector, or the injector queue.
            if let Some(job) = self.find_work() {
                // SAFETY: No reference is held to the thread's job queue within
                // the main loop, and since it is thread-local there can be no
                // references anywhere.
                self.execute(job);
                continue;
            }

            let control = &self.thread_info().control;
            let mut is_sleeping = control.is_sleeping.lock();

            if latch.probe() {
                return;
            }

            *is_sleeping = true;
            while *is_sleeping {
                control.awakened.wait(&mut is_sleeping);
            }
        }
    }

    /// Looks for jobs for this worker to work on. It first pulls from the local
    /// queue, then the shared jobs, then the global injector queue.
    ///
    /// It can be as fast as a local deque pop, or as slow as a contested lock.
    #[inline]
    pub fn find_work(&self) -> Option<JobRef> {
        // First we try to pop a job off the local stack. This is an entirely
        // synchronous and local operation, with no atomics or locks.
        //
        // When there are no local jobs, we will try to claim one of the shared
        // jobs. This is more expensive and can result in contested locking.
        //
        // If there are no local jobs and no shared jobs, we will try to pop
        // work off the thread pool's injector queue. This is atomic but may
        // cause us to spin very briefly.
        self.pop()
            .or_else(|| self.claim_shared())
            .or_else(|| self.thread_pool().pop())
    }
}

// -----------------------------------------------------------------------------
// Main worker loop

/// This is the main loop for a worker thread. It's in charge of executing jobs.
/// Operating on the principle that you should finish what you start before
/// starting something new, workers will first execute their queue, then execute
/// shared jobs, then pull new jobs from the injector.
///
/// # Safety
///
/// This must not be called after `set_current` has been called. As a
/// consequence, this function cannot be called twice on the same thread.
unsafe fn main_loop(thread_pool: &'static ThreadPool, index: usize) {
    // Store a reference to this thread's control data.
    let control = &thread_pool.threads[index].control;
    
    // Register the worker on the thread.
    let worker_thread = WorkerThread {
        index,
        thread_pool,
        queue: UnsafeCell::new(VecDeque::with_capacity(32)),
        rng: XorShift64Star::new(index as u64 + 1),
    };

    // SAFETY: This function is the only thing that has been run on this thread,
    // so this will be called only once.
    unsafe {
        worker_thread.set_current();
    }

    // Inform other threads that we are starting the main worker loop.
    control.post_ready_status();

    // Run the worker thread until the thread is asked to terminate.
    worker_thread.run_until(&control.should_terminate);

    // Offload any remaining local work into the global queue.
    // SAFETY: We don't call `get_queue` ever again on this thread.
    let local_queue = unsafe { worker_thread.get_queue() };
    for job in local_queue.drain(..) {
        thread_pool.inject(job);
    }

    // If we had a shared job, push that to the global queue after all the local queue is pushed.
    if let Some(job) = worker_thread.thread_info().shared_job.take() {
        thread_pool.inject(job);
    }

    // Inform other threads that we are terminating.
    control.post_termination_status();
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
fn heartbeat_loop(thread_pool: &'static ThreadPool) {
    // Use a 100 microsecond heartbeat interval. Eventually this will be
    // configurable.
    let interval = Duration::from_micros(50);

    let control = &thread_pool.state.heartbeat_control;
    
    control.post_ready_status();

    // Loop as long as the thread pool is running.
    let mut i = 0;
    while !control.should_terminate.probe() {
        // Load the current number of running threads.
        let num_threads = thread_pool.state.running_threads.load(Ordering::Relaxed);

        // If there are no threads, automatically shut down.
        if num_threads == 0 {
            break;
        }

        // It's possible for the pool to be resized out from under us and end up
        // already above the number of threads. When that happens, we jump back
        // to zero.
        if i >= num_threads {
            i = 0;
            continue;
        }

        // Otherwise we will emit a heartbeat for the selected thread.
        thread_pool.threads[i]
            .heartbeat
            .store(true, Ordering::Relaxed);

        // Increment the thread index for the next iteration.
        i += 1;

        // We want to space out the heartbeat to each thread, so we divide the
        // interval by the current number of threads. When the thread pool is
        // not resized, this will mean we will stagger the heartbeats out evenly
        // and each thread will get a heartbeat on the given frequency.
        let interval = interval / num_threads as u32;

        // Sleep for the specified interval (or the thread is woken).
        let mut is_running = control.is_running.lock();
        control.awakened.wait_for(&mut is_running, interval);
    }

    control.post_termination_status();
}
