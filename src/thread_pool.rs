use std::{
    cell::{Cell, UnsafeCell},
    collections::VecDeque,
    future::Future,
    mem,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::Arc,
    task::{Context, Poll},
    thread,
    time::Duration,
};

use async_task::{Runnable, Task};
use concurrent_queue::ConcurrentQueue;
use crossbeam_utils::CachePadded;
use parking_lot::{Condvar, Mutex};

use crate::{
    job::{HeapJob, JobRef, StackJob},
    latch::{AtomicLatch, Latch, LockLatch, Probe, SetOnWake, WakeLatch},
    scope::*,
    util::CallOnDrop,
};

// -----------------------------------------------------------------------------
// Thread pool types

/// TODO
pub struct ThreadPool {
    /// Immutable data about each worker thread. The number of threads in a
    /// pool is hard-capped at 32.
    threads: [ThreadInfo; 16],
    /// The number of currently running worker threads.
    running_threads: CachePadded<AtomicUsize>,
    /// Set when the threadpool is running.
    heartbeat_running: CachePadded<AtomicBool>,
    /// The global job injector queue. This is a queue of pending jobs that can
    /// be taken by any thread. It uses the lock-free injector queue
    /// implementation from crossbeam.
    queue: ConcurrentQueue<JobRef>,
    /// Holds prevent the registry from terminating.
    hold_count: CachePadded<AtomicUsize>,
}

/// Information for a specific worker thread.
struct ThreadInfo {
    /// This is the thread's "heartbeat": an atomic bool which is set
    /// periodically by a coordination thread. The heartbeat is used to
    /// "promote" local jobs to shared jobs.
    heartbeat: CachePadded<AtomicBool>,
    /// Each worker may "share" one job, allowing other workers to claim it if
    /// they are busy. This is typically the last (oldest) job on their queue.
    ///
    /// To reduce global lock contention, these shared jobs are stored behind
    /// individual mutex lock. We should try to keep contention on the shared
    /// job locks as low as possible.
    shared_job: CachePadded<Mutex<Option<SharedJob>>>,
    /// Blocking synchronization data for the thread.
    sleep_state: CachePadded<SleepState>,
    /// Set to true when the thread descriptor is occupied by a thread.
    occupied: AtomicBool,
    /// A latch that terminates the worker thread when set.
    terminate: AtomicLatch,
}

/// Allows a thread to send itself to sleep and be re-awoken with a notification.
struct SleepState {
    /// Set to true when the worker is blocked (sleeping).
    is_blocked: Mutex<bool>,
    /// Condition used together with `is_blocked` to wake the thread.
    should_wake: Condvar,
}

/// This represents a job that may be claimed by any worker, but currently is
/// assigned to a specific worker.
struct SharedJob {
    /// When a shared job is created, this number is set to zero and increments
    /// each time a worker receives a heartbeat. This allows workers to
    /// prioritize older jobs when looking for shared jobs to claim.
    age: u64,
    /// The reference to the job.
    job_ref: JobRef,
}

// -----------------------------------------------------------------------------
// ThreadPool implementation

const THREAD_INFO: ThreadInfo = ThreadInfo {
    heartbeat: CachePadded::new(AtomicBool::new(false)),
    shared_job: CachePadded::new(Mutex::new(None)),
    sleep_state: CachePadded::new(SleepState {
        is_blocked: Mutex::new(false),
        should_wake: Condvar::new(),
    }),
    occupied: AtomicBool::new(false),
    terminate: AtomicLatch::new(),
};

impl ThreadPool {
    pub const fn new() -> ThreadPool {
        ThreadPool {
            threads: [THREAD_INFO; 16],
            running_threads: CachePadded::new(AtomicUsize::new(0)),
            heartbeat_running: CachePadded::new(AtomicBool::new(false)),
            queue: ConcurrentQueue::unbounded(),
            hold_count: CachePadded::new(AtomicUsize::new(1)),
        }
    }

    /// Starts the heartbeat thread.
    pub fn start_heartbeat(&'static self) {
        if self.heartbeat_running.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            // The thread pool is not running. Start the heartbeat thread.
            thread::spawn(move || heartbeat_loop(self) );
        }
    }

    /// Resizes the threadpool.
    pub fn resize(&'static self, new_size: usize) {
        // Swap out the size first, then do the work to move from the old size
        // to the new size.
        let old_size = self.running_threads.swap(new_size, Ordering::Relaxed);

        if old_size < new_size {
            // Increase the size of the pool.
            for i in old_size..new_size {
                if self.threads[i].occupied.load(Ordering::Relaxed) {
                    // If this index is already occupied by a thread, it must be
                    // in the process of terminating. We will end it's
                    // termination and allow it to continue.
                    self.threads[i].terminate.reset();
                } else {
                    // If the index is not occupied by a thread, 
                    thread::spawn(move || unsafe { main_loop(self, i) });
                }
            }
        } else if old_size > new_size {
            // Decrease the size of the pool. Ask each thread to terminate.
            for i in new_size..old_size {
                // SAFETY: This latch has a static lifetime.
                unsafe { Latch::set(&self.threads[i].terminate) };
            }
        }

        // Start the thread pool if it's not already been started.
        self.start_heartbeat();
    }

    /// Resizes the threadpool to fit the available parallelism.
    pub fn resize_to_avalible(&'static self) {
        let num_threads = thread::available_parallelism()
            .map(|num_threads| num_threads.get())
            .unwrap_or(1);
        self.resize(num_threads);
    }
    
    /// Returns an opaque identifier for this registry.
    pub fn id(&'static self) -> usize {
        // We can rely on `self` not to change since it's a static ref.
        self as *const Self as usize
    }

    /// When called on a worker thread, this injects the job directly into the
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
        let num_jobs = self.queue.len();
        let _ = self.queue.push(job_ref);
        // If the queue is non-empty, then we always wake up a worker -- clearly
        // the existing idle jobs aren't enough.
        if num_jobs != 0 {
            self.wake_any(1);
        }
    }

    /// Pops a job from the thread pool. This will try three times to get a job,
    /// and return None if it still can't.
    pub fn pop(&'static self) -> Option<JobRef> {
        self.queue.pop().ok()
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
            // We are in a worker thread, but not in the same registry. Package
            // the job into a thread but then do idle work until it completes.
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
        thread_local!(static LOCK_LATCH: LockLatch = LockLatch::new());

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
            let num_threads = self.running_threads.load(Ordering::Relaxed);
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
    ///
    /// This sets a mutex, but it should basically never be contested.
    pub fn wake_thread(&'static self, index: usize) -> bool {
        let thread = &self.threads[index];
        let mut is_blocked = thread.sleep_state.is_blocked.lock();
        if *is_blocked {
            // The thread has `is_blocked = true` which indicates that it is
            // actually asleep. We set it to false to kick it out of it's sleep
            // loop and notify it to wake up.
            *is_blocked = false;
            thread.sleep_state.should_wake.notify_one();
            true
        } else {
            // The thread was not asleep.
            false
        }
    }

    /// Adds a hold to the registry. It will not terminate until a after a
    /// corresponding `remove_hold` call.
    pub fn add_hold(&'static self) {
        self.hold_count.fetch_add(1, Ordering::AcqRel);
    }

    /// Removes a hold on the registry. When there are zero holds on the
    /// registry, it will shut itself down.
    pub fn remove_hold(&'static self) {
        if self.hold_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            // If the previous value was 1 then no holds remain. The registry
            // can now ask the threads to terminate.
            for (index, thread) in self.threads.iter().enumerate() {
                // SAFETY: The latch is valid for the entire scope.
                unsafe { Latch::set(&thread.terminate) };
                self.wake_thread(index);
            }
            // This tells the heartbeat thread to shut down.
            self.heartbeat_running.store(false, Ordering::Relaxed);
        }
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
        // Ensure that registry cannot terminate until this job has
        // executed. This ref is decremented at the (*) below.
        self.add_hold();
        let job = HeapJob::new({
            move || {
                f();
                self.remove_hold(); // (*) Remove the hold on the registry.
            }
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
        // Ensure that registry cannot terminate until this job has
        // executed. This ref is decremented at the (*) below.
        self.add_hold();
        let future = async move {
            let _guard = CallOnDrop(move || {
                self.remove_hold(); // (*) Remove the hold on the registry.
            });
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
                JobRef::new_raw(
                    runnable.into_raw().as_ptr(),
                    |this| {
                        let this = NonNull::new_unchecked(this as *mut ());
                        let runnable = Runnable::<()>::from_raw(this);
                        // Poll the task.
                        runnable.run();
                    }
                )
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
        let future = async move {
            f().await
        };
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
            // We create a async waker which will wake the thread when the
            // future is awoken. This latch will also keep the registry alive.
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
    job_queue: UnsafeCell<VecDeque<JobRef>>,
    thread_pool: &'static ThreadPool,
    index: usize,
}

thread_local! {
    static WORKER_THREAD_STATE: Cell<*const WorkerThread> = const { Cell::new(ptr::null()) };
}

impl WorkerThread {
    /// Returns access to the this thread's section of the registry.
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
    unsafe fn job_queue(&self) -> &mut VecDeque<JobRef> {
        &mut *self.job_queue.get()
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
        let job_queue = unsafe { self.job_queue() };
        // We treat the queue as a stack, with the newest jobs on the front and
        // the oldest on the back.
        job_queue.push_front(job);
    }

    /// Pops a job from the local queue. This operation is cheap and local, with
    /// no atomics or locks.
    #[inline]
    pub fn pop(&self) -> Option<JobRef> {
        // SAFETY: The job queue reference is not returned, and `job_queue` is
        // not called again within this scope.
        let job_queue = unsafe { self.job_queue() };
        // Pop a job from the front of the stack, where the jobs are newest.
        job_queue.pop_front()
    }

    /// Claims a shared job. Will try to reclaim the worker's shared job first,
    /// and then will try to claim the oldest job.
    ///
    /// Only the "owner" of the shared job is allowed to hard-lock on it. Other
    /// threads always use `try_lock` and skip if not
    #[inline]
    pub fn claim_shared(&self) -> Option<JobRef> {
        // Try to reclaim this worker's own shared job first.
        if let Some(shared_job) = mem::take(self.thread_info().shared_job.lock().deref_mut()) {
            return Some(shared_job.job_ref);
        }

        // If that doesn't work, we will try to find the oldest job shared by a
        // different worker and claim that. We will store a lock on the oldest
        // job we have found along with it's age.
        let mut candidate_job_lock = None;
        // Shared job age starts at 1, so setting this to 0 initially means we
        // will take the first job we find.
        let mut candidate_age = 0;

        // We will iterate over all threads in index order, starting with the
        // next thread. We do this as an alternative to starting with thread 0
        // to reduce lock contention in the event that multiple threads wake
        // simultaneously.
        let current_thread = self.index();
        let num_threads = self.thread_pool.running_threads.load(Ordering::Relaxed);
        for i in 0..num_threads {
            let i = (current_thread + 1 + i) % num_threads;
            let thread = &self.thread_pool.threads[i];

            // Now we will try to acquire a lock on the next shared job. At this
            // point, it's possible we already hold a lock on another shared job
            // (the candidate). To avoid deadlocks, we will simply skip shared
            // jobs that are already locked.
            if let Some(new_job_lock) = thread.shared_job.try_lock() {
                // Promote the new job to the candidate if it's older.
                if let Some(new_job) = new_job_lock.deref() {
                    if new_job.age > candidate_age {
                        candidate_age = new_job.age;
                        // This releases the lock on the previous best job and
                        // replaces it with the lock on the new best job.
                        candidate_job_lock = Some(new_job_lock);
                    }
                }
            }
        }

        // If we have a locked job candidate, take it.
        if let Some(mut job_lock) = candidate_job_lock {
            let shared = mem::take(job_lock.deref_mut());
            return Some(shared.unwrap().job_ref);
        }

        None
    }

    /// Pops a job off the local queue and promotes it to a shared job. If the
    /// local job queue is empty, this does nothing. If the worker has an
    /// existing shared job, it increment that job's age.
    #[cold]
    fn promote(&self) {
        // SAFETY: The job queue reference is not returned, and `job_queue` is
        // not called again within this scope.
        let job_queue = unsafe { self.job_queue() };
        if job_queue.is_empty() {
            return;
        }
        // Acquire the lock on the shared job
        let mut shared_job_lock = self.thread_info().shared_job.lock();
        if let Some(ref mut shared_job) = shared_job_lock.deref_mut() {
            // If a shared job already exists, increase it's age.
            shared_job.age += 1;
        } else {
            // No shared job exists, so pop the oldest task off the back of the
            // stack and share it.
            *shared_job_lock = Some(SharedJob {
                job_ref: job_queue.pop_back().unwrap(),
                age: 1, // Starts at 1 so that we can use 0 default in `claim_shared`.
            });
        }
        // Attempt to wake one other thread to claim this shared job.
        self.thread_pool.wake_any(1);
    }

    /// Promotes the oldest local job into a shared job which can be claimed and
    /// executed by other workers in the thread pool.
    ///
    /// This function is amortized. Promotion is expensive, so this function
    /// will only perform a promotion once in a fixed interval of time (the
    /// heartbeat interval).
    ///
    /// Many parts of the core thread pool api call this function automatically,
    /// but it can also be called manually by users.
    #[inline]
    pub fn tick(&self) {
        // Only runs the promotion if we are received the heartbeat signal. This
        // will happen infrequently so the promotion itself is marked cold.
        if self.thread_info().heartbeat.load(Ordering::Relaxed) {
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

            let thread = self.thread_info();
            let mut is_blocked = thread.sleep_state.is_blocked.lock();

            if latch.probe() {
                return;
            }

            *is_blocked = true;
            while *is_blocked {
                thread.sleep_state.should_wake.wait(&mut is_blocked);
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
    // Register the worker on the thread.
    let worker_thread = WorkerThread {
        index,
        thread_pool,
        job_queue: UnsafeCell::new(VecDeque::with_capacity(32)),
    };

    // SAFETY: This function is the only thing that has been run on this thread,
    // so this will be called only once.
    unsafe {
        worker_thread.set_current();
    }

    // Run the worker thread until the registry is terminated.
    worker_thread.run_until(&thread_pool.threads[index].terminate);
}

// -----------------------------------------------------------------------------
// Heartbeat loop

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
    let interval = Duration::from_micros(100);
    
    // Loop as long as the thread pool is running.
    let mut i = 0;
    while thread_pool.heartbeat_running.load(Ordering::Relaxed) {
        // Load the current number of running threads from the registry.
        let num_threads = thread_pool.running_threads.load(Ordering::Relaxed);

        // If there are no threads, shut down.
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
        thread_pool.threads[i].heartbeat.store(true, Ordering::Relaxed);
        i = (i + 1) % num_threads;
        
        // We want to space out the heartbeat to each thread, so we divide the
        // interval by the current number of threads. When the thread pool is
        // not resized, this will mean we will stagger the heartbeats out evenly
        // and each thread will get a heartbeat on the given frequency.
        let interval = interval / num_threads as u32;
        thread::sleep(interval);
    }
}
