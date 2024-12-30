//! This is an example of how to integreat a threadpool with an external event
//! loop (winit in this case).

use std::sync::LazyLock;

use winit::{
    application::ApplicationHandler,
    event::WindowEvent,
    event_loop::{ActiveEventLoop, ControlFlow, EventLoop},
    window::WindowId,
};

use forte::job::{HeapJob, JobRef};
use forte::prelude::*;
use forte::thread_pool::WorkerThread;

static COMPUTE: ThreadPool = ThreadPool::new();

struct DemoApp;

impl ApplicationHandler<JobRef> for DemoApp {
    fn resumed(&mut self, event_loop: &ActiveEventLoop) {
        // Do whatever we normally do
    }

    fn window_event(
        &mut self,
        event_loop: &ActiveEventLoop,
        window_id: WindowId,
        event: WindowEvent,
    ) {
        // Do whatever we normally do
    }

    fn user_event(&mut self, event_loop: &ActiveEventLoop, job: JobRef) {
        // SAFTEY: Since (for now) we only pass static heap allocated job refs
        // to the main thread, the job-ref pointer will remain valid until it is
        // executed.
        unsafe {
            job.execute();
        }
    }
}

fn main() {
    // Resize the threadpool to the avalible number of threads.
    COMPUTE.resize_to_avalible();

    // Setup winit event loop.
    let event_loop = EventLoop::<JobRef>::with_user_event().build().unwrap();
    event_loop.set_control_flow(ControlFlow::Poll);

    // Spawn a bunch of tasks onto the pool.
    for i in 1..1000 {
        let main_thread_proxy = event_loop.create_proxy();
        COMPUTE.spawn(move || {
            // SAFTEY: This is executed on the thread pool so the worker thread must be non-null.
            let thread = unsafe { &*WorkerThread::current() };

            println!("Thread {} says {}", thread.index(), i);

            // Manually construct and send a job to the main thread
            let main_thread_job = HeapJob::new(move || println!("Main thread says {}", i));
            let job_ref = main_thread_job.into_static_job_ref();
            main_thread_proxy.send_event(job_ref);
        });
    }

    let mut app = DemoApp;
    event_loop.run_app(&mut app);
}
