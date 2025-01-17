//! This is an example of how to integreat a threadpool with an external event
//! loop (winit in this case).

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
    fn resumed(&mut self, _event_loop: &ActiveEventLoop) {
        // Do whatever we normally do
    }

    fn window_event(
        &mut self,
        _event_loop: &ActiveEventLoop,
        _window_id: WindowId,
        _event: WindowEvent,
    ) {
        // Do whatever we normally do
    }

    fn user_event(&mut self, _event_loop: &ActiveEventLoop, job: JobRef) {
        // Execute a job-ref within the main loop.
        job.execute();
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
            WorkerThread::with(|thread| {
                // This is executed on the thread pool so the worker thread must be non-null.
                let thread = thread.unwrap();

                println!("Thread {} says {}", thread.index(), i);

                // Manually construct and send a job to the main thread.
                let main_thread_job = HeapJob::new(move || println!("Main thread says {}", i));
                // SAFETY: This job is executed once when it is sent to the executor.
                let job_ref = unsafe { main_thread_job.into_static_job_ref() };
                // Send the event to the main thread to be executed.
                let _result = main_thread_proxy.send_event(job_ref);
            });
        });
    }

    // Start the app.
    let mut app = DemoApp;
    let _result = event_loop.run_app(&mut app);
}
