//! Unwinding recovery utilities taken from rayon.

use alloc::boxed::Box;
use core::any::Any;
use core::panic::AssertUnwindSafe;
use std::eprintln;
use std::panic::catch_unwind;
use std::panic::resume_unwind;
use std::process::abort;
use std::thread::Result;

/// Executes `f` and captures any panic, translating that panic into a
/// `Err` result. The assumption is that any panic will be propagated
/// later with `resume_unwinding`, and hence `f` can be treated as
/// exception safe.
#[inline(always)]
pub fn halt_unwinding<F, R>(func: F) -> Result<R>
where
    F: FnOnce() -> R,
{
    catch_unwind(AssertUnwindSafe(func))
}

#[cold]
pub fn resume_unwinding(payload: Box<dyn Any + Send>) -> ! {
    resume_unwind(payload)
}

/// Aborts the program when dropped.
pub struct AbortOnDrop;

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        eprintln!("Forte: detected unexpected panic; aborting");
        abort();
    }
}
