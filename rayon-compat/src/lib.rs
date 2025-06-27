use std::sync::atomic::{AtomicBool, Ordering};

pub static THREAD_POOL: forte::ThreadPool = const { forte::ThreadPool::new() };

pub static STARTED: AtomicBool = const { AtomicBool::new(false) };

#[inline(always)]
fn ensure_started() {
    if !STARTED.load(Ordering::Relaxed) && !STARTED.swap(true, Ordering::Relaxed) {
        THREAD_POOL.resize_to_available();
    }
}

#[inline(always)]
pub fn current_num_threads() -> usize {
    64 // Forte prefers smaller tasks, so it's better to lie to rayon about the size of the pool
}

#[inline(always)]
pub fn current_thread_index() -> Option<usize> {
    forte::Worker::map_current(|worker| worker.index())
}

#[inline(always)]
pub fn max_num_threads() -> usize {
    usize::MAX // The number of forte workers is only bounded by the size of a vector.
}

// -----------------------------------------------------------------------------
// Join

#[derive(Debug)]
pub struct FnContext {
    /// True if the task was migrated.
    migrated: bool,
}

impl FnContext {
    #[inline(always)]
    pub fn migrated(&self) -> bool {
        self.migrated
    }
}

#[inline(always)]
pub fn join_context<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce(FnContext) -> RA + Send,
    B: FnOnce(FnContext) -> RB + Send,
    RA: Send,
    RB: Send,
{
    ensure_started();
    THREAD_POOL.join(
        |worker| {
            let migrated = worker.migrated();
            let ctx = FnContext { migrated };
            oper_a(ctx)
        },
        |worker| {
            let migrated = worker.migrated();
            let ctx = FnContext { migrated };
            oper_b(ctx)
        },
    )
}

#[inline(always)]
pub fn join<A, B, RA, RB>(oper_a: A, oper_b: B) -> (RA, RB)
where
    A: FnOnce() -> RA + Send,
    B: FnOnce() -> RB + Send,
    RA: Send,
    RB: Send,
{
    ensure_started();
    THREAD_POOL.join(|_| oper_a(), |_| oper_b())
}

// -----------------------------------------------------------------------------
// Scope

pub use forte::Scope;

#[inline(always)]
pub fn scope<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'scope>) -> R + Send,
    R: Send,
{
    ensure_started();
    forte::scope(op)
}

#[inline(always)]
pub fn in_place_scope<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'scope>) -> R,
{
    ensure_started();
    forte::scope(op)
}

// -----------------------------------------------------------------------------
// Spawn

#[inline(always)]
pub fn spawn<F>(func: F)
where
    F: FnOnce() + Send + 'static,
{
    ensure_started();
    THREAD_POOL.spawn(|_| func())
}

// -----------------------------------------------------------------------------
// Yield

pub use forte::Yield;

pub fn yield_local() -> Yield {
    let result = forte::Worker::map_current(forte::Worker::yield_local);
    match result {
        Some(status) => status,
        _ => Yield::Idle,
    }
}

pub fn yield_now() -> Yield {
    let result = forte::Worker::map_current(forte::Worker::yield_now);
    match result {
        Some(status) => status,
        _ => Yield::Idle,
    }
}

// -----------------------------------------------------------------------------
// Fake stuff that dosn't work. These are here only so so that rayon can export
// them.

pub struct ThreadBuilder;

pub struct ThreadPool;

pub struct ThreadPoolBuildError;

pub struct ThreadPoolBuilder;

pub struct BroadcastContext;

pub struct ScopeFifo;

pub fn broadcast() {
    unimplemented!()
}

pub fn spawn_broadcast() {
    unimplemented!()
}

pub fn scope_fifo() {
    unimplemented!()
}

pub fn in_place_scope_fifo() {
    unimplemented!()
}

pub fn spawn_fifo() {
    unimplemented!()
}
