use std::{
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
};

pub static THREAD_POOL: forte::ThreadPool = forte::ThreadPool::new();

pub static STARTED: AtomicBool = AtomicBool::new(false);

#[inline(always)]
fn ensure_started() {
    if !STARTED.load(Ordering::Relaxed) {
        if !STARTED.swap(true, Ordering::Relaxed) {
            THREAD_POOL.resize_to_available();
        }
    }
}

// -----------------------------------------------------------------------------
// Join

#[derive(Debug)]
pub struct FnContext {
    /// True if the task was migrated.
    migrated: bool,
}

impl FnContext {
    #[inline]
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
            let ctx = FnContext {
                migrated: worker.migrated(),
            };
            oper_a(ctx)
        },
        |worker| {
            let ctx = FnContext {
                migrated: worker.migrated(),
            };
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

#[inline(always)]
pub fn current_num_threads() -> usize {
    64 // Forte prefers smaller tasks, so it's better to lie to rayon about the size of the pool
}

#[inline(always)]
pub fn current_thread_index() -> Option<usize> {
    forte::Worker::map_current(|worker| worker.index())
}

// -----------------------------------------------------------------------------
// Scope

pub struct Scope<'r, 'scope: 'r> {
    inner: Pin<&'r forte::Scope<'scope>>,
}

impl<'scope> Scope<'_, 'scope> {
    #[inline(always)]
    pub fn spawn<BODY>(&self, body: BODY)
    where
        BODY: FnOnce(&Scope) + Send + 'scope,
    {
        self.inner.spawn(|inner| {
            let scope = Scope { inner };
            body(&scope)
        });
    }
}

#[inline(always)]
pub fn scope<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'_, 'scope>) -> R + Send,
    R: Send,
{
    THREAD_POOL.scope(|inner| {
        let scope = Scope { inner };
        op(&scope)
    })
}

#[inline(always)]
pub fn in_place_scope<'scope, OP, R>(op: OP) -> R
where
    OP: FnOnce(&Scope<'_, 'scope>) -> R,
{
    THREAD_POOL.scope(|inner| {
        let scope = Scope { inner };
        op(&scope)
    })
}

// -----------------------------------------------------------------------------
// Spawn

#[inline(always)]
pub fn spawn<F>(func: F)
where
    F: FnOnce() + Send + 'static,
{
    THREAD_POOL.spawn(|_| func())
}

// -----------------------------------------------------------------------------
// Fake stuff that dosn't work

pub struct ThreadBuilder;

pub struct ThreadPool;

pub struct ThreadPoolBuildError;

pub struct ThreadPoolBuilder;

pub struct BroadcastContext;

pub struct ScopeFifo;

pub struct Yield;

pub fn broadcast() {
    unimplemented!()
}

pub fn spawn_broadcast() {
    unimplemented!()
}

pub fn max_num_threads() {
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

pub fn yield_local() {
    unimplemented!()
}

pub fn yield_now() {
    unimplemented!()
}
