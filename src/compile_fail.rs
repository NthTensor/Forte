//! Contains a set of compile failure doctests.

// -----------------------------------------------------------------------------
// Ensures non-send data cannot be moved into a join.

/** ```compile_fail,E0277

use std::rc::Rc;
use forte::ThreadPool;

static THREAD_POOL: ThreadPool = ThreadPool::new();

let r = Rc::new(22);
THREAD_POOL.join(|_| r.clone(), |_| r.clone());
//~^ ERROR

``` */
mod nonsend_input {}

// -----------------------------------------------------------------------------
// Ensures non-send data cannot be returned by join.

/** ```compile_fail,E0277

use std::rc::Rc;
use forte::ThreadPool;

static THREAD_POOL: ThreadPool = ThreadPool::new();

THREAD_POOL.join(|_| Rc::new(22), |_| ()); //~ ERROR

THREAD_POOL.depopulate();

``` */
mod nonsend_left_join {}

/** ```compile_fail,E0277

use std::rc::Rc;
use forte::ThreadPool;

static THREAD_POOL: ThreadPool = ThreadPool::new();

THREAD_POOL.join(|_| (), |_| Rc::new(23)); //~ ERROR

THREAD_POOL.depopulate();

``` */
mod nonsend_right_join {}

// -----------------------------------------------------------------------------
// Ensures scopes can not borrow data spawned within the closure.

/** ```compile_fail,E0373

use forte::ThreadPool;
use forte::Worker;

static THREAD_POOL: ThreadPool = ThreadPool::new();

fn bad_scope<F>(f: F)
    where F: FnOnce(&i32) + Send,
{
    THREAD_POOL.scope(|scope| {
        let x = 22;
        scope.spawn(|_: &Worker| f(&x)); //~ ERROR `x` does not live long enough
    });
}

fn good_scope<F>(f: F)
    where F: FnOnce(&i32) + Send,
{
    let x = 22;
    THREAD_POOL.scope(|scope| {
        scope.spawn(|_: &Worker| f(&x));
    });
}

fn main() { }

``` */
mod scope_join_bad {}

// -----------------------------------------------------------------------------
// Ensures the two branches of a join mutably borrow the same data.

/** ```compile_fail,E0524

use forte::ThreadPool;
use forte::Worker;

static THREAD_POOL: ThreadPool = ThreadPool::new();

fn quick_sort<T:PartialOrd+Send>(v: &mut [T]) {
    if v.len() <= 1 {
        return;
    }

    let mid = partition(v);
    let (lo, _hi) = v.split_at_mut(mid);
    THREAD_POOL.join(|_| quick_sort(lo), |_| quick_sort(lo)); //~ ERROR
}

fn partition<T:PartialOrd+Send>(v: &mut [T]) -> usize {
    let pivot = v.len() - 1;
    let mut i = 0;
    for j in 0..pivot {
        if v[j] <= v[pivot] {
            v.swap(i, j);
            i += 1;
        }
    }
    v.swap(i, pivot);
    i
}

fn main() { }

``` */
mod quicksort_race_1 {}

/** ```compile_fail,E0500

use forte::ThreadPool;
use forte::Worker;

static THREAD_POOL: ThreadPool = ThreadPool::new();

fn quick_sort<T:PartialOrd+Send>(v: &mut [T]) {
    if v.len() <= 1 {
        return;
    }

    let mid = partition(v);
    let (lo, _hi) = v.split_at_mut(mid);
    THREAD_POOL.join(|_| quick_sort(lo), |_| quick_sort(v)); //~ ERROR
}

fn partition<T:PartialOrd+Send>(v: &mut [T]) -> usize {
    let pivot = v.len() - 1;
    let mut i = 0;
    for j in 0..pivot {
        if v[j] <= v[pivot] {
            v.swap(i, j);
            i += 1;
        }
    }
    v.swap(i, pivot);
    i
}

fn main() { }

``` */
mod quicksort_race_2 {}

/** ```compile_fail,E0524

use forte::ThreadPool;
use forte::Worker;

static THREAD_POOL: ThreadPool = ThreadPool::new();

fn quick_sort<T:PartialOrd+Send>(v: &mut [T]) {
    if v.len() <= 1 {
        return;
    }

    let mid = partition(v);
    let (_lo, hi) = v.split_at_mut(mid);
    THREAD_POOL.join(|_| quick_sort(hi), |_| quick_sort(hi)); //~ ERROR
}

fn partition<T:PartialOrd+Send>(v: &mut [T]) -> usize {
    let pivot = v.len() - 1;
    let mut i = 0;
    for j in 0..pivot {
        if v[j] <= v[pivot] {
            v.swap(i, j);
            i += 1;
        }
    }
    v.swap(i, pivot);
    i
}

fn main() { }

``` */
mod quicksort_race_3 {}
