# Forte

[![Crates.io](https://img.shields.io/crates/v/forte.svg)](https://crates.io/crates/forte)
[![Docs](https://docs.rs/forte/badge.svg)](https://docs.rs/forte/latest/forte/)

Forte is a low-overhead parallel & async work scheduler. It can be used as a
lower-overhead, lower-latency alternative to `rayon_core`, or as an async
executor (like `tokio`).

## Static + Resizable Thread-Pools

Thread pools are `const`-constructed, and intended to be defined as `static`
variables within a binary crate. Adding a new thread-pool to your project is as
simple as:

```rust
static THREAD_POOL: ThreadPool = ThreadPool::new();
```

Thread pools are empty when created, and can be resized on demand. Up to 32
threads can participate in a pool at a time (including worker threads and
non-worker threads making blocking calls to the pool).

```rust
// Add as many workers to the thread pool as you have cores in your computer.
THREAD_POOL.resize_to_available();

// Resize the thread-pool to have exactly five workers
THREAD_POOL.resize_to(5);

// Remove all workers from the pool and shut it down.
THREAD_POOL.depopulate();
```

## Fork-Join Parallelism

Forte provides an extremely low-overhead parallelization primitive for blocking
compute, similar to [`rayon::join`] or [`chili::Scope::join`]. At any point, it _may_
run the two closures in parallel.

```rust
fn sum(node: &Node, worker: &Worker) -> u64 {
    let (left, right) = worker.join(
        |w| node.left.as_deref().map(|n| sum(n, w)).unwrap_or_default(),
        |w| node.right.as_deref().map(|n| sum(n, w)).unwrap_or_default(),
    );
    
    node.val + left + right
}
```

This is optimized for depth-first traversal and hierarchical work-splitting,
where each of the closures passed to `join` potentially contains another call to
`join`.

## Spawn Closures & Futures

Forte also provides tools for load-balancing ultra-low-latency non-blocking
compute (like polling `Futures`), similar to [`rayon::spawn`] or
[`tokio::task::spawn`].

```rust
async fn serve() {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    let mut incoming = listener.incoming();

    while let Some(stream) = incoming.next().await {
        // A new task is spawned for each inbound tcp stream. The stream is
        // moved to the new task and processed there.
        let task = THREAD_POOL.spawn(async move {
            process(stream).await;
        });
        // Spawning a future gives us back a task handle we can use to await
        // its completion, but we don't care about that here. `detach` lets
        // drop the handle without canceling the stream-processing task.
        task.detach();
    }
}
```

## Scoped Spawns

For scheduling with non-static work, forte provides tools akin to
[`std::thread::scope`], [`tokio_scoped::scope`] or [`rayon::scope`].

```rust
let mut v = String::from("Hello");
forte::scope(|scope| {
    scope.spawn(|_: &Worker| {
        v.push('!');
    });
});
// The scope doesn't exit until all spawned work is complete.
assert_eq!(v.as_str(), "Hello!");
```

## Lazy Heartbeat Scheduling

Forte uses a combination of [_heartbeat scheduling_][hb] and [_lazy
scheduling_][lz] to achieve ultra-low overhead and minimize cpu-utilization.

The vast majority of operations are local and serial. Most jobs are stored in
simple double-ended queues, and adding new jobs to a worker has a zero-overhead
path without any shared data-structures.

Every worker also has a small fixed-capacity work-stealing queue (currently each
has space for 32 jobs). Approximately every 5us (gated by the CPU's instruction
counter) if there's space available, each worker pushes a small number of jobs
into this queue. When a worker runs out of jobs to execute, it briefly tries to
steal from its coworkers, then goes to sleep. 

This approach has several benefits over more brute-force applications of
work-stealing:

* For any particular time-slice, there is an upper-bound on the overhead due to
  synchronization. Since workers only touch shared data-structures every so
  often, it can only slow them down so much. This reduces runtime variance and
  lowers overhead.

* There is a cap on frequency at which local-work is made available for sharing.
  This reduces the probability that new work will become available at any given
  instant, which means (unlike many work-stealing implementations) it doesn't
  make sense to spin while trying to steal work. This can also reduce
  over-sharing at the tail-end of a parallel operation.
  
* The occupancy of the work-stealing queue represents an estimate of system
  load. When a worker's shared-queue is empty, that's a sign that some workers
  may be starved, and more tasks should be shared. By contrast, when a worker's
  shared-queue is full to capacity, that's a sign that the thread-pool may have
  reached full resource-utilization, and should avoid the costs of
  synchronization for a bit.
  
Jobs created by `join` are executed in LIFO order. When it comes time to share
work, the oldest `join` job is promoted into the shared work-stealing queue. In
the case of a binary tree, this means that execution progresses depth-first, but
sharing progresses breadth-first.

Jobs created by `spawn` are executed in FIFO order, to minimize latency. When it
comes time to share work, the newest `spawn` jobs are grouped into small batches
(16 jobs each) and those batches are promoted into the shared work-stealing
queue. This means that spawns generally stay on the thread that spawned them,
unless the thread is overwhelmed by an influx of new tasks.

# License

Forte is distributed under the terms of both the MIT license and the Apache
License (Version 2.0). See LICENSE-APACHE and LICENSE-MIT for details. 

Opening a pull request is assumed to signal agreement with these licensing terms.

[`rayon::join`]: https://docs.rs/rayon/latest/rayon/fn.join.html
[`chili::Scope::join`]: https://docs.rs/chili/latest/chili/struct.Scope.html#method.join
[`rayon::spawn`]: https://docs.rs/rayon/latest/rayon/fn.spawn.html
[`tokio::task::spawn`]: https://docs.rs/tokio/latest/tokio/task/fn.spawn.html
[`std::thread::scope`]: https://doc.rust-lang.org/std/thread/fn.scope.html
[`tokio_scoped::scope`]: https://docs.rs/tokio-scoped/latest/tokio_scoped/fn.scope.html
[`rayon::scope`]: https://docs.rs/rayon/latest/rayon/fn.scope.html

[hb]: https://www.andrew.cmu.edu/user/mrainey/heartbeat/heartbeat.html
[lz]: https://dl.acm.org/doi/10.1145/2629643
