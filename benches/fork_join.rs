//! A benchmark for fork-join workloads adapted from `chili`.

use chili::Scope;
use divan::Bencher;
use forte::Worker;
use tracing::debug;
use tracing::info;
use tracing_subscriber::fmt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

// -----------------------------------------------------------------------------
// Workload

/// A node in a binary tree.
struct Node {
    val: u64,
    left: Option<Box<Node>>,
    right: Option<Box<Node>>,
}

impl Node {
    // Constructs a new binary tree with the given number of layers.
    pub fn tree(layers: usize) -> Self {
        Self {
            val: 1,
            left: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
            right: (layers != 1).then(|| Box::new(Self::tree(layers - 1))),
        }
    }
}

// Returns an iterator over the number of layers. Also returns the total number
// of nodes.
const LAYERS: &[usize] = &[
    5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
];

fn nodes() -> impl Iterator<Item = (usize, usize)> {
    LAYERS.iter().map(|&l| (l, (1 << l) - 1))
}

// -----------------------------------------------------------------------------
// Benchmark

#[divan::bench(args = nodes())]
fn baseline(bencher: Bencher, nodes: (usize, usize)) {
    fn join_no_overhead<A, B, RA, RB>(a: A, b: B) -> (RA, RB)
    where
        A: FnOnce() -> RA + Send,
        B: FnOnce() -> RB + Send,
        RA: Send,
        RB: Send,
    {
        (a(), b())
    }

    #[inline]
    fn sum(node: &Node) -> u64 {
        let (left, right) = join_no_overhead(
            || node.left.as_deref().map(sum).unwrap_or_default(),
            || node.right.as_deref().map(sum).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);

    bencher.bench_local(move || {
        assert_eq!(sum(&tree), nodes.1 as u64);
    });
}

static COMPUTE: forte::ThreadPool = forte::ThreadPool::new();

#[divan::bench(args = nodes())]
fn forte(bencher: Bencher, nodes: (usize, usize)) {
    fn sum(node: &Node, worker: &Worker) -> u64 {
        let (left, right) = worker.join(
            |w| {
                let sum = node.left.as_deref().map(|n| sum(n, w)).unwrap_or_default();
                debug!("sum: {}", sum);
                sum
            },
            |w| {
                let sum = node.right.as_deref().map(|n| sum(n, w)).unwrap_or_default();
                debug!("sum: {}", sum);
                sum
            },
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);

    COMPUTE.with_worker(|worker| {
        info!("Staring Benchmark");
        bencher.bench_local(move || {
            assert_eq!(sum(&tree, worker), nodes.1 as u64);
        });
    });
}

#[divan::bench(args = nodes())]
fn chili(bencher: Bencher, nodes: (usize, usize)) {
    fn sum(node: &Node, scope: &mut Scope<'_>) -> u64 {
        let (left, right) = scope.join(
            |s| node.left.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
            |s| node.right.as_deref().map(|n| sum(n, s)).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);
    let mut scope = Scope::global();

    bencher.bench_local(move || {
        assert_eq!(sum(&tree, &mut scope), nodes.1 as u64);
    });
}

#[divan::bench(args = nodes())]
fn rayon(bencher: Bencher, nodes: (usize, usize)) {
    fn sum(node: &Node) -> u64 {
        let (left, right) = rayon::join(
            || node.left.as_deref().map(sum).unwrap_or_default(),
            || node.right.as_deref().map(sum).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(nodes.0);

    bencher.bench_local(move || {
        assert_eq!(sum(&tree), nodes.1 as u64);
    });
}

fn main() {
    let fmt_layer = fmt::layer()
        .without_time()
        .with_target(false)
        .with_thread_names(true)
        .compact();

    tracing_subscriber::registry().with(fmt_layer).init();

    COMPUTE.resize_to_available();

    divan::main();
}
