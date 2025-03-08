//! Tests specifically for miri

#![cfg(miri)]

use forte::prelude::*;
use tracing::info;

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

#[test]
fn fork_join() {
    let layers = 10;
    let target = (1 << layers) - 1;

    static COMPUTE: ThreadPool = ThreadPool::new();

    fn sum(node: &Node, worker: &Worker) -> u64 {
        let (left, right) = worker.join(
            |w| node.left.as_deref().map(|n| sum(n, w)).unwrap_or_default(),
            |w| node.right.as_deref().map(|n| sum(n, w)).unwrap_or_default(),
        );

        node.val + left + right
    }

    let tree = Node::tree(layers);

    COMPUTE.as_worker(|worker| {
        let worker = worker.unwrap();
        COMPUTE.resize_to_available();
        info!("Work beginning");
        assert_eq!(sum(&tree, worker), target);
        info!("Work completed");
        COMPUTE.depopulate();
    });
}
