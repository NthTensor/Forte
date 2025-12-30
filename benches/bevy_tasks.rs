//! Comparative benchmarks against bevy_tasks

struct BevyParChunksMut<'a, T>(core::slice::ChunksMut<'a, T>);
impl<'a, T> bevy_tasks::ParallelIterator<core::slice::IterMut<'a, T>> for BevyParChunksMut<'a, T>
where
    T: 'a + Send + Sync,
{
    fn next_batch(&mut self) -> Option<core::slice::IterMut<'a, T>> {
        self.0.next().map(|s| s.iter_mut())
    }
}

static THREAD_POOL: forte::ThreadPool = forte::ThreadPool::new();

fn forte_chunks<const CHUNK_SIZE: usize, T, F>(worker: &forte::Worker, data: &mut [T], func: &F)
where
    T: Send + Sync,
    F: Fn(&mut [T]) + Send + Sync,
{
    if data.len() <= CHUNK_SIZE {
        func(data);
    } else {
        let split_index = data.len() / 2;
        let (left, right) = data.split_at_mut(split_index);
        worker.join(
            |worker| forte_chunks::<CHUNK_SIZE, _, _>(worker, left, func),
            |worker| forte_chunks::<CHUNK_SIZE, _, _>(worker, right, func),
        );
    }
}

#[divan::bench_group]
mod overhead {

    use divan::prelude::*;

    const LEN: &[usize] = &[100, 1000, 10_000, 100_000, 1_000_000, 10_000_000];

    fn work(value: &mut usize) {
        for i in 0..80 {
            black_box(i);
        }
        // std::thread::sleep(Duration::from_nanos(100));
        black_box(value);
    }

    #[divan::bench(args = LEN)]
    fn serial(bencher: Bencher, len: usize) {
        let mut vec: Vec<_> = (0..len).collect();
        bencher.bench_local(|| vec.iter_mut().for_each(work));
    }

    #[divan::bench(args = LEN)]
    fn bevy_tasks(bencher: Bencher, len: usize) {
        use crate::BevyParChunksMut;
        use bevy_tasks::ParallelIterator;

        let mut vec: Vec<_> = (0..len).collect();
        let pool = bevy_tasks::TaskPoolBuilder::new()
            .thread_name("bevy_tasks".to_string())
            .build();
        bencher.bench_local(|| {
            BevyParChunksMut(vec.chunks_mut(100)).for_each(&pool, work);
        });
    }

    #[divan::bench(args = LEN)]
    fn rayon(bencher: Bencher, len: usize) {
        use rayon::iter::ParallelIterator;
        use rayon::slice::ParallelSliceMut;

        let mut vec: Vec<_> = (0..len).collect();
        bencher.bench_local(|| {
            vec.par_chunks_mut(100)
                .for_each(|c| c.iter_mut().for_each(work))
        });
    }

    #[divan::bench(args = LEN)]
    fn forte(bencher: Bencher, len: usize) {
        use crate::THREAD_POOL;
        use crate::forte_chunks;

        let mut vec: Vec<_> = (0..len).collect();

        THREAD_POOL.resize_to_available();

        #[inline(always)]
        fn handle_chunk(chunk: &mut [usize]) {
            chunk.iter_mut().for_each(work);
        }

        bencher.bench_local(|| {
            THREAD_POOL.with_worker(|worker| {
                forte_chunks::<8, _, _>(worker, &mut vec, &handle_chunk);
            })
        });
    }
}

fn main() {
    divan::main();
}
