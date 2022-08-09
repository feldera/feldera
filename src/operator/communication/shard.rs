//! Operators to shard batches across multiple worker threads based on keys
//! and to gather sharded batches in one worker.

// TODOs:
// - different sharding modes.

use crate::{
    circuit::GlobalNodeId,
    circuit_cache_key,
    trace::{cursor::Cursor, spine_fueled::Spine, Batch, BatchReader, Builder, Trace},
    Circuit, Runtime, Stream,
};
use std::hash::Hash;

circuit_cache_key!(ShardId<C, D>((GlobalNodeId, ShardingPolicy) => Stream<C, D>));
circuit_cache_key!(GatherId<C, D>((GlobalNodeId, usize) => Stream<C, D>));

// An attempt to future-proof the design for when we support multiple sharding
// disciplines.
#[derive(Hash, PartialEq, Eq)]
pub struct ShardingPolicy;

fn sharding_policy<P>(_circuit: &Circuit<P>) -> ShardingPolicy {
    ShardingPolicy
}

impl<P, IB> Stream<Circuit<P>, IB>
where
    P: Clone + 'static,
    IB: BatchReader<Time = ()> + Clone + 'static,
    IB::Key: Ord + Clone + Hash,
    IB::Val: Ord + Clone,
{
    /// Shard batches across multiple worker threads based on keys.
    ///
    /// # Theory
    ///
    /// We parallelize processing across `N` worker threads by creating a
    /// replica of the same circuit per thread and sharding data across
    /// replicas.  To ensure correctness (i.e., that the sum of outputs
    /// produced by individual workers is equal to the output produced
    /// by processing the entire dataset by one worker), sharding must satisfy
    /// certain requirements determined by each operator.  In particular,
    /// for `distinct`, and `aggregate` all tuples that share the same key
    /// must be processed by the same worker.  For `join`, tuples from both
    /// input streams with the same key must be processed by the same worker.
    ///
    /// Other operators, e.g., `filter` and `flat_map`, impose no restrictions
    /// on the sharding scheme: as long as each tuple in a batch is
    /// processed by some worker, the correct result will be produced.  This
    /// is true for all linear operators.
    ///
    /// The `shard` operator shards input batches based on the hash of the key,
    /// making sure that tuples with the same key always end up at the same
    /// worker.  More precisely, the operator **re-shards** its input by
    /// partitioning batches in the input stream of each worker based on the
    /// hash of the key, distributing resulting fragments amond peers
    /// and re-assembling fragments at each peer:
    ///
    /// ```text
    ///         ┌──────────────────┐
    /// worker1 │                  │
    /// ───────►├─────┬───────────►├──────►
    ///         │     │            │
    /// ───────►├─────┴───────────►├──────►
    /// worker2 │                  │
    ///         └──────────────────┘
    /// ```
    ///
    /// # Usage
    ///
    /// Most users do not need to invoke `shard` directly (and doing so is
    /// likely to lead to incorrect results unless you know exactly what you
    /// are doing).  Instead, each operator re-shards its inputs as
    /// necessary, e.g., `join` applies `shard` to both of its
    /// input streams, while `filter` consumes its input directly without
    /// re-sharding.
    ///
    /// # Performance considerations
    ///
    /// In the current implementation, the `shard` operator introduces a
    /// synchronization barrier across all workers: its output at any worker
    /// is only produced once input batches have been collected from all
    /// workers.  This limits the scalability since a slow worker (e.g., running
    /// on a busy CPU core or sharing the core with other workers) or uneven
    /// sharding can slow down the whole system and reduce gains from
    /// parallelization.
    pub fn shard(&self) -> Stream<Circuit<P>, IB>
    where
        IB: Batch + Send,
    {
        // `shard_generic` returns `None` if there is only one worker thread
        // and hence sharding is a no-op.  In this case, we simply return the
        // input stream.  This allows us to use `shard` unconditionally without
        // incurring any overhead in the single-threaded case.
        self.shard_generic().unwrap_or_else(|| self.clone())
    }

    /// Like [`Self::shard`], but can assemble the results into any output batch
    /// type `OB`.
    ///
    /// Returns `None` when the circuit is not running inside a multithreaded
    /// rutime or is running in a runtime with a single worker thread.
    pub fn shard_generic<OB>(&self) -> Option<Stream<Circuit<P>, OB>>
    where
        OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R> + Clone + Send + 'static,
    {
        Runtime::runtime().and_then(|runtime| {
            let num_workers = runtime.num_workers();

            if num_workers == 1 {
                None
            } else {
                let output = self
                    .circuit()
                    .cache_get_or_insert_with(
                        ShardId::new((
                            self.origin_node_id().clone(),
                            sharding_policy(self.circuit()),
                        )),
                        move || {
                            // As a minor optimization, we reuse this array across all invocations
                            // of the sharding operator.
                            let mut builders = Vec::with_capacity(runtime.num_workers());
                            let (sender, receiver) = self.circuit().new_exchange_operators(
                                &runtime,
                                Runtime::worker_index(),
                                move |batch: IB, batches: &mut Vec<OB>| {
                                    Self::shard_batch(&batch, num_workers, &mut builders, batches);
                                },
                                |trace: &mut Spine<OB>, batch: OB| trace.insert(batch),
                            );
                            // Is `consolidate` always necessary? Some (all?) consumers may be happy
                            // working with traces.
                            self.circuit()
                                .add_exchange(sender, receiver, self)
                                .consolidate()
                        },
                    )
                    .clone();
                Some(output)
            }
        })
    }

    /// Collect all shards of a stream at the same worker.
    ///
    /// The output stream in `receiver_worker` will contain a union of all
    /// input batches across all workers.  The output streams in all other
    /// workers will contain empty batches.
    pub fn gather(&self, receiver_worker: usize) -> Stream<Circuit<P>, IB>
    where
        IB: Batch + Send,
    {
        match Runtime::runtime() {
            None => self.clone(),
            Some(runtime) => {
                let num_workers = runtime.num_workers();
                assert!(receiver_worker < num_workers);

                if num_workers == 1 {
                    self.clone()
                } else {
                    self.circuit()
                        .cache_get_or_insert_with(
                            GatherId::new((self.origin_node_id().clone(), receiver_worker)),
                            move || {
                                let (sender, receiver) = self.circuit().new_exchange_operators(
                                    &runtime,
                                    Runtime::worker_index(),
                                    move |batch: IB, batches: &mut Vec<IB>| {
                                        for _ in 0..num_workers {
                                            batches.push(IB::empty(()));
                                        }
                                        batches[receiver_worker] = batch;
                                    },
                                    |trace: &mut Spine<IB>, batch: IB| trace.insert(batch),
                                );
                                // Is `consolidate` always necessary? Some (all?) consumers may be
                                // happy working with traces.
                                self.circuit()
                                    .add_exchange(sender, receiver, self)
                                    .consolidate()
                            },
                        )
                        .clone()
                }
            }
        }
    }

    // Partitions the batch into `nshards` partitions based on the hash of the key.
    fn shard_batch<OB>(
        batch: &IB,
        nshards: usize,
        builders: &mut Vec<OB::Builder>,
        outputs: &mut Vec<OB>,
    ) where
        OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R>,
    {
        builders.clear();

        for _ in 0..nshards {
            // We iterate over tuples in the batch in order; hence tuples added
            // to each shard are also ordered, so we can use the more efficient
            // `Builder` API (instead of `Batcher`) to construct output batches.
            builders.push(OB::Builder::with_capacity((), batch.len() / nshards));
        }

        let mut cursor = batch.cursor();

        while cursor.key_valid() {
            let batch_index = fxhash::hash(cursor.key()) % nshards;
            while cursor.val_valid() {
                builders[batch_index].push((
                    OB::item_from(cursor.key().clone(), cursor.val().clone()),
                    cursor.weight(),
                ));
                cursor.step_val();
            }
            cursor.step_key();
        }

        for builder in builders.drain(..) {
            outputs.push(builder.done());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operator::Generator,
        trace::{Batch, BatchReader},
        Circuit, OrdIndexedZSet, Runtime,
    };

    #[test]
    fn test_shard() {
        do_test_shard(2);
        do_test_shard(4);
        do_test_shard(16);
    }

    fn test_data(worker_index: usize, num_workers: usize) -> OrdIndexedZSet<usize, usize, isize> {
        let tuples: Vec<_> = (0..1000)
            .filter(|n| n % num_workers == worker_index)
            .flat_map(|n| vec![((n, n), 1), ((n, 1000 * n), 1)])
            .collect();
        <OrdIndexedZSet<usize, usize, isize>>::from_tuples((), tuples)
    }

    fn do_test_shard(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            let circuit = Circuit::build(move |circuit| {
                let input = circuit.add_source(Generator::new(|| {
                    let worker_index = Runtime::worker_index();
                    let num_workers = Runtime::runtime().unwrap().num_workers();
                    test_data(worker_index, num_workers)
                }));
                input
                    .shard()
                    .gather(0)
                    .inspect(|batch: &OrdIndexedZSet<usize, usize, isize>| {
                        if Runtime::worker_index() == 0 {
                            assert_eq!(batch, &test_data(0, 1))
                        } else {
                            assert_eq!(batch.len(), 0);
                        }
                    });
            })
            .unwrap()
            .0;

            for _ in 0..3 {
                circuit.step().unwrap();
            }
        });

        hruntime.join().unwrap();
    }
}
