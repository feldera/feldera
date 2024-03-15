//! Operators to shard batches across multiple worker threads based on keys
//! and to gather sharded batches in one worker.

// TODOs:
// - different sharding modes.

use crate::{
    circuit::GlobalNodeId,
    circuit_cache_key,
    dynamic::Data,
    operator::communication::new_exchange_operators,
    trace::{merge_batches, Batch, BatchReader, BatchReaderFactories, Builder, Cursor},
    Circuit, Runtime, Stream,
};

use crate::dynamic::ClonableTrait;
use std::{hash::Hash, panic::Location};

circuit_cache_key!(ShardId<C, D>((GlobalNodeId, ShardingPolicy) => Stream<C, D>));
circuit_cache_key!(UnshardId<C, D>(GlobalNodeId => Stream<C, D>));

// An attempt to future-proof the design for when we support multiple sharding
// disciplines.
#[derive(Hash, PartialEq, Eq)]
pub struct ShardingPolicy;

fn sharding_policy<C>(_circuit: &C) -> ShardingPolicy {
    ShardingPolicy
}

impl<C, IB> Stream<C, IB>
where
    C: Circuit,
    IB: BatchReader<Time = ()> + Clone,
{
    /// See [`Stream::shard`].
    #[track_caller]
    pub fn dyn_shard(&self, factories: &IB::Factories) -> Stream<C, IB>
    where
        IB: Batch + Send,
    {
        // `shard_generic` returns `None` if there is only one worker thread
        // and hence sharding is a no-op.  In this case, we simply return the
        // input stream.  This allows us to use `shard` unconditionally without
        // incurring any overhead in the single-threaded case.
        self.dyn_shard_generic(factories)
            .unwrap_or_else(|| self.clone())
    }

    /// Like [`Self::dyn_shard`], but can assemble the results into any output batch
    /// type `OB`.
    ///
    /// Returns `None` when the circuit is not running inside a multithreaded
    /// runtime or is running in a runtime with a single worker thread.
    #[track_caller]
    pub fn dyn_shard_generic<OB>(&self, factories: &OB::Factories) -> Option<Stream<C, OB>>
    where
        OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R> + Send,
    {
        let location = Location::caller();

        Runtime::runtime().and_then(|runtime| {
            let num_workers = runtime.num_workers();
            let factories_clone = factories.clone();

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
                            let factories_clone2 = factories_clone.clone();
                            let factories_clone3 = factories_clone.clone();

                            let (sender, receiver) = new_exchange_operators(
                                &runtime,
                                Runtime::worker_index(),
                                Some(location),
                                move || Vec::new(),
                                move |batch: IB, batches: &mut Vec<OB>| {
                                    Self::shard_batch(
                                        &batch,
                                        num_workers,
                                        &mut builders,
                                        batches,
                                        &factories_clone3,
                                    );
                                },
                                |batches: &mut Vec<OB>, batch: OB| batches.push(batch),
                            );

                            let output = self
                                .circuit()
                                .add_exchange(sender, receiver, self)
                                .apply_owned_named("merge shards", move |batches| {
                                    merge_batches(&factories_clone2, batches)
                                });

                            self.circuit().cache_insert(
                                ShardId::new((
                                    output.origin_node_id().clone(),
                                    sharding_policy(self.circuit()),
                                )),
                                output.clone(),
                            );

                            self.circuit().cache_insert(
                                UnshardId::new(output.origin_node_id().clone()),
                                self.clone(),
                            );

                            output
                        },
                    )
                    .clone();

                Some(output)
            }
        })
    }

    // Partitions the batch into `nshards` partitions based on the hash of the key.
    fn shard_batch<OB>(
        batch: &IB,
        shards: usize,
        builders: &mut Vec<OB::Builder>,
        outputs: &mut Vec<OB>,
        factories: &OB::Factories,
    ) where
        OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R>,
    {
        builders.clear();

        for _ in 0..shards {
            // We iterate over tuples in the batch in order; hence tuples added
            // to each shard are also ordered, so we can use the more efficient
            // `Builder` API (instead of `Batcher`) to construct output batches.
            builders.push(OB::Builder::with_capacity(
                factories,
                (),
                batch.len() / shards,
            ));
        }

        let mut cursor = batch.cursor();
        let mut weight = factories.weight_factory().default_box();

        while cursor.key_valid() {
            let batch_index = cursor.key().default_hash() as usize % shards;
            while cursor.val_valid() {
                cursor.weight().clone_to(&mut *weight);
                builders[batch_index].push_refs(cursor.key(), cursor.val(), &*weight);
                cursor.step_val();
            }
            cursor.step_key();
        }

        for builder in builders.drain(..) {
            outputs.push(builder.done());
        }
    }
}

impl<C, T> Stream<C, T>
where
    C: Circuit,
    T: 'static,
{
    /// Marks the data within the current stream as sharded, meaning that all
    /// further calls to `.shard()` will have no effect.
    ///
    /// This must only be used on streams of values that are properly sharded
    /// across workers, otherwise this will cause the dataflow to yield
    /// incorrect results
    pub fn mark_sharded(&self) -> Self {
        self.circuit().cache_insert(
            ShardId::new((
                self.origin_node_id().clone(),
                sharding_policy(self.circuit()),
            )),
            self.clone(),
        );
        self.clone()
    }

    /// Returns `true` if a sharded version of the current stream exists
    pub fn has_sharded_version(&self) -> bool {
        self.circuit().cache_contains(&ShardId::<C, T>::new((
            self.origin_node_id().clone(),
            sharding_policy(self.circuit()),
        )))
    }

    /// Returns the sharded version of the stream if it exists
    /// (which may be the stream itself or the result of applying
    /// the `shard` operator to it).  Otherwise, returns `self`.
    pub fn try_sharded_version(&self) -> Self {
        self.circuit()
            .cache_get(&ShardId::new((
                self.origin_node_id().clone(),
                sharding_policy(self.circuit()),
            )))
            .unwrap_or_else(|| self.clone())
    }

    /// Returns the unsharded version of the stream if it exists, and otherwise
    /// `self`.
    pub fn try_unsharded_version(&self) -> Self {
        self.circuit()
            .cache_get(&UnshardId::new(self.origin_node_id().clone()))
            .unwrap_or_else(|| self.clone())
    }

    /// Returns `true` if this stream is sharded.
    pub fn is_sharded(&self) -> bool {
        self.circuit()
            .cache_get(&ShardId::<C, T>::new((
                self.origin_node_id().clone(),
                sharding_policy(self.circuit()),
            )))
            .map_or(false, |sharded| sharded.ptr_eq(self))
    }

    /// Marks `self` as sharded if `input` has a sharded version of itself
    pub fn mark_sharded_if<C2, U>(&self, input: &Stream<C2, U>)
    where
        C2: Circuit,
        U: 'static,
    {
        if input.has_sharded_version() {
            self.mark_sharded();
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        operator::Generator, trace::BatchReader, typed_batch::OrdIndexedZSet, utils::Tup2, Circuit,
        RootCircuit, Runtime,
    };

    #[test]
    fn test_shard() {
        do_test_shard(2);
        do_test_shard(4);
        do_test_shard(16);
    }

    fn test_data(worker_index: usize, num_workers: usize) -> OrdIndexedZSet<u64, u64> {
        let tuples: Vec<_> = (0..1000)
            .filter(|n| n % num_workers == worker_index)
            .flat_map(|n| {
                vec![
                    Tup2(Tup2(n as u64, n as u64), 1i64),
                    Tup2(Tup2(n as u64, 1000 * n as u64), 1),
                ]
            })
            .collect();
        <OrdIndexedZSet<u64, u64>>::from_tuples((), tuples)
    }

    fn do_test_shard(workers: usize) {
        let hruntime = Runtime::run(workers, || {
            let circuit = RootCircuit::build(move |circuit| {
                let input = circuit.add_source(Generator::new(|| {
                    let worker_index = Runtime::worker_index();
                    let num_workers = Runtime::runtime().unwrap().num_workers();
                    test_data(worker_index, num_workers)
                }));
                input
                    .shard()
                    .gather(0)
                    .inspect(|batch: &OrdIndexedZSet<u64, u64>| {
                        if Runtime::worker_index() == 0 {
                            assert_eq!(batch, &test_data(0, 1))
                        } else {
                            assert_eq!(batch.len(), 0);
                        }
                    });
                Ok(())
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
