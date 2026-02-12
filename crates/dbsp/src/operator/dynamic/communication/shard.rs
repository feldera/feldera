//! Operators to shard batches across multiple worker threads based on keys
//! and to gather sharded batches in one worker.

// TODOs:
// - different sharding modes.

use rkyv::{archived_root, ser::Serializer as _};

use crate::{
    Circuit, Runtime, Stream,
    circuit::circuit_builder::StreamId,
    circuit_cache_key,
    dynamic::{Data, DataTrait, DynPairs, Factory},
    operator::communication::new_exchange_operators,
    trace::{
        Batch, BatchReader, Builder, Serializer, deserialize_indexed_wset, merge_batches,
        serialize_indexed_wset,
    },
};

use std::{ops::Range, panic::Location};

circuit_cache_key!(ShardId<C, D>((StreamId, Range<usize>) => Stream<C, D>));
circuit_cache_key!(UnshardId<C, D>(StreamId => Stream<C, D>));

fn all_workers() -> Range<usize> {
    0..Runtime::num_workers()
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

    /// See [`Stream::shard_workers`].
    #[track_caller]
    pub fn dyn_shard_workers(
        &self,
        workers: Range<usize>,
        factories: &IB::Factories,
    ) -> Stream<C, IB>
    where
        IB: Batch + Send,
    {
        // `shard_generic_workers` returns `None` if there is only one worker
        // thread and hence sharding is a no-op.  In this case, we simply return
        // the input stream.  This allows us to use `shard_workers`
        // unconditionally without incurring any overhead in the single-threaded
        // case.
        self.dyn_shard_generic_workers(workers, factories)
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
        self.dyn_shard_generic_workers(all_workers(), factories)
    }

    /// Like [`Self::dyn_shard`], but can assemble the results into any output batch
    /// type `OB`.
    ///
    /// Returns `None` when the circuit is not running inside a multithreaded
    /// runtime or is running in a runtime with a single worker thread.
    #[track_caller]
    pub fn dyn_shard_generic_workers<OB>(
        &self,
        workers: Range<usize>,
        factories: &OB::Factories,
    ) -> Option<Stream<C, OB>>
    where
        OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R> + Send,
    {
        if Runtime::num_workers() == 1 {
            return None;
        }
        let location = Location::caller();
        let output = self
            .circuit()
            .cache_get_or_insert_with(
                ShardId::new((self.stream_id(), workers.clone())),
                move || {
                    // As a minor optimization, we reuse this array across all invocations
                    // of the sharding operator.
                    let mut builders = Vec::with_capacity(Runtime::num_workers());
                    let factories_clone2 = factories.clone();
                    let factories_clone3 = factories.clone();
                    let factories_clone4 = factories.clone();
                    let workers_clone = workers.clone();
                    let workers_clone2 = workers.clone();

                    let output = self.circuit().region("shard", || {
                        let (sender, receiver) = new_exchange_operators(
                            Some(location),
                            || Vec::new(),
                            move |batch: IB, batches: &mut Vec<OB>| {
                                shard_batch(
                                    batch,
                                    &workers_clone,
                                    &mut builders,
                                    batches,
                                    &factories_clone3,
                                );
                            },
                            |batch| serialize_indexed_wset(&batch),
                            move |data| deserialize_indexed_wset(&factories_clone4, &data),
                            |batches: &mut Vec<OB>, batch: OB| batches.push(batch),
                        )
                        .unwrap();

                        self.circuit()
                            .add_exchange(sender, receiver, self)
                            .apply_owned_named("merge shards", move |batches| {
                                merge_batches(&factories_clone2, batches, &None, &None)
                            })
                    });

                    self.circuit().cache_insert(
                        ShardId::new((output.stream_id(), workers_clone2)),
                        output.clone(),
                    );

                    self.circuit()
                        .cache_insert(UnshardId::new(output.stream_id()), self.clone());

                    output.set_persistent_id(
                        self.get_persistent_id()
                            .map(|name| format!("{name}.shard"))
                            .as_deref(),
                    )
                },
            )
            .clone();

        Some(output)
    }
}

impl<C, K, V> Stream<C, Vec<Box<DynPairs<K, V>>>>
where
    C: Circuit,
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    #[track_caller]
    pub fn dyn_shard_pairs(
        &self,
        pairs_factory: &'static dyn Factory<DynPairs<K, V>>,
    ) -> Stream<C, Vec<Box<DynPairs<K, V>>>> {
        if self.is_sharded() {
            return self.clone();
        }

        let location = Location::caller();

        let (sender, receiver) = new_exchange_operators(
            Some(location),
            Vec::new,
            move |input_pairs: Vec<Box<DynPairs<K, V>>>,
                  output_pairs: &mut Vec<Box<DynPairs<K, V>>>| {
                shard_pairs(input_pairs, &all_workers(), output_pairs, pairs_factory);
            },
            |batch| {
                let mut s = Serializer::default();
                let offset = batch.serialize(&mut s).unwrap();
                s.serialize_value(&offset).unwrap();
                s.into_serializer().into_inner().into_vec()
            },
            move |data| {
                let offset = unsafe { archived_root::<usize>(&data) };
                let mut output = pairs_factory.default_box();

                unsafe { output.deserialize_from_bytes(&data, *offset as usize) };
                output
            },
            |output_pairs: &mut Vec<Box<DynPairs<K, V>>>, batch: Box<DynPairs<K, V>>| {
                output_pairs.push(batch);
            },
        )
        .unwrap();

        let output = self.circuit().add_exchange(sender, receiver, self);

        output.set_persistent_id(
            self.get_persistent_id()
                .map(|name| format!("{name}.shard"))
                .as_deref(),
        );
        output
    }
}

// Partitions the batch into shards covering `workers` (out of
// `all_workers()`), based on the hash of the key.
pub fn shard_batch<IB, OB>(
    mut batch: IB,
    workers: &Range<usize>,
    builders: &mut Vec<OB::Builder>,
    outputs: &mut Vec<OB>,
    factories: &OB::Factories,
) where
    IB: BatchReader<Time = ()>,
    OB: Batch<Key = IB::Key, Val = IB::Val, Time = (), R = IB::R>,
{
    builders.clear();

    // XXX If `shards == 1` and `OB` and `IB` are the same, then we could
    // implement this more efficiently, without copying.
    let shards = workers.len();
    for _ in 0..shards {
        // We iterate over tuples in the batch in order; hence tuples added
        // to each shard are also ordered, so we can use the more efficient
        // `Builder` API (instead of `Batcher`) to construct output batches.
        builders.push(OB::Builder::with_capacity(
            factories,
            batch.key_count() / shards,
            batch.len() / shards,
        ));
    }

    let mut cursor = batch.consuming_cursor(None, None);
    if cursor.has_mut() {
        while cursor.key_valid() {
            let b = &mut builders[cursor.key().default_hash() as usize % shards];
            while cursor.val_valid() {
                b.push_diff_mut(cursor.weight_mut());
                b.push_val_mut(cursor.val_mut());
                cursor.step_val();
            }
            b.push_key_mut(cursor.key_mut());
            cursor.step_key();
        }
    } else {
        while cursor.key_valid() {
            let b = &mut builders[cursor.key().default_hash() as usize % shards];
            while cursor.val_valid() {
                b.push_diff(cursor.weight());
                b.push_val(cursor.val());
                cursor.step_val();
            }
            b.push_key(cursor.key());
            cursor.step_key();
        }
    }
    for _ in 0..workers.start {
        outputs.push(OB::dyn_empty(factories));
    }
    for builder in builders.drain(..) {
        outputs.push(builder.done());
    }
    for _ in workers.end..Runtime::num_workers() {
        outputs.push(OB::dyn_empty(factories));
    }
}

// Partitions the batch into shards covering `workers` (out of
// `all_workers()`), based on the hash of the key.
pub fn shard_pairs<K, V>(
    input_pairs: Vec<Box<DynPairs<K, V>>>,
    workers: &Range<usize>,
    output_pairs: &mut Vec<Box<DynPairs<K, V>>>,
    pairs_factory: &'static dyn Factory<DynPairs<K, V>>,
) where
    K: DataTrait + ?Sized,
    V: DataTrait + ?Sized,
{
    output_pairs.clear();
    output_pairs.resize(workers.len(), pairs_factory.default_box());

    for mut pairs in input_pairs {
        for pair in pairs.dyn_iter_mut() {
            let k = pair.fst();
            let shard_index = k.default_hash() as usize % workers.len();
            output_pairs[shard_index].push_val(pair);
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
            ShardId::new((self.stream_id(), all_workers())),
            self.clone(),
        );
        self.clone()
    }

    /// Returns `true` if a sharded version of the current stream exists
    pub fn has_sharded_version(&self) -> bool {
        self.circuit()
            .cache_contains(&ShardId::<C, T>::new((self.stream_id(), all_workers())))
    }

    /// Returns the sharded version of the stream if it exists
    /// (which may be the stream itself or the result of applying
    /// the `shard` operator to it).  Otherwise, returns `self`.
    pub fn try_sharded_version(&self) -> Self {
        self.circuit()
            .cache_get(&ShardId::new((self.stream_id(), all_workers())))
            .unwrap_or_else(|| self.clone())
    }

    /// Returns the unsharded version of the stream if it exists, and otherwise
    /// `self`.
    pub fn try_unsharded_version(&self) -> Self {
        self.circuit()
            .cache_get(&UnshardId::new(self.stream_id()))
            .unwrap_or_else(|| self.clone())
    }

    /// Returns `true` if this stream is sharded.
    pub fn is_sharded(&self) -> bool {
        if Runtime::num_workers() == 1 {
            return true;
        }

        self.circuit()
            .cache_get(&ShardId::<C, T>::new((self.stream_id(), all_workers())))
            .is_some_and(|sharded| sharded.ptr_eq(self))
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
        Circuit, RootCircuit, Runtime, operator::Generator, trace::BatchReader,
        typed_batch::OrdIndexedZSet, utils::Tup2,
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
        let hruntime = Runtime::run(workers, |_parker| {
            let circuit = RootCircuit::build(move |circuit| {
                let input = circuit.add_source(Generator::new(|| {
                    let worker_index = Runtime::worker_index();
                    let num_workers = Runtime::num_workers();
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
                circuit.transaction().unwrap();
            }
        })
        .expect("failed to run runtime");

        hruntime.join().unwrap();
    }
}
