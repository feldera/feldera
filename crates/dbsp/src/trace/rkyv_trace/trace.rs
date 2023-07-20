//! The implementation of the persistent trace.
use crate::{
    circuit::Activator,
    time::Antichain,
    trace::{
        layers::{column_layer::ColumnLayer, ordered::OrderedLayer, OrdOffset},
        ord::OrdKeyBatch,
        AntichainRef, BatchReader, Consumer, Cursor, DBData, DBTimestamp, DBWeight, Trace,
        ValueConsumer,
    },
    NumEntries,
};
use memmap2::Mmap;
use rand::Rng;
use rkyv::{bytecheck, Archive, Archived, CheckBytes, Deserialize, Serialize};
use size_of::SizeOf;
use std::marker::PhantomData;

/// Key batches
pub type OrdKeyBatchLayer<K, T, R, O> = OrderedLayer<K, ColumnLayer<T, R>, O>;

/// Key-value batches
pub type OrdValueBatchLayer<K, V, T, R, O> =
    OrderedLayer<K, OrderedLayer<V, ColumnLayer<T, R>, O>, O>;

/// The hash key for key trace checksums, see [`blake3::derive_key()`].
///
/// We use a few different hash keys throughout our code to prevent bad
/// interactions. Note that using this to hash things **is not secure** and
/// should **never** be used on secure data.
///
/// [`blake3::derive_key()`]: https://docs.rs/blake3/latest/blake3/fn.derive_key.html
static KEY_TRACE_HASH_KEY: &str =
    "Feldera DBSP 2023-07-17 11:49:50 checksums for rkyv disk backed key traces";

/// The hash key for key trace checksums, see [`blake3::derive_key()`].
///
/// We use a few different hash keys throughout our code to prevent bad
/// interactions. Note that using this to hash things **is not secure** and
/// should **never** be used on secure data.
///
/// [`blake3::derive_key()`]: https://docs.rs/blake3/latest/blake3/fn.derive_key.html
static VALUE_TRACE_HASH_KEY: &str =
    "Feldera DBSP 2023-07-17 11:55:32 checksums for rkyv disk backed value traces";

/// This is the portion of the trace that resides on disk, this is what's
/// physically stored
// TODO: This is where columnation would be really useful
// TODO: Bloom filter
// TODO: HyperLogLog
// TODO: Tombstone bitset
// TODO: Some general versioning metadata like dbsp version so that we can detect stale data
#[derive(SizeOf, Archive, CheckBytes, Deserialize, Serialize)]
#[archive(bound(archive = "K: Archive, T: Archive, R: Archive, O: Archive"))]
struct DiskKeyBatch<K, R, T, O> {
    /// Checksum of the batch's data
    checksum: [u8; 32],
    /// Upper time bound contained within the trace
    lower: Antichain<T>,
    /// Lower time bound contained within the trace
    upper: Antichain<T>,
    /// The trace's data
    data: OrdKeyBatchLayer<K, T, R, O>,
}

// #[derive(SizeOf, Archive, CheckBytes, Deserialize, Serialize)]
// #[check_bytes(
//     bound = "B::Time: CheckBytes<__C>, B::Key: CheckBytes<__C>, B::Val: CheckBytes<__C>, B::R: CheckBytes<__C>"
// )]
// struct DiskValueBatch<B>
// where
//     B: Batch,
// {
//     /// Checksum of the batch's data
//     checksum: [u8; 32],
//     /// Upper time bound contained within the trace
//     lower: Antichain<B::Time>,
//     /// Lower time bound contained within the trace
//     upper: Antichain<B::Time>,
//     /// The trace's data
//     data: OrdValueBatchLayer<B::Key, B::Val, B::Time, B::R>,
// }

#[derive(SizeOf)]
pub struct DiskKeyTrace<K, R, T, O = usize> {
    /// The disk portion of the trace
    #[size_of(skip)]
    data: Mmap,
    __phantom: PhantomData<DiskKeyBatch<K, R, T, O>>,
}

impl<K, R, T, O> DiskKeyTrace<K, R, T, O>
where
    K: DBData + Archive,
    R: DBWeight + Archive,
    T: DBTimestamp + Archive,
    O: OrdOffset + Archive,
    Antichain<T>: Archive,
    DiskKeyBatch<K, R, T, O>: Archive,
{
    pub fn view(&self) -> &Archived<DiskKeyBatch<K, R, T, O>> {
        // Safety: The trace should already be validated
        unsafe { rkyv::util::archived_root::<DiskKeyBatch<K, R, T, O>>(&self.data) }
    }
}

// TODO: If we need cloning we probably want to use some sort of reference
// counting scheme to share disk traces
impl<K, R, T, O> Clone for DiskKeyTrace<K, R, T, O> {
    fn clone(&self) -> Self {
        unreachable!()
    }
}

impl<K, R, T, O> NumEntries for DiskKeyTrace<K, R, T, O>
where
    K: DBData + Archive,
    R: DBWeight + Archive,
    T: DBTimestamp + Archive,
    O: OrdOffset + Archive,
    Antichain<T>: Archive,
    DiskKeyBatch<K, R, T, O>: Archive,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.view().data.key_len()
    }

    fn num_entries_deep(&self) -> usize {
        self.view().data.key_len()
    }
}

impl<K, R, T, O> BatchReader for DiskKeyTrace<K, R, T, O>
where
    K: DBData + Archive,
    R: DBWeight + Archive,
    T: DBTimestamp + Archive,
    O: OrdOffset + Archive,
    Antichain<T>: Archive,
    DiskKeyBatch<K, R, T, O>: Archive,
{
    // TODO: Should we return the archived versions of things?
    type Key = K;
    type Val = ();
    type Time = T;
    type R = R;

    type Cursor<'s> = DiskCursor<'s, K, (),  R, T>
    where
        Self: 's;

    type Consumer = DiskConsumer<K, (), R, T>;

    fn cursor(&self) -> Self::Cursor<'_> {
        todo!()
    }

    fn consumer(self) -> Self::Consumer {
        todo!()
    }

    fn key_count(&self) -> usize {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn lower(&self) -> AntichainRef<'_, Self::Time> {
        todo!()
    }

    fn upper(&self) -> AntichainRef<'_, Self::Time> {
        todo!()
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        todo!()
    }

    fn sample_keys<RG>(&self, rng: &mut RG, sample_size: usize, sample: &mut Vec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        todo!()
    }
}

impl<K, R, T, O> Trace for DiskKeyTrace<K, R, T, O>
where
    K: DBData + Archive,
    R: DBWeight + Archive,
    T: DBTimestamp + Archive,
    O: OrdOffset + Archive,
    Antichain<T>: Archive,
    DiskKeyBatch<K, R, T, O>: Archive,
{
    type Batch = OrdKeyBatch<K, T, R, O>;

    fn new(activator: Option<Activator>) -> Self {
        todo!()
    }

    fn recede_to(&mut self, frontier: &Self::Time) {
        todo!()
    }

    fn exert(&mut self, effort: &mut isize) {
        todo!()
    }

    fn consolidate(self) -> Option<Self::Batch> {
        todo!()
    }

    fn insert(&mut self, batch: Self::Batch) {
        todo!()
    }

    fn clear_dirty_flag(&mut self) {
        todo!()
    }

    fn dirty(&self) -> bool {
        todo!()
    }

    fn truncate_values_below(&mut self, lower_bound: &Self::Val) {
        todo!()
    }

    fn lower_value_bound(&self) -> &Option<Self::Val> {
        todo!()
    }
}

pub struct DiskConsumer<K, V, R, T> {
    __type: PhantomData<(K, V, R, T)>,
}

impl<K, V, R, T> Consumer<K, V, R, T> for DiskConsumer<K, V, R, T> {
    type ValueConsumer<'a> = DiskValueConsumer<'a, V, R, T>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &K {
        todo!()
    }

    fn next_key(&mut self) -> (K, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &K)
    where
        K: Ord,
    {
        todo!()
    }
}

pub struct DiskValueConsumer<'a, V, R, T> {
    __type: PhantomData<&'a (V, R, T)>,
}

impl<'a, V, R, T> ValueConsumer<'a, V, R, T> for DiskValueConsumer<'a, V, R, T> {
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (V, R, T) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}

pub struct DiskCursor<'a, K, V, R, T> {
    __type: PhantomData<&'a (K, V, R, T)>,
}

impl<'a, K, V, R, T> Cursor<K, V, T, R> for DiskCursor<'a, K, V, R, T> {
    fn key_valid(&self) -> bool {
        todo!()
    }

    fn val_valid(&self) -> bool {
        todo!()
    }

    fn key(&self) -> &K {
        todo!()
    }

    fn val(&self) -> &V {
        todo!()
    }

    fn fold_times<F, U>(&mut self, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        todo!()
    }

    fn fold_times_through<F, U>(&mut self, upper: &T, init: U, fold: F) -> U
    where
        F: FnMut(U, &T, &R) -> U,
    {
        todo!()
    }

    fn weight(&mut self) -> R
    where
        T: PartialEq<()>,
    {
        todo!()
    }

    fn step_key(&mut self) {
        todo!()
    }

    fn step_key_reverse(&mut self) {
        todo!()
    }

    fn seek_key(&mut self, key: &K) {
        todo!()
    }

    fn seek_key_with<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        todo!()
    }

    fn seek_key_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&K) -> bool + Clone,
    {
        todo!()
    }

    fn seek_key_reverse(&mut self, key: &K) {
        todo!()
    }

    fn step_val(&mut self) {
        todo!()
    }

    fn step_val_reverse(&mut self) {
        todo!()
    }

    fn seek_val(&mut self, val: &V) {
        todo!()
    }

    fn seek_val_reverse(&mut self, val: &V) {
        todo!()
    }

    fn seek_val_with<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        todo!()
    }

    fn seek_val_with_reverse<P>(&mut self, predicate: P)
    where
        P: Fn(&V) -> bool + Clone,
    {
        todo!()
    }

    fn rewind_keys(&mut self) {
        todo!()
    }

    fn fast_forward_keys(&mut self) {
        todo!()
    }

    fn rewind_vals(&mut self) {
        todo!()
    }

    fn fast_forward_vals(&mut self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{trace::rkyv_trace::trace::DiskKeyTrace, RootCircuit, Runtime};

    #[test]
    fn rkyv_trace() {
        Runtime::run(1, move || {
            RootCircuit::build(move |circuit| {
                let (input, _input_handle) = circuit.add_input_set::<u32, i32>();

                let input_trace = input.trace::<DiskKeyTrace<u32, i32, ()>>();

                Ok(())
            })
            .unwrap();
        });
    }
}
