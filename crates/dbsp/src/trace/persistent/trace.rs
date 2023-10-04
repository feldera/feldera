//! The implementation of the persistent trace.
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use rand::Rng;
use rkyv::ser::Serializer;
use rkyv::{to_bytes, Archive, Archived, Deserialize, Fallible, Serialize};
use rocksdb::compaction_filter::Decision;
use rocksdb::{BoundColumnFamily, MergeOperands, Options, WriteBatch};
use size_of::SizeOf;
use uuid::Uuid;

use super::{
    rocksdb_key_comparator, PersistedKey, PersistentTraceCursor, Values, ROCKS_DB_INSTANCE,
};
use crate::algebra::AddAssignByRef;
use crate::circuit::Activator;
use crate::time::{Antichain, Timestamp};
use crate::trace::cursor::Cursor;
use crate::trace::{
    unaligned_deserialize, AntichainRef, Batch, BatchReader, Builder, Consumer, DBData,
    DBTimestamp, DBWeight, HasZero, Trace, ValueConsumer,
};
use crate::NumEntries;

/// A persistent trace implementation.
///
/// - It mimics the (external) behavior of a `Spine`, but internally it uses a
///   RocksDB ColumnFamily to store its data.
///
/// - It also relies on merging and compaction of the RocksDB key-value store
///   rather than controlling these aspects itself.
#[derive(SizeOf)]
pub struct PersistentTrace<B>
where
    B: Batch,
{
    lower: Antichain<B::Time>,
    upper: Antichain<B::Time>,
    dirty: bool,
    approximate_len: usize,

    lower_key_bound: Option<B::Key>,
    lower_val_bound: Option<B::Val>,

    /// Where all the dataz is.
    #[size_of(skip)]
    cf: Arc<BoundColumnFamily<'static>>,
    cf_name: String,
    #[size_of(skip)]
    _cf_options: Options,

    _phantom: std::marker::PhantomData<B>,
}

impl<B> Default for PersistentTrace<B>
where
    B: Batch,
{
    fn default() -> Self {
        PersistentTrace::new(None)
    }
}

impl<B> Drop for PersistentTrace<B>
where
    B: Batch,
{
    /// Deletes the RocksDB column family.
    fn drop(&mut self) {
        ROCKS_DB_INSTANCE
            .drop_cf(&self.cf_name)
            .expect("Can't delete CF?");
    }
}

impl<B> Debug for PersistentTrace<B>
where
    B: Batch,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut cursor: PersistentTraceCursor<B> = self.cursor();
        writeln!(f, "PersistentTrace:")?;
        writeln!(f, "    rocksdb column {}:", self.cf_name)?;
        while cursor.key_valid() {
            writeln!(f, "{:?}:", cursor.key())?;
            while cursor.val_valid() {
                writeln!(f, "{:?}:", cursor.val())?;
                cursor.map_times(|t, w| {
                    writeln!(
                        f,
                        "{}",
                        textwrap::indent(format!("{t:?} -> {w:?}").as_str(), "        ")
                    )
                    .expect("can't write out");
                });

                cursor.step_val();
            }
            cursor.step_key();
        }
        writeln!(f)?;
        Ok(())
    }
}

impl<B> Clone for PersistentTrace<B>
where
    B: Batch,
{
    fn clone(&self) -> Self {
        unimplemented!("PersistentTrace::clone")
    }
}

impl<B> NumEntries for PersistentTrace<B>
where
    B: Batch,
{
    const CONST_NUM_ENTRIES: Option<usize> = None;

    fn num_entries_shallow(&self) -> usize {
        self.key_count()
    }

    fn num_entries_deep(&self) -> usize {
        // Same as Spine implementation:
        self.num_entries_shallow()
    }
}

impl<B> Archive for PersistentTrace<B>
where
    B: Batch,
{
    type Archived = ();
    type Resolver = ();

    unsafe fn resolve(&self, _pos: usize, _resolver: Self::Resolver, _out: *mut Self::Archived) {
        unimplemented!();
    }
}

impl<B: Batch, S: Serializer + ?Sized> Serialize<S> for PersistentTrace<B> {
    fn serialize(&self, _serializer: &mut S) -> Result<Self::Resolver, S::Error> {
        unimplemented!();
    }
}

impl<B: Batch, D: Fallible> Deserialize<PersistentTrace<B>, D> for Archived<PersistentTrace<B>> {
    fn deserialize(&self, _deserializer: &mut D) -> Result<PersistentTrace<B>, D::Error> {
        unimplemented!();
    }
}

pub struct PersistentConsumer<B>
where
    B: Batch,
{
    __type: PhantomData<B>,
}

impl<B> Consumer<B::Key, B::Val, B::R, B::Time> for PersistentConsumer<B>
where
    B: Batch,
{
    type ValueConsumer<'a> = PersistentTraceValueConsumer<'a, B>
    where
        Self: 'a;

    fn key_valid(&self) -> bool {
        todo!()
    }

    fn peek_key(&self) -> &B::Key {
        todo!()
    }

    fn next_key(&mut self) -> (B::Key, Self::ValueConsumer<'_>) {
        todo!()
    }

    fn seek_key(&mut self, _key: &B::Key)
    where
        B::Key: Ord,
    {
        todo!()
    }
}

pub struct PersistentTraceValueConsumer<'a, B> {
    __type: PhantomData<&'a B>,
}

impl<'a, B> ValueConsumer<'a, B::Val, B::R, B::Time> for PersistentTraceValueConsumer<'a, B>
where
    B: Batch,
{
    fn value_valid(&self) -> bool {
        todo!()
    }

    fn next_value(&mut self) -> (B::Val, B::R, B::Time) {
        todo!()
    }

    fn remaining_values(&self) -> usize {
        todo!()
    }
}

impl<B> BatchReader for PersistentTrace<B>
where
    B: Batch,
{
    type Key = B::Key;
    type Val = B::Val;
    type Time = B::Time;
    type R = B::R;

    type Cursor<'s> = PersistentTraceCursor<'s, B>;
    type Consumer = PersistentConsumer<B>;

    fn consumer(self) -> Self::Consumer {
        PersistentConsumer {
            __type: std::marker::PhantomData,
        }
    }

    /// The number of keys in the batch.
    ///
    /// This is an estimate as there is no way to get an exact count from
    /// RocksDB.
    fn key_count(&self) -> usize {
        ROCKS_DB_INSTANCE
            .property_int_value_cf(&self.cf, rocksdb::properties::ESTIMATE_NUM_KEYS)
            .expect("Can't get key count estimate")
            .map_or_else(|| 0, |c| c as usize)
    }

    /// The number of updates in the batch.
    ///
    /// This is an estimate, not an accurate count.
    fn len(&self) -> usize {
        self.approximate_len
    }

    fn lower(&self) -> AntichainRef<Self::Time> {
        self.lower.as_ref()
    }

    fn upper(&self) -> AntichainRef<Self::Time> {
        self.upper.as_ref()
    }

    fn cursor(&self) -> Self::Cursor<'_> {
        PersistentTraceCursor::new(&self.cf, &self.lower_key_bound)
    }

    fn truncate_keys_below(&mut self, lower_bound: &Self::Key) {
        let bound = if let Some(bound) = &self.lower_key_bound {
            max(bound, lower_bound).clone()
        } else {
            lower_bound.clone()
        };
        self.lower_key_bound = Some(bound);
    }

    fn sample_keys<RG>(&self, _rng: &mut RG, _sample_size: usize, _sample: &mut Vec<Self::Key>)
    where
        Self::Time: PartialEq<()>,
        RG: Rng,
    {
        todo!();
    }
}

/// The data-type that is persisted as the value in RocksDB.
#[derive(Debug, Archive, Serialize, Deserialize)]
pub(super) enum PersistedValue<T, R>
where
    T: DBTimestamp,
    R: DBWeight,
{
    /// Values with key-weight pairs.
    Values(Values<T, R>),
    /// A tombstone for a key which had its values deleted (during merges).
    ///
    /// It signifies that the key shouldn't exist anymore. See also
    /// [`tombstone_compaction`] which gets rid of Tombstones during compaction.
    Tombstone,
}

/// A merge-op is what [`PersistentTrace`] supplies to the RocksDB instance to
/// indicate how to update the values.
#[derive(Clone, Debug, Archive, Serialize, Deserialize)]
enum MergeOp<T, R>
where
    T: DBTimestamp,
    R: DBWeight,
{
    /// A recede-to command to reset times of values.
    RecedeTo(T),
    /// An insertion of a new value or update of an existing value.
    Insert(Values<T, R>),
}

/// The implementation of the merge operator for the RocksDB instance.
///
/// This essentially re-implements the core-logic of [`PersistentTrace`] in how
/// values, times and weights are updated.
///
/// # TODO
/// Probably lots of efficiency improvements to be had here: We're sorting
/// several times when we probably can be smarter etc. -- not clear it matters
/// without benchmarking though.
fn rocksdb_concat_merge<K, V, R, T>(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    T: DBTimestamp,
{
    //let (_key, _value): (K, V) = unaligned_deserialize(new_key);

    let mut vals: Values<T, R> = if let Some(val) = existing_val {
        let decoded_val: PersistedValue<T, R> = unaligned_deserialize(val);
        match decoded_val {
            PersistedValue::Values(vals) => vals,
            PersistedValue::Tombstone => Vec::new(),
        }
    } else {
        Vec::new()
    };

    for op in operands {
        let decoded_update: MergeOp<T, R> = unaligned_deserialize(op);
        match decoded_update {
            MergeOp::Insert(tws) => {
                for (t, w) in &tws {
                    if let Some((_, ref mut existing_w)) = vals.iter_mut().find(|(et, _)| et == t) {
                        existing_w.add_assign_by_ref(w);
                        if existing_w.is_zero() {
                            vals.retain(|(_, w)| !w.is_zero());
                        }
                    } else {
                        vals.push((t.clone(), w.clone()));
                        // TODO: May be better if push (above) inserts at the
                        // right place instead of paying the cost of sorting
                        // everything:
                        vals.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                        break;
                    }
                }
                // Delete values which ended up with zero weights
                vals.retain(|(_t, ret_w)| !ret_w.is_zero());
            }
            MergeOp::RecedeTo(frontier) => {
                log::info!("MergeOp::RecedeTo {frontier:?}");
                let mut modified_t = false;
                for (existing_t, _existing_w) in vals.iter_mut() {
                    // I think due to this being sorted by Ord we have to
                    // walk all of them (see also `map_batches_through`):
                    if !existing_t.less_equal(&frontier) {
                        // example: times [1,2,3,4], frontier 3 -> [1,2,3,3]
                        *existing_t = existing_t.meet(&frontier);
                        modified_t = true;
                    }
                }

                if modified_t {
                    // I think due to this being sorted by `Ord` we can't
                    // rely on `recede_to(x)` having only affected
                    // consecutive elements, so we create a new (t, w)
                    // vector with a hashmap that we sort again.
                    let mut new_tw = HashMap::with_capacity(vals.len());
                    for (cur_t, cur_w) in &*vals {
                        new_tw
                            .entry(cur_t.clone())
                            .and_modify(|w: &mut R| w.add_assign_by_ref(cur_w))
                            .or_insert_with(|| cur_w.clone());
                    }
                    let new_tw_vec: Vec<(T, R)> = new_tw.into_iter().collect();
                    vals = new_tw_vec;
                }
                vals.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));

                // Delete values which ended up with zero weights
                vals.retain(|(_ret_t, ret_w)| !ret_w.is_zero());
            }
        }
    }

    // TODO: We can probably avoid re-sorting in some cases (see if found_v above)?
    vals.sort_unstable_by(|v1, v2| v1.0.cmp(&v2.0));

    let vals = if !vals.is_empty() {
        PersistedValue::Values(vals)
    } else {
        PersistedValue::Tombstone
    };

    let buf = to_bytes(&vals).expect("Can't encode `vals`");
    Some(buf.into())
}

/// Throw away keys that no longer have values and should've been deleted (but
/// can't because RocksDB doesn't support deletions during merge operations).
fn tombstone_compaction<V, T, R>(_level: u32, _key: &[u8], val: &[u8]) -> Decision
where
    V: DBData,
    R: DBWeight,
    T: DBTimestamp,
{
    // TODO: Ideally we shouldn't have to pay the price of decoding the whole
    // Vec<(T, R)> as we only care about what the enum variant is.
    let decoded_val: PersistedValue<T, R> = unaligned_deserialize(val);
    match decoded_val {
        PersistedValue::Values(_vals) => Decision::Keep,
        PersistedValue::Tombstone => Decision::Remove,
    }
}

impl<B> Trace for PersistentTrace<B>
where
    B: Batch + Clone + 'static,
    B::Time: DBTimestamp,
{
    type Batch = B;

    /// Create a new PersistentTrace.
    ///
    /// It works by creating a new column-family with a random name and
    /// configuring it with the right custom functions for comparison, merge,
    /// and compaction.
    ///
    /// # Arguments
    /// - `activator`: This is not used, None should be supplied.
    fn new(_activator: Option<Activator>) -> Self {
        // Create a new column family for the Trace
        let cf_name = Uuid::new_v4().to_string();
        let mut cf_options = Options::default();
        cf_options.set_comparator(
            "Rust type compare",
            Box::new(rocksdb_key_comparator::<B::Key, B::Val>),
        );
        cf_options.set_merge_operator_associative(
            "Trace value merge function",
            rocksdb_concat_merge::<B::Key, B::Val, B::R, B::Time>,
        );
        cf_options.set_compaction_filter(
            "Remove empty vals",
            tombstone_compaction::<B::Val, B::Time, B::R>,
        );
        cf_options.create_if_missing(true);

        ROCKS_DB_INSTANCE
            .create_cf(cf_name.as_str(), &cf_options)
            .expect("Can't create column family?");
        let cf = ROCKS_DB_INSTANCE
            .cf_handle(cf_name.as_str())
            .expect("Can't find just created column family?");

        Self {
            lower: Antichain::from_elem(B::Time::minimum()),
            upper: Antichain::new(),
            approximate_len: 0,
            lower_key_bound: None,
            lower_val_bound: None,
            dirty: false,
            cf,
            cf_name,
            _cf_options: cf_options,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Recede to works by sending a `RecedeTo` command to every key in the
    /// trace.
    fn recede_to(&mut self, frontier: &B::Time) {
        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let kv: PersistedKey<B::Key, B::Val> =
                    (cursor.key().clone(), Some(cursor.val().clone()));
                let encoded_key = to_bytes(&kv).expect("Can't encode `key`");

                let update: MergeOp<B::Time, B::R> = MergeOp::RecedeTo(frontier.clone());
                let encoded_update = to_bytes(&update).expect("Can't encode `vals`");

                ROCKS_DB_INSTANCE
                    .merge_cf(&self.cf, encoded_key, encoded_update)
                    .expect("Can't merge recede update");

                cursor.step_val();
            }
            cursor.step_key();
        }
    }

    fn exert(&mut self, _effort: &mut isize) {
        // This is a no-op for the persistent trace as RocksDB will decide when
        // to apply the merge / compaction operators etc.
    }

    fn consolidate(self) -> Option<Self::Batch> {
        // TODO: Not clear what the time of the batch should be here -- in Spine
        // the batch will not be `minimum` as it's created through merges of all
        // batches.
        //
        // In discussion with Leonid: We probably want to move consolidate out
        // of the trace trait.
        let mut builder = <Self::Batch as Batch>::Builder::new_builder(Self::Time::minimum());

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            while cursor.val_valid() {
                let v = cursor.val().clone();
                let mut w = B::R::zero();
                cursor.map_times(|_t, cur_w| {
                    w.add_assign_by_ref(cur_w);
                });
                let k = cursor.key().clone();

                builder.push((Self::Batch::item_from(k, v), w));
                cursor.step_val();
            }

            cursor.step_key();
        }

        Some(builder.done())
    }

    fn insert(&mut self, batch: Self::Batch) {
        assert!(batch.lower() != batch.upper());

        // Ignore empty batches.
        // Note: we may want to use empty batches to artificially force compaction.
        if batch.is_empty() {
            return;
        }

        self.dirty = true;
        self.lower = self.lower.as_ref().meet(batch.lower());
        self.upper = self.upper.as_ref().join(batch.upper());

        self.add_batch_to_cf(batch);
    }

    fn clear_dirty_flag(&mut self) {
        self.dirty = false;
    }

    fn dirty(&self) -> bool {
        self.dirty
    }

    fn truncate_values_below(&mut self, lower_bound: &Self::Val) {
        self.lower_val_bound = Some(if let Some(bound) = &self.lower_val_bound {
            max(bound, lower_bound).clone()
        } else {
            lower_bound.clone()
        });
    }

    fn lower_value_bound(&self) -> &Option<Self::Val> {
        &self.lower_val_bound
    }
}

impl<B> PersistentTrace<B>
where
    B: Batch,
{
    fn add_batch_to_cf(&mut self, batch: B) {
        let mut sstable = WriteBatch::default();
        let mut batch_cursor = batch.cursor();
        while batch_cursor.key_valid() {
            while batch_cursor.val_valid() {
                let kv: PersistedKey<B::Key, B::Val> =
                    (batch_cursor.key().clone(), Some(batch_cursor.val().clone()));

                let mut weights = Vec::new();
                batch_cursor.map_times(|ts, r| {
                    weights.push((ts.clone(), r.clone()));
                });
                let encoded_vals = to_bytes(&MergeOp::Insert(weights)).expect("Can't encode `val`");

                let encoded_key = to_bytes(&kv).expect("Can't encode `key--value`");
                sstable.merge_cf(&self.cf, encoded_key, encoded_vals);
                batch_cursor.step_val();
                self.approximate_len += 1;
            }

            batch_cursor.step_key();
        }

        ROCKS_DB_INSTANCE
            .write(sstable)
            .expect("Could not write batch to db");
    }
}
