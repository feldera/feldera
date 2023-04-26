//! The implementation of the persistent trace.
use std::cmp::max;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

use bincode::{decode_from_slice, Decode, Encode};
use rocksdb::compaction_filter::Decision;
use rocksdb::{BoundColumnFamily, MergeOperands, Options, WriteBatch};
use size_of::SizeOf;
use uuid::Uuid;

use super::{rocksdb_key_comparator, PersistentTraceCursor, ReusableEncodeBuffer, Values};
use super::{BINCODE_CONFIG, ROCKS_DB_INSTANCE};
use crate::algebra::AddAssignByRef;
use crate::circuit::Activator;
use crate::time::{Antichain, Timestamp};
use crate::trace::cursor::Cursor;
use crate::trace::{
    AntichainRef, Batch, BatchReader, Builder, Consumer, DBData, DBTimestamp, DBWeight, HasZero,
    Trace, ValueConsumer,
};
use crate::NumEntries;

/// A persistent trace implementation.
///
/// - It mimics the (external) behavior of a `Spine`, but internally it uses a
///   RocksDB ColumnFamily to store it's data.
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
}

/// The data-type that is persisted as the value in RocksDB.
#[derive(Debug)]
pub(super) enum PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    /// Values with key-weight pairs.
    Values(Values<V, T, R>),
    /// A tombstone for a key which had its values deleted (during merges).
    ///
    /// It signifies that the key shouldn't exist anymore. See also
    /// [`tombstone_compaction`] which gets rid of Tombstones during compaction.
    Tombstone,
}

/// Decode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Decode for PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let typ: u8 = bincode::Decode::decode(decoder)?;

        match typ {
            0 => Ok(Self::Tombstone),
            1 => Ok(Self::Values(bincode::Decode::decode(decoder)?)),
            _ => panic!("Unknown PersistedValue type"),
        }
    }
}

/// Encode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Encode for PersistedValue<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        match self {
            Self::Tombstone => {
                bincode::Encode::encode(&0u8, encoder)?;
            }
            Self::Values(v) => {
                bincode::Encode::encode(&1u8, encoder)?;
                bincode::Encode::encode(v, encoder)?;
            }
        }
        Ok(())
    }
}

/// A merge-op is what [`PersistentTrace`] supplies to the RocksDB instance to
/// indicate how to update the values.
#[derive(Clone, Debug)]
enum MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    /// A recede-to command to reset times of values.
    RecedeTo(T),
    /// An insertion of a new value or update of an existing value.
    Insert(Values<V, T, R>),
}

/// Decode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Decode for MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let typ: u8 = bincode::Decode::decode(decoder)?;

        match typ {
            0 => Ok(Self::RecedeTo(bincode::Decode::decode(decoder)?)),
            1 => Ok(Self::Insert(bincode::Decode::decode(decoder)?)),
            _ => panic!("Unknown MergeOp type"),
        }
    }
}

/// Encode for [`Self`] is currently implemented manually thanks to a bug in
/// bincode (enum+generics seems to break things).
///
/// See also: https://github.com/bincode-org/bincode/issues/537
impl<V, T, R> Encode for MergeOp<V, T, R>
where
    V: DBData,
    T: DBTimestamp,
    R: DBWeight,
{
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        match self {
            Self::RecedeTo(t) => {
                bincode::Encode::encode(&0u8, encoder)?;
                bincode::Encode::encode(t, encoder)?;
            }
            Self::Insert(v) => {
                bincode::Encode::encode(&1u8, encoder)?;
                bincode::Encode::encode(v, encoder)?;
            }
        }
        Ok(())
    }
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
    new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &MergeOperands,
) -> Option<Vec<u8>>
where
    K: DBData,
    V: DBData,
    R: DBWeight,
    T: DBTimestamp,
{
    let (_key, _) =
        decode_from_slice::<K, _>(new_key, BINCODE_CONFIG).expect("Can't decode_from_slice");

    let mut vals: Values<V, T, R> = if let Some(val) = existing_val {
        let (decoded_val, _): (PersistedValue<V, T, R>, usize) =
            decode_from_slice(val, BINCODE_CONFIG).expect("Can't decode current value");
        match decoded_val {
            PersistedValue::Values(vals) => vals,
            PersistedValue::Tombstone => Vec::new(),
        }
    } else {
        Vec::new()
    };

    for op in operands {
        let (decoded_update, _): (MergeOp<V, T, R>, usize) =
            decode_from_slice(op, BINCODE_CONFIG).expect("Can't decode current value");

        match decoded_update {
            MergeOp::Insert(new_vals) => {
                for (v, tws) in new_vals {
                    let mut found_v = false;
                    let mut found_t = false;
                    let mut delete_zero_w = false;
                    for (existing_v, ref mut existing_tw) in vals.iter_mut() {
                        if existing_v == &v {
                            for (t, w) in &tws {
                                for (existing_t, ref mut existing_w) in existing_tw.iter_mut() {
                                    if existing_t == t {
                                        existing_w.add_assign_by_ref(w);
                                        found_t = true;
                                        if existing_w.is_zero() {
                                            delete_zero_w = true;
                                        }
                                    }
                                }
                                if !found_t {
                                    existing_tw.push((t.clone(), w.clone()));
                                    // TODO: May be better if push (above) inserts at the
                                    // right place instead of paying the cost of sorting
                                    // everything:
                                    existing_tw.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                                    break;
                                } else if delete_zero_w {
                                    existing_tw.retain(|(_, w)| !w.is_zero());
                                }
                            }
                            found_v = true;
                            break;
                        }
                    }

                    if !found_v {
                        //tws.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                        vals.push((v, tws));
                    } else {
                        // Delete values which ended up with zero weights
                        vals.retain(|(_ret_v, ret_tws)| {
                            ret_tws
                                .iter()
                                .filter(|(_ret_t, ret_w)| !ret_w.is_zero())
                                .count()
                                != 0
                        });
                    }
                }
            }
            MergeOp::RecedeTo(frontier) => {
                for (_existing_v, ref mut existing_tw) in vals.iter_mut() {
                    let mut modified_t = false;
                    for (ref mut existing_t, _existing_w) in existing_tw.iter_mut() {
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
                        let mut new_tw = HashMap::with_capacity(existing_tw.len());
                        for (cur_t, cur_w) in &*existing_tw {
                            new_tw
                                .entry(cur_t.clone())
                                .and_modify(|w: &mut R| w.add_assign_by_ref(cur_w))
                                .or_insert_with(|| cur_w.clone());
                        }
                        let new_tw_vec: Vec<(T, R)> = new_tw.into_iter().collect();
                        *existing_tw = new_tw_vec;
                    }
                    existing_tw.sort_unstable_by(|(t1, _), (t2, _)| t1.cmp(t2));
                }

                // Delete values which ended up with zero weights
                vals.retain(|(_ret_v, ret_tws)| {
                    ret_tws
                        .iter()
                        .filter(|(_ret_t, ret_w)| !ret_w.is_zero())
                        .count()
                        != 0
                });
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

    let mut buf = ReusableEncodeBuffer::with_capacity(existing_val.map(|v| v.len()).unwrap_or(0));
    buf.encode(&vals).expect("Can't encode `vals`");
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
    // Vec<(V, Vec<(T, R)>)> as we only care about what the enum variant is.
    let (decoded_val, _): (PersistedValue<V, T, R>, usize) =
        decode_from_slice(val, BINCODE_CONFIG).expect("Can't decode current value");
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
        cf_options.set_comparator("Rust type compare", rocksdb_key_comparator::<B::Key>);
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
        let mut tmp_key = ReusableEncodeBuffer::default();
        let mut tmp_val = ReusableEncodeBuffer::default();

        let mut cursor = self.cursor();
        while cursor.key_valid() {
            let key = cursor.key();
            let encoded_key = tmp_key.encode(&key).expect("Can't encode `key`");

            let update: MergeOp<B::Val, B::Time, B::R> = MergeOp::RecedeTo(frontier.clone());
            let encoded_update = tmp_val.encode(&update).expect("Can't encode `vals`");

            ROCKS_DB_INSTANCE
                .merge_cf(&self.cf, encoded_key, encoded_update)
                .expect("Can't merge recede update");
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
        use crate::trace::cursor::CursorDebug;

        let mut tmp_key = ReusableEncodeBuffer::default();
        let mut tmp_val = ReusableEncodeBuffer::default();

        let mut sstable = WriteBatch::default();
        let mut batch_cursor = batch.cursor();
        while batch_cursor.key_valid() {
            let key = batch_cursor.key();
            let encoded_key = tmp_key.encode(&key).expect("Can't encode `key`");
            let vals: Values<B::Val, B::Time, B::R> = batch_cursor.val_to_vec();
            self.approximate_len += vals.len();
            let encoded_vals = tmp_val
                .encode(&MergeOp::Insert(vals))
                .expect("Can't encode `vals`");
            sstable.merge_cf(&self.cf, encoded_key, encoded_vals);

            batch_cursor.step_key();
        }

        ROCKS_DB_INSTANCE
            .write(sstable)
            .expect("Could not write batch to db");
    }
}
