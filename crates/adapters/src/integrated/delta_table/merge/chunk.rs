//! The bounded lookup chunk.
//!
//! Merge mode never materializes the output batch; it walks the spine snapshot through one
//! forward cursor pass. This chunk is the only thing derived from that walk held in memory:
//! the encoded keys whose rows must be located in the target table.
//!
//! A key set over the byte budget is split into successive chunks, each driving its own
//! lookup pass. That bounds memory at the cost of re-scanning candidate row groups, so the
//! budget defaults high enough that a steady-state flush fits in one chunk.

use anyhow::{Result as AnyResult, anyhow};
use arrow::row::{Row, Rows};
use feldera_modular_bloom::{ModularBloomFilter, ModularBloomFilterBuilder, ModuleLayout};
use xxhash_rust::xxh3::xxh3_64;

/// Encoded keys awaiting a lookup pass, sorted on demand.
///
/// Sorted on the encoded bytes rather than inheriting the cursor's order, so a key type only
/// has to preserve equality under encoding, not order.
pub struct LookupChunk {
    /// Concatenated encoded keys.
    buffer: Vec<u8>,
    /// Start offset of each key in `buffer`; `offsets[i]..offsets[i + 1]` is key `i`.
    offsets: Vec<u32>,
    /// Permutation of key indices in ascending byte order, built by [`Self::sort`].
    order: Vec<u32>,
    sorted: bool,
    budget_bytes: usize,
    /// Set when a key in this chunk is null in some column. See [`Self::note_null_key`].
    has_null_key: bool,
    /// Rejects absent keys ahead of the binary search; `None` until [`Self::sort`] builds it,
    /// when every key falls through to the search.
    ///
    /// A lookup asks the chunk about every key of every file it opens, so this runs once per
    /// row in the *table* rather than once per row in the batch, and almost every answer is
    /// no. Proving that by binary search walks log2(keys) scattered offsets, which at a
    /// hundred thousand keys misses cache on most of them.
    filter: Option<ModularBloomFilter>,
}

/// Bits the filter spends per key. Charges the filter to the chunk's byte budget before
/// [`LookupChunk::sort`] builds it; [`test::the_filter_fits_its_budget`] keeps it honest.
const FILTER_BITS_PER_KEY: usize = 16;

/// False positives the filter is sized for. A false positive costs only the binary search
/// that would have run anyway, so a rate near a tenth of a percent is already deep into
/// diminishing returns.
const FILTER_TARGET_FALSE_POSITIVES: f64 = 0.0013;

/// Modules to split the filter across. Two, so an absent key is usually rejected on the
/// first module's hashes and never pays for the second's.
const FILTER_MODULES: u32 = 2;

/// Sizes the filter for `keys` keys.
fn filter_layout(keys: usize) -> ModuleLayout {
    ModuleLayout::for_keys(
        keys.max(1) as u64,
        FILTER_TARGET_FALSE_POSITIVES,
        FILTER_MODULES,
    )
    .expect("a positive key count and a rate in (0, 1) always describe a layout")
}

impl LookupChunk {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            offsets: vec![0],
            order: Vec::new(),
            sorted: false,
            budget_bytes,
            has_null_key: false,
            filter: None,
        }
    }

    /// Append one encoded key.
    pub fn push(&mut self, key: Row<'_>) -> AnyResult<()> {
        let key = key.as_ref();
        let end = checked_end(self.buffer.len(), key.len())?;
        self.buffer.extend_from_slice(key);
        self.offsets.push(end);
        self.sorted = false;
        Ok(())
    }

    /// Append every key in `rows`.
    pub fn extend(&mut self, rows: &Rows) -> AnyResult<()> {
        for i in 0..rows.num_rows() {
            self.push(rows.row(i))?;
        }
        Ok(())
    }

    /// Record that a key in this chunk is null in some column.
    ///
    /// Min/max statistics leave nulls out, so once this is set nothing prunes.
    pub fn note_null_key(&mut self) {
        self.has_null_key = true;
    }

    pub fn has_null_key(&self) -> bool {
        self.has_null_key
    }

    /// Key count. `offsets` always keeps its leading 0, so this cannot underflow.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes held, counting the offset, permutation and filter vectors. The last two are
    /// counted before `sort` allocates them, so the budget covers what querying will cost.
    pub fn bytes(&self) -> usize {
        self.buffer.len()
            + self.offsets.len() * 4
            + self.len() * 4
            + self.len() * FILTER_BITS_PER_KEY / 8
    }

    /// Whether the chunk has reached its budget and should be handed to a lookup pass.
    pub fn is_full(&self) -> bool {
        self.bytes() >= self.budget_bytes
    }

    /// Drop every key, keeping the allocations for the next chunk.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.offsets.clear();
        self.offsets.push(0);
        self.order.clear();
        self.sorted = false;
        self.has_null_key = false;
        self.filter = None;
    }

    fn key_at(&self, index: usize) -> &[u8] {
        let start = self.offsets[index] as usize;
        let end = self.offsets[index + 1] as usize;
        &self.buffer[start..end]
    }

    /// Sort the chunk on encoded bytes. Idempotent.
    pub fn sort(&mut self) {
        if self.sorted {
            return;
        }
        // Move the permutation out so the comparator can borrow the buffer immutably.
        let mut order = std::mem::take(&mut self.order);
        order.clear();
        order.extend(0..self.len() as u32);

        let (buffer, offsets) = (&self.buffer, &self.offsets);
        let key_at = |i: u32| -> &[u8] {
            let start = offsets[i as usize] as usize;
            let end = offsets[i as usize + 1] as usize;
            &buffer[start..end]
        };
        // Sort an index permutation rather than the keys, so variable-length keys never
        // move in the buffer.
        order.sort_unstable_by(|a, b| key_at(*a).cmp(key_at(*b)));

        self.order = order;

        self.filter = Some(self.build_filter());
        self.sorted = true;
    }

    /// Build the pre-filter over every key in the chunk.
    fn build_filter(&self) -> ModularBloomFilter {
        let mut builder = ModularBloomFilterBuilder::new(filter_layout(self.len()));
        for i in 0..self.len() {
            builder.insert_hash(xxh3_64(self.key_at(i)));
        }
        builder.finish()
    }

    /// Index into sorted order of the first key not less than `key`.
    ///
    /// Requires [`Self::sort`].
    pub fn lower_bound(&self, key: &[u8]) -> usize {
        debug_assert!(self.sorted, "sort() before querying the chunk");
        self.order
            .partition_point(|i| self.key_at(*i as usize) < key)
    }

    /// Whether `key` is present. Requires [`Self::sort`].
    #[cfg(test)]
    pub fn contains(&self, key: &[u8]) -> bool {
        self.position(key).is_some()
    }

    /// Index of `key` in sorted order, or `None` when absent. Requires [`Self::sort`].
    ///
    /// The lookup counts distinct indices found, which is what tells "this key is not in the
    /// table" from "this key is in two files".
    pub fn position(&self, key: &[u8]) -> Option<usize> {
        // A filter miss is decisive; a hit still has to be confirmed by the search below.
        if let Some(filter) = &self.filter
            && !filter.contains_hash(xxh3_64(key))
        {
            return None;
        }
        let i = self.lower_bound(key);
        (i < self.order.len() && self.key_at(self.order[i] as usize) == key).then_some(i)
    }

    /// Whether any key falls within `[min, max]` inclusive. Requires [`Self::sort`].
    ///
    /// The exact test pruning applies to a unit's key statistics: one lower-bound search
    /// rather than an interval approximation of the chunk.
    pub fn intersects(&self, min: &[u8], max: &[u8]) -> bool {
        let i = self.lower_bound(min);
        i < self.order.len() && self.key_at(self.order[i] as usize) <= max
    }
}

/// End offset of a key appended to a buffer of `len` bytes.
///
/// A wrapped offset would lose track of where a key ends and supersede the wrong rows, and
/// `is_full` cannot prevent it: the flush encodes keys in batches and only consults the
/// budget between them, so one batch of keys can overshoot it by however many bytes it holds.
fn checked_end(len: usize, added: usize) -> AnyResult<u32> {
    u32::try_from(len.saturating_add(added)).map_err(|_| {
        anyhow!(
            "the keys buffered for one lookup pass exceed the 4 GiB that the connector's \
             32-bit offsets address. Lower 'lookup_chunk_bytes', or index the view on a \
             smaller key: the connector buffers a batch of keys past the budget before it \
             checks the budget again."
        )
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use arrow::datatypes::DataType;
    use arrow::row::{RowConverter, SortField};
    use std::sync::Arc;

    fn encode(values: &[i64]) -> Rows {
        let converter = RowConverter::new(vec![SortField::new(DataType::Int64)]).unwrap();
        let column: ArrayRef = Arc::new(Int64Array::from(values.to_vec()));
        converter.convert_columns(&[column]).unwrap()
    }

    fn chunk_of(values: &[i64]) -> (LookupChunk, Rows) {
        let rows = encode(values);
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.extend(&rows).unwrap();
        chunk.sort();
        (chunk, rows)
    }

    /// Every key in the chunk must survive the filter. A false negative is a row the lookup
    /// never locates, which leaves the superseded copy live and duplicates the key, so this
    /// is the one filter property that matters.
    #[test]
    fn the_filter_never_rejects_a_key_the_chunk_holds() {
        for keys in [1usize, 2, 1_000, 100_000] {
            // Spread out rather than contiguous, so the fingerprints are not a dense run.
            let values: Vec<i64> = (0..keys as i64)
                .map(|i| i.wrapping_mul(0x9e37_79b9))
                .collect();
            let (chunk, rows) = chunk_of(&values);
            for i in 0..keys {
                assert!(
                    chunk.position(rows.row(i).as_ref()).is_some(),
                    "filter rejected key {i} of {keys}"
                );
            }
        }
    }

    /// The filter is worth its memory only if it rejects nearly everything, and a rate far
    /// above the design point means the sizing or the hash is wrong.
    #[test]
    fn the_filter_rejects_almost_every_absent_key() {
        const KEYS: i64 = 100_000;
        let (chunk, _) = chunk_of(&(0..KEYS).collect::<Vec<_>>());
        let absent = encode(&(KEYS..2 * KEYS).collect::<Vec<_>>());
        let passed = (0..KEYS as usize)
            .filter(|i| {
                let filter = chunk.filter.as_ref().unwrap();
                filter.contains_hash(xxh3_64(absent.row(*i).as_ref()))
            })
            .count();
        let rate = passed as f64 / KEYS as f64;
        assert!(
            rate < 0.02,
            "false positive rate {rate} is above the 2% ceiling"
        );
    }

    /// The chunk charges `FILTER_BITS_PER_KEY` to its byte budget before the filter exists,
    /// so a change to the target rate that outgrew that would overrun `lookup_chunk_bytes`.
    #[test]
    fn the_filter_fits_its_budget() {
        // Each module rounds up to a whole word, so a small filter costs a fixed floor
        // beyond the per-key charge.
        const FLOOR: usize = FILTER_MODULES as usize * size_of::<u64>();
        for keys in [1usize, 1_000, 100_000, 10_000_000] {
            let actual = filter_layout(keys).total_bytes();
            let budgeted = keys * FILTER_BITS_PER_KEY / 8 + FLOOR;
            assert!(
                actual <= budgeted,
                "{keys} keys cost {actual} bytes, over the {budgeted} budgeted"
            );
        }
    }

    /// Membership must hold on encoded bytes rather than on insertion order, and a second
    /// `sort` must not disturb it.
    #[test]
    fn contains_finds_members_and_rejects_others() {
        let (mut chunk, _) = chunk_of(&[5, 1, 9, 3]);
        chunk.sort();
        let probe = encode(&[1, 3, 5, 9, 0, 4, 10]);
        for i in 0..4 {
            assert!(chunk.contains(probe.row(i).as_ref()), "missing member {i}");
        }
        for i in 4..7 {
            assert!(!chunk.contains(probe.row(i).as_ref()), "false hit {i}");
        }
    }

    #[test]
    fn intersects_matches_exact_set_membership() {
        // Clustered at the ends: an interval test over [min, max] would keep the middle
        // range, the exact test prunes it.
        let (chunk, _) = chunk_of(&[1, 2, 100, 101]);
        let b = encode(&[1, 2, 100, 101, 40, 60]);
        assert!(chunk.intersects(b.row(0).as_ref(), b.row(1).as_ref()));
        assert!(chunk.intersects(b.row(2).as_ref(), b.row(3).as_ref()));
        assert!(
            !chunk.intersects(b.row(4).as_ref(), b.row(5).as_ref()),
            "an empty middle range must prune"
        );
    }

    #[test]
    fn empty_chunk_never_matches() {
        let mut chunk = LookupChunk::new(usize::MAX);
        chunk.sort();
        let probe = encode(&[1, 2]);
        assert!(chunk.is_empty());
        assert!(!chunk.contains(probe.row(0).as_ref()));
        assert!(!chunk.intersects(probe.row(0).as_ref(), probe.row(1).as_ref()));
    }

    #[test]
    fn budget_triggers_and_clear_resets() {
        let rows = encode(&[1, 2, 3, 4, 5, 6, 7, 8]);
        let mut chunk = LookupChunk::new(64);
        let mut pushed = 0;
        while !chunk.is_full() {
            chunk.push(rows.row(pushed)).unwrap();
            pushed += 1;
            assert!(pushed <= rows.num_rows(), "budget never reached");
        }
        assert!(!chunk.is_empty());

        chunk.clear();
        assert!(chunk.is_empty());
        assert!(!chunk.is_full());
        assert_eq!(chunk.bytes(), 4, "only the leading offset remains");
    }

    /// The guard has to trip exactly at what a 32-bit offset addresses.
    ///
    /// Tested on the arithmetic rather than on a real buffer: reaching the limit for real
    /// needs 4 GiB of keys.
    #[test]
    fn checked_end_stops_at_the_addressable_limit() {
        const LIMIT: usize = u32::MAX as usize;
        assert_eq!(checked_end(0, 0).unwrap(), 0);
        assert_eq!(checked_end(LIMIT - 1, 1).unwrap(), u32::MAX);
        assert!(checked_end(LIMIT, 1).is_err(), "one byte past must fail");
        assert!(
            checked_end(usize::MAX, usize::MAX).is_err(),
            "must not wrap"
        );
        assert!(
            checked_end(LIMIT, 1)
                .unwrap_err()
                .to_string()
                .contains("lookup_chunk_bytes"),
            "the error must name the setting to lower"
        );
    }
}
