//! The bounded lookup chunk.
//!
//! Merge mode never materializes the output batch: it arrives as a spine snapshot that
//! the storage layer already keeps on disk when it is large, and the connector reads it
//! through one forward cursor pass. The only thing derived from it that is held in memory
//! is this chunk, which accumulates the encoded keys whose rows must be located in the
//! target table.
//!
//! A flush whose key set exceeds the byte budget is split into successive chunks, each
//! driving its own lookup pass. That bounds memory at the cost of re-scanning candidate
//! row groups, which is why the budget defaults high enough that a steady-state flush
//! fits in one chunk.

use arrow::row::{Row, Rows};

/// Encoded keys awaiting a lookup pass, sorted on demand.
///
/// Sorting is done on the encoded bytes rather than inherited from the cursor's order.
/// That costs `C log C` on a buffer that is already in cache and buys independence from
/// whether Feldera's key ordering and the row encoding's ordering agree, which keeps the
/// supported-key-type rule at equality preservation rather than order preservation.
pub struct LookupChunk {
    /// Concatenated encoded keys.
    buffer: Vec<u8>,
    /// Start offset of each key in `buffer`; `offsets[i]..offsets[i + 1]` is key `i`.
    offsets: Vec<u32>,
    /// Permutation of key indices in ascending byte order, built by [`Self::sort`].
    order: Vec<u32>,
    sorted: bool,
    budget_bytes: usize,
}

impl LookupChunk {
    pub fn new(budget_bytes: usize) -> Self {
        Self {
            buffer: Vec::new(),
            offsets: vec![0],
            order: Vec::new(),
            sorted: false,
            budget_bytes,
        }
    }

    /// Append one encoded key.
    pub fn push(&mut self, key: Row<'_>) {
        self.buffer.extend_from_slice(key.as_ref());
        self.offsets.push(self.buffer.len() as u32);
        self.sorted = false;
    }

    /// Append every key in `rows`.
    pub fn extend(&mut self, rows: &Rows) {
        for i in 0..rows.num_rows() {
            self.push(rows.row(i));
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes held, counting the offset and permutation vectors.
    ///
    /// The permutation is counted even before [`Self::sort`] allocates it, so that
    /// crossing the budget cannot be followed by a surprise allocation.
    pub fn bytes(&self) -> usize {
        self.buffer.len() + self.offsets.len() * 4 + self.len() * 4
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
        self.sorted = true;
    }

    /// The smallest key, or `None` when empty. Requires [`Self::sort`].
    pub fn min(&self) -> Option<&[u8]> {
        debug_assert!(self.sorted, "sort() before querying the chunk");
        self.order.first().map(|i| self.key_at(*i as usize))
    }

    /// The largest key, or `None` when empty. Requires [`Self::sort`].
    pub fn max(&self) -> Option<&[u8]> {
        debug_assert!(self.sorted, "sort() before querying the chunk");
        self.order.last().map(|i| self.key_at(*i as usize))
    }

    /// Index into sorted order of the first key not less than `key`.
    ///
    /// Requires [`Self::sort`].
    pub fn lower_bound(&self, key: &[u8]) -> usize {
        debug_assert!(self.sorted, "sort() before querying the chunk");
        self.order.partition_point(|i| self.key_at(*i as usize) < key)
    }

    /// Whether `key` is present. Requires [`Self::sort`].
    pub fn contains(&self, key: &[u8]) -> bool {
        self.position(key).is_some()
    }

    /// Index of `key` in sorted order, or `None` when absent.
    ///
    /// The lookup uses the index to count how many distinct keys it found, which is what
    /// distinguishes "this key is not in the table" from "this key is in two files".
    /// Requires [`Self::sort`].
    pub fn position(&self, key: &[u8]) -> Option<usize> {
        let i = self.lower_bound(key);
        (i < self.order.len() && self.key_at(self.order[i] as usize) == key).then_some(i)
    }

    /// Whether any key falls within `[min, max]` inclusive.
    ///
    /// This is the exact test candidate pruning applies to a file or row group's key
    /// statistics: one lower-bound search rather than an interval approximation.
    /// Requires [`Self::sort`].
    pub fn intersects(&self, min: &[u8], max: &[u8]) -> bool {
        let i = self.lower_bound(min);
        i < self.order.len() && self.key_at(self.order[i] as usize) <= max
    }
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
        chunk.extend(&rows);
        chunk.sort();
        (chunk, rows)
    }

    #[test]
    fn contains_finds_members_and_rejects_others() {
        let (chunk, _) = chunk_of(&[5, 1, 9, 3]);
        let probe = encode(&[1, 3, 5, 9, 0, 4, 10]);
        for i in 0..4 {
            assert!(chunk.contains(probe.row(i).as_ref()), "missing member {i}");
        }
        for i in 4..7 {
            assert!(!chunk.contains(probe.row(i).as_ref()), "false hit {i}");
        }
    }

    #[test]
    fn sort_is_idempotent_and_order_is_by_encoded_bytes() {
        let (mut chunk, _) = chunk_of(&[5, 1, 9, 3]);
        chunk.sort();
        let expected = encode(&[1, 9]);
        assert_eq!(chunk.min().unwrap(), expected.row(0).as_ref());
        assert_eq!(chunk.max().unwrap(), expected.row(1).as_ref());
    }

    #[test]
    fn intersects_matches_exact_set_membership() {
        // Keys are clustered at the ends, so an interval test over [min, max] would keep
        // the middle range while the exact test prunes it.
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
        assert!(chunk.min().is_none());
    }

    #[test]
    fn budget_triggers_and_clear_resets() {
        let rows = encode(&[1, 2, 3, 4, 5, 6, 7, 8]);
        // Budget sized so that a handful of keys crosses it.
        let mut chunk = LookupChunk::new(64);
        let mut pushed = 0;
        while !chunk.is_full() {
            chunk.push(rows.row(pushed));
            pushed += 1;
            assert!(pushed <= rows.num_rows(), "budget never reached");
        }
        assert!(chunk.len() > 0);

        chunk.clear();
        assert!(chunk.is_empty());
        assert!(!chunk.is_full());
        assert_eq!(chunk.bytes(), 4, "only the leading offset remains");
    }

    #[test]
    fn duplicate_keys_are_tolerated() {
        // The indexed cursor yields each key once, but a chunk must not misbehave if it
        // ever sees a repeat.
        let (chunk, rows) = chunk_of(&[7, 7, 7]);
        assert_eq!(chunk.len(), 3);
        assert!(chunk.contains(rows.row(0).as_ref()));
    }
}
