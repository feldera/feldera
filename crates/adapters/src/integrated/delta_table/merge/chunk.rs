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

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Bytes held, counting the offset and permutation vectors. The permutation is counted
    /// before `sort` allocates it, so the budget covers what sorting will cost.
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
        self.order
            .partition_point(|i| self.key_at(*i as usize) < key)
    }

    /// Whether `key` is present. Requires [`Self::sort`].
    pub fn contains(&self, key: &[u8]) -> bool {
        let i = self.lower_bound(key);
        i < self.order.len() && self.key_at(self.order[i] as usize) == key
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
/// budget between them, so one batch of large keys can overshoot it by any amount.
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
        assert!(chunk.min().is_none());
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
        assert!(chunk.len() > 0);

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
