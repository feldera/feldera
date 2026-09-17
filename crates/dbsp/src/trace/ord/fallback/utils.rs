use crate::{
    Runtime,
    dynamic::WeightTrait,
    trace::{Batch, BatchLocation, BatchReader, Builder, Cursor},
};

/// Reads all of the data from `cursor` and writes it to `builder`.
pub(super) fn copy_to_builder<B, Output, C, K, V, T, R>(builder: &mut B, mut cursor: C)
where
    B: Builder<Output>,
    Output: Batch<Key = K, Val = V, Time = T, R = R>,
    C: Cursor<K, V, T, R>,
    K: ?Sized,
    V: ?Sized,
    R: WeightTrait + ?Sized,
{
    while cursor.key_valid() {
        while cursor.val_valid() {
            cursor.map_times(&mut |time, diff| builder.push_time_diff(time, diff));
            builder.push_val(cursor.val());
            cursor.step_val();
        }
        builder.push_key(cursor.key());
        cursor.step_key();
    }
}

/// Bytes per item that a capacity guess is converted at, short of a better
/// estimate from the caller.
const GUESSED_BYTES_PER_ITEM: usize = 32;

/// The location where a [Builder] should build.
pub(super) enum BuildTo {
    /// Build in memory.
    Memory,

    /// Build in storage.
    Storage,

    /// Build in memory up to a maximum `.0` bytes in size, then spill to
    /// storage.
    Threshold(usize),
}

impl BuildTo {
    pub fn for_capacity(
        key_capacity: usize,
        value_capacity: usize,
        location: Option<BatchLocation>,
    ) -> Self {
        if let Some(location) = location {
            return location.into();
        }
        match Runtime::min_step_storage_bytes().unwrap_or(usize::MAX) {
            usize::MAX => {
                // Storage is disabled.
                Self::Memory
            }

            // A threshold of zero comes from Critical memory pressure or from
            // a user who set `min_step_storage_bytes` to 0, and either way the
            // point is to keep a step's records out of memory.  An empty batch
            // holds none, and a batch that turns out to hold some spills at
            // its first item through `Threshold` below.  Going to storage on
            // the capacity guess alone creates a layer file, and an fsync of
            // it, for every step that produces nothing.
            min_step_storage_bytes
                if min_step_storage_bytes > 0
                    && key_capacity
                        .saturating_add(value_capacity)
                        .saturating_mul(GUESSED_BYTES_PER_ITEM)
                        >= min_step_storage_bytes =>
            {
                // Just guess that this will need to go to storage.
                Self::Storage
            }
            min_step_storage_bytes => {
                // Start out in memory and spill to storage if
                // `min_storage_bytes` is used.
                Self::Threshold(min_step_storage_bytes)
            }
        }
    }

    /// How many of `capacity` items a builder that spills past `threshold`
    /// bytes should make room for up front: no more than fit under the
    /// threshold, since the rest spills before the room is used.  Under a
    /// zero threshold that is none, so a builder that turns out empty costs
    /// no memory, and one that receives an item spills at once.
    pub fn capped_capacity(threshold: usize, capacity: usize) -> usize {
        capacity.min(threshold / GUESSED_BYTES_PER_ITEM)
    }
}

impl From<BatchLocation> for BuildTo {
    fn from(location: BatchLocation) -> Self {
        match location {
            BatchLocation::Memory => Self::Memory,
            BatchLocation::Storage => Self::Storage,
        }
    }
}

pub fn pick_merge_destination<'a, B, I>(
    batches: I,
    dst_hint: Option<BatchLocation>,
) -> BatchLocation
where
    B: BatchReader,
    I: IntoIterator<Item = &'a B>,
{
    if let Some(location) = dst_hint {
        return location;
    }

    // This is equivalent to `batch1.byte_size() + batch2.byte_size() >=
    // Runtime::min_storage_bytes()` but it avoids calling `byte_size()` any
    // more than necessary since it can be expensive.
    match Runtime::min_merge_storage_bytes().unwrap_or(usize::MAX) {
        0 => BatchLocation::Storage,
        usize::MAX => BatchLocation::Memory,
        min_storage_bytes => {
            let mut size = 0;
            for b in batches {
                size += b.approximate_byte_size();
                if size >= min_storage_bytes {
                    return BatchLocation::Storage;
                }
            }

            BatchLocation::Memory
        }
    }
}

pub fn pick_insert_destination<B>(batch: &B) -> BatchLocation
where
    B: BatchReader,
{
    if batch.is_empty() {
        return BatchLocation::Memory;
    }
    match Runtime::min_insert_storage_bytes().unwrap_or(usize::MAX) {
        0 => BatchLocation::Storage,
        usize::MAX => BatchLocation::Memory,
        min_storage_bytes => {
            if batch.approximate_byte_size() >= min_storage_bytes {
                BatchLocation::Storage
            } else {
                BatchLocation::Memory
            }
        }
    }
}
