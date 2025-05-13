use crate::{
    dynamic::WeightTrait,
    trace::{Batch, BatchLocation, BatchReader, Builder, Cursor},
    Runtime,
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
    pub fn for_capacity(capacity: usize) -> Self {
        match Runtime::min_step_storage_bytes().unwrap_or(usize::MAX) {
            usize::MAX => {
                // Storage is disabled.
                Self::Memory
            }

            min_step_storage_bytes if capacity.saturating_mul(32) >= min_step_storage_bytes => {
                // Just guess that this will need to go to storage.
                //
                // 32 bytes per item is a guess.  I don't know a better way to
                // guess, short of having the caller provide it.
                Self::Storage
            }
            min_step_storage_bytes => {
                // Start out in memory and spill to storage if
                // `min_storage_bytes` is used.
                Self::Threshold(min_step_storage_bytes)
            }
        }
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
    match Runtime::min_index_storage_bytes().unwrap_or(usize::MAX) {
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
