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
    match Runtime::min_storage_bytes().unwrap_or(usize::MAX) {
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
