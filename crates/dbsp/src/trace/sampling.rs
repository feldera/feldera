use crate::{
    dynamic::DynVec,
    trace::{BatchReader, BatchReaderFactories, Cursor, cursor::CursorList},
};
use rand::Rng;

/// Samples keys from a set of batches by invoking each batch's
/// [`BatchReader::sample_keys`] implementation and merging the results.
///
/// `sample_size_for` decides how many keys to request from each batch. The
/// helper deduplicates keys across batches before appending them to `sample`,
/// which keeps it usable for overlapping inputs such as merge planning.
pub(crate) fn sample_keys_from_batches<B, RG, F>(
    factories: &B::Factories,
    batches: &[&B],
    rng: &mut RG,
    sample_size_for: F,
    sample: &mut DynVec<B::Key>,
) where
    B: BatchReader,
    RG: Rng,
    F: Fn(&B) -> usize,
{
    if batches.is_empty() {
        return;
    }

    let total_sample_size = batches
        .iter()
        .map(|batch| sample_size_for(*batch))
        .sum::<usize>();
    if total_sample_size == 0 {
        return;
    }

    let mut intermediate = factories.keys_factory().default_box();
    let mut merged_cursor = CursorList::new(
        factories.weight_factory(),
        batches.iter().map(|batch| batch.cursor()).collect(),
    );
    intermediate.reserve(total_sample_size);

    for batch in batches {
        let sample_size = sample_size_for(*batch);
        if sample_size == 0 {
            continue;
        }
        batch.sample_keys(rng, sample_size, intermediate.as_mut());
    }

    intermediate.as_mut().sort_unstable();
    intermediate.dedup();
    for key in intermediate.dyn_iter_mut() {
        merged_cursor.seek_key(key);
        if let Some(current_key) = merged_cursor.get_key()
            && current_key == key
        {
            sample.push_ref(key);
        }
    }
}
