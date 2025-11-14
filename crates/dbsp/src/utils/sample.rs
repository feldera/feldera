use rand::{seq::index::sample, RngCore};

/// Compute a random sample of size `sample_size` of values in `slice`.
///
/// Invokes `callback` for each value in the sample in the order
/// in which values occur in `slice`.
pub fn sample_slice<T, F>(slice: &[T], rng: &mut dyn RngCore, sample_size: usize, callback: &mut F)
where
    F: FnMut(&T),
{
    let size = slice.len();

    if sample_size >= size {
        for v in slice.iter() {
            callback(v);
        }
    } else {
        let mut indexes = sample(rng, size, sample_size).into_vec();
        indexes.sort_unstable();
        for index in indexes.into_iter() {
            callback(&slice[index]);
        }
    }
}
