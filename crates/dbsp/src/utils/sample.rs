use rand::{seq::index::sample, Rng};

/// Compute a random sample of size `sample_size` of values in `slice`.
///
/// Pushes the random sample of values to the `output` vector in the order
/// in which values occur in `slice`, i.e., if `slice` is sorted, the output
/// will be sorted too.
pub fn sample_slice<T, RG>(slice: &[T], rng: &mut RG, sample_size: usize, output: &mut Vec<T>)
where
    T: Clone,
    RG: Rng,
{
    let size = slice.len();

    if sample_size >= size {
        output.reserve(size);

        for v in slice.iter() {
            output.push(v.clone());
        }
    } else {
        output.reserve(sample_size);

        let mut indexes = sample(rng, size, sample_size).into_vec();
        indexes.sort_unstable();
        for index in indexes.into_iter() {
            output.push(slice[index].clone());
        }
    }
}
