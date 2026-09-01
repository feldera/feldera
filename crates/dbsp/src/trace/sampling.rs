use crate::{
    dynamic::DynVec,
    trace::{BatchReader, BatchReaderFactories, Cursor, cursor::CursorList},
};
use rand::Rng;

/// Splits `sample_size` draws among `batches` in proportion to their key counts.
///
/// Walks the batches from largest to smallest, giving each its share of the
/// draws that are left, and stops once the draws run out.
///
/// Every batch draws at least once while draws are left, so keys held only by
/// small batches can still reach the sample. A batch is never asked for more
/// keys than it holds, so asking for at least as many keys as the spine holds
/// draws every one of them.
fn apportion_draws(counts: &[usize], sample_size: usize) -> Vec<usize> {
    let mut largest_first: Vec<usize> = (0..counts.len())
        .filter(|&index| counts[index] > 0)
        .collect();
    largest_first
        .sort_unstable_by(|&left, &right| counts[right].cmp(&counts[left]).then(left.cmp(&right)));

    let mut draws = vec![0usize; counts.len()];
    let mut remaining_draws = sample_size;
    let mut remaining_keys: usize = counts.iter().sum();

    for index in largest_first {
        if remaining_draws == 0 {
            break;
        }

        let count = counts[index];
        let fair_share =
            (count as u128 * remaining_draws as u128 / remaining_keys as u128) as usize;

        draws[index] = fair_share.max(1).min(count).min(remaining_draws);
        remaining_draws -= draws[index];
        remaining_keys -= count;
    }

    draws
}

/// Samples keys from a set of batches by invoking each batch's
/// [`BatchReader::sample_keys`] implementation and merging the results.
///
/// `sample_size` is split across the batches in proportion to their key counts.
/// The helper deduplicates keys across batches and drops keys whose weights sum
/// to zero, which keeps it usable for overlapping inputs such as merge planning.
/// Both of those shrink the result, so the sample can hold fewer than
/// `sample_size` keys.
pub(crate) fn sample_keys_from_batches<B, RG>(
    factories: &B::Factories,
    batches: &[&B],
    rng: &mut RG,
    sample_size: usize,
    sample: &mut DynVec<B::Key>,
) where
    B: BatchReader,
    RG: Rng,
{
    if batches.is_empty() {
        return;
    }

    let counts: Vec<usize> = batches.iter().map(|batch| batch.key_count()).collect();
    let draws = apportion_draws(&counts, sample_size);
    let total_draws = draws.iter().sum::<usize>();
    if total_draws == 0 {
        return;
    }

    let mut intermediate = factories.keys_factory().default_box();
    let mut merged_cursor = CursorList::new(
        factories.weight_factory(),
        batches.iter().map(|batch| batch.cursor()).collect(),
    );
    intermediate.reserve(total_draws);

    for (batch, &draws) in batches.iter().zip(draws.iter()) {
        if draws == 0 {
            continue;
        }
        batch.sample_keys(rng, draws, intermediate.as_mut());
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

#[cfg(test)]
mod test {
    use super::apportion_draws;

    fn shapes() -> Vec<Vec<usize>> {
        let mut shapes: Vec<Vec<usize>> = vec![
            vec![20_000],
            vec![500; 40],
            vec![100; 200],
            vec![1; 1_000],
            vec![1_000_000, 1, 1, 1],
            vec![7, 0, 13, 0, 5],
            vec![3, 5],
        ];

        // Every small spine of three batches, empty ones included.
        for first in 0..6 {
            for second in 0..6 {
                for third in 0..6 {
                    shapes.push(vec![first, second, third]);
                }
            }
        }

        // Larger spines with uneven batches, from a fixed seed so a failure
        // reproduces.
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for _ in 0..2_000 {
            let batches = (next() % 60) as usize + 1;
            shapes.push((0..batches).map(|_| (next() % 5_000) as usize).collect());
        }

        shapes
    }

    const SAMPLE_SIZES: [usize; 9] = [0, 1, 2, 3, 7, 17, 100, 999, 100_000];

    /// The two invariants callers depend on, over a few thousand spine shapes.
    ///
    /// The total is what makes the sample size a usable denominator for a caller
    /// estimating a proportion; flooring a share computed once against the whole
    /// spine used to lose up to one draw per batch. No batch is asked for more
    /// keys than it holds, so every apportioned draw is one that can be taken.
    #[test]
    fn draws_sum_to_the_sample_size_and_fit_their_batches() {
        for counts in shapes() {
            let total_keys: usize = counts.iter().sum();

            for sample_size in SAMPLE_SIZES {
                let draws = apportion_draws(&counts, sample_size);

                assert_eq!(
                    draws.iter().sum::<usize>(),
                    sample_size.min(total_keys),
                    "counts={counts:?} sample_size={sample_size}"
                );
                for (index, (&draw, &count)) in draws.iter().zip(counts.iter()).enumerate() {
                    assert!(
                        draw <= count,
                        "counts={counts:?} sample_size={sample_size} \
                         batch {index} drew {draw} of {count}"
                    );
                }
            }
        }
    }

    /// Asking for at least as many keys as the spine holds draws every key.
    /// `BatchReader::sample_keys` promises an exhaustive sample in that case.
    #[test]
    fn a_full_sample_draws_every_key() {
        for counts in shapes() {
            let total_keys: usize = counts.iter().sum();

            for sample_size in [total_keys, total_keys + 1, total_keys * 2] {
                assert_eq!(
                    apportion_draws(&counts, sample_size),
                    counts,
                    "counts={counts:?} sample_size={sample_size}"
                );
            }
        }
    }

    /// A batch holding half the keys draws about half the sample.
    #[test]
    fn draws_track_key_counts() {
        assert_eq!(
            apportion_draws(&[10_000, 5_000, 5_000], 100),
            vec![50, 25, 25]
        );
    }

    /// A batch whose fair share rounds down to nothing still draws once, so keys
    /// held only by small batches can reach the sample.
    #[test]
    fn small_batches_draw_at_least_once() {
        assert_eq!(
            apportion_draws(&[1_000, 10, 10, 10], 100),
            vec![97, 1, 1, 1]
        );
    }

    /// The draws are spent largest batch first and stop when they run out, so a
    /// batch that dominates the spine can leave the smallest ones with nothing.
    #[test]
    fn a_dominant_batch_can_exhaust_the_draws() {
        assert_eq!(
            apportion_draws(&[1_000_000, 1, 1, 1], 100),
            vec![99, 1, 0, 0]
        );
    }

    /// Empty batches never draw, and they do not consume anyone else's draw.
    #[test]
    fn empty_batches_do_not_draw() {
        assert_eq!(apportion_draws(&[50, 0, 50, 0], 10), vec![5, 0, 5, 0]);
    }

    /// With more non-empty batches than draws, the largest batches take one each
    /// and the rest get nothing.
    #[test]
    fn more_batches_than_draws_still_sums_to_the_sample_size() {
        let draws = apportion_draws(&vec![1; 200], 100);

        assert_eq!(draws.iter().sum::<usize>(), 100);
        assert!(draws.iter().all(|&draw| draw <= 1));
    }

    /// A sample size of zero, and a spine holding no keys at all, draw nothing.
    #[test]
    fn degenerate_inputs_draw_nothing() {
        assert_eq!(apportion_draws(&[10, 20], 0), vec![0, 0]);
        assert_eq!(apportion_draws(&[0, 0], 100), vec![0, 0]);
        assert_eq!(apportion_draws(&[], 100), Vec::<usize>::new());
    }
}
