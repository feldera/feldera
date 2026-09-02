use crate::{
    dynamic::DynVec,
    trace::{BatchReader, BatchReaderFactories, Cursor, cursor::CursorList},
};
use rand::Rng;

/// Splits `sample_size` draws among `batches` in proportion to their key counts.
///
/// Lays the batches' keys end to end and places a draw every
/// `total_keys / sample_size` positions, from a random offset into the first
/// interval. A batch holding some fraction of the keys draws that fraction of
/// the sample, rounded up or down by where the offset falls, and the shares sum
/// to `sample_size` exactly.
///
/// A batch is never asked for more keys than it holds, so asking for at least
/// as many keys as the batches hold draws every one of them.
fn apportion_draws<RG>(counts: &[usize], sample_size: usize, rng: &mut RG) -> Vec<usize>
where
    RG: Rng,
{
    let total_keys: usize = counts.iter().sum();
    let offset = if 0 < sample_size && sample_size < total_keys {
        rng.gen_range(0..total_keys)
    } else {
        // Every other case draws all of the keys or none of them, and the
        // offset does not come into it.
        0
    };

    apportion_draws_from(counts, sample_size, offset)
}

/// [`apportion_draws`] with the offset picked by the caller rather than at
/// random, so that a test can sweep every offset the generator could produce.
fn apportion_draws_from(counts: &[usize], sample_size: usize, offset: usize) -> Vec<usize> {
    let total_keys: usize = counts.iter().sum();
    if sample_size >= total_keys {
        return counts.to_vec();
    }

    let mut draws = vec![0usize; counts.len()];
    if sample_size == 0 {
        return draws;
    }
    debug_assert!(offset < total_keys);

    // Position of each draw among the combined keys. The product reaches
    // `total_keys` squared, so it needs more room than a `usize`.
    let position = |draw: usize| {
        ((offset as u128 + (draw as u128) * (total_keys as u128)) / (sample_size as u128)) as usize
    };

    let mut next = 0;
    let mut keys_before_next_batch = 0;
    for (batch, &count) in counts.iter().enumerate() {
        keys_before_next_batch += count;
        while next < sample_size && position(next) < keys_before_next_batch {
            draws[batch] += 1;
            next += 1;
        }
    }
    debug_assert_eq!(next, sample_size);

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
    let draws = apportion_draws(&counts, sample_size, rng);
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
    use super::{apportion_draws, apportion_draws_from};
    use rand::SeedableRng;
    use rand_chacha::ChaCha8Rng;

    /// A generator seeded per call, so a failure reproduces.
    fn rng() -> ChaCha8Rng {
        ChaCha8Rng::seed_from_u64(0x5eed)
    }

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

    /// Every offset `apportion_draws` could pick for a spine small enough to
    /// sweep, and a spread over the range for one that is not.
    fn offsets(total_keys: usize) -> Vec<usize> {
        if total_keys <= 512 {
            (0..total_keys.max(1)).collect()
        } else {
            vec![
                0,
                total_keys / 4,
                total_keys / 2,
                total_keys - total_keys / 4,
                total_keys - 1,
            ]
        }
    }

    /// Everything `apportion_draws` could return for these arguments, one entry
    /// per offset, so that an invariant asserted over them holds whatever the
    /// generator picks.
    fn apportionments(counts: &[usize], sample_size: usize) -> Vec<Vec<usize>> {
        offsets(counts.iter().sum())
            .into_iter()
            .map(|offset| apportion_draws_from(counts, sample_size, offset))
            .collect()
    }

    /// The two invariants a caller depends on, over a few thousand spine shapes
    /// and every offset each of them can be drawn at.
    ///
    /// * The draws total `sample_size`, or every key the spine holds if it
    ///   holds fewer than that.
    /// * No batch draws more keys than it holds, so every draw apportioned to a
    ///   batch is one that batch can take.
    #[test]
    fn draws_sum_to_the_sample_size_and_fit_their_batches() {
        for counts in shapes() {
            let total_keys: usize = counts.iter().sum();

            for sample_size in SAMPLE_SIZES {
                for draws in apportionments(&counts, sample_size) {
                    assert_eq!(
                        draws.iter().sum::<usize>(),
                        sample_size.min(total_keys),
                        "counts={counts:?} sample_size={sample_size} draws={draws:?}"
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
    }

    /// The invariants over every small spine and every offset, exhaustively:
    /// each batch count from none to five, up to four batches, every sample size
    /// up to a dozen. These are the sizes where a rounding slip has nowhere to
    /// hide, and where the two early returns meet the general case.
    #[test]
    fn small_spines_hold_the_invariants_at_every_offset() {
        fn check(counts: &[usize]) {
            let total_keys: usize = counts.iter().sum();

            for sample_size in 0..=12 {
                for offset in 0..total_keys.max(1) {
                    let draws = apportion_draws_from(counts, sample_size, offset);
                    let context = format!(
                        "counts={counts:?} sample_size={sample_size} \
                         offset={offset} draws={draws:?}"
                    );

                    assert_eq!(
                        draws.iter().sum::<usize>(),
                        sample_size.min(total_keys),
                        "{context}"
                    );
                    for (&draw, &count) in draws.iter().zip(counts.iter()) {
                        assert!(draw <= count, "{context}");
                        // A batch with no keys has none to give.
                        assert!(count > 0 || draw == 0, "{context}");
                        if sample_size < total_keys {
                            // Within one of the share the batch's keys are worth.
                            let share = count * sample_size;
                            assert!(
                                (share / total_keys..=share.div_ceil(total_keys)).contains(&draw),
                                "{context}"
                            );
                        }
                    }
                }
            }
        }

        let mut counts = Vec::new();
        for batches in 0..=4 {
            counts.clear();
            counts.resize(batches, 0);
            loop {
                check(&counts);

                // Odometer over every combination of counts in 0..=5.
                let Some(carry) = counts.iter().position(|&count| count < 5) else {
                    break;
                };
                counts[carry] += 1;
                counts[..carry].fill(0);
            }
        }
    }

    /// Summed over every offset the generator could pick, a batch draws exactly
    /// the share its keys are worth.
    #[test]
    fn draws_are_unbiased_over_the_offsets() {
        for counts in [
            vec![3, 5],
            vec![7, 0, 13, 0, 5],
            vec![1; 20],
            vec![1_000, 10, 10, 10],
            vec![500; 40],
        ] {
            let total_keys: usize = counts.iter().sum();

            for sample_size in [1, 2, 3, 7, 17, 100] {
                if sample_size >= total_keys {
                    continue;
                }

                let mut totals = vec![0usize; counts.len()];
                for offset in 0..total_keys {
                    for (total, draw) in
                        totals
                            .iter_mut()
                            .zip(apportion_draws_from(&counts, sample_size, offset))
                    {
                        *total += draw;
                    }
                }

                for (&total, &count) in totals.iter().zip(counts.iter()) {
                    assert_eq!(
                        total,
                        count * sample_size,
                        "counts={counts:?} sample_size={sample_size} totals={totals:?}"
                    );
                }
            }
        }
    }

    /// Batches large enough that a draw's position outgrows a `usize` on the way
    /// to being computed.
    #[test]
    fn a_huge_spine_does_not_overflow() {
        let counts = vec![usize::MAX / 4; 3];
        let total_keys: usize = counts.iter().sum();

        for offset in [0, total_keys / 2, total_keys - 1] {
            let draws = apportion_draws_from(&counts, 100, offset);

            assert_eq!(draws.iter().sum::<usize>(), 100);
            for &draw in &draws {
                assert!((33..=34).contains(&draw), "draws={draws:?}");
            }
        }
    }

    /// The offset `apportion_draws` picks is always one `apportion_draws_from`
    /// accepts, over a spread of generator states.
    #[test]
    fn the_offset_stays_in_range() {
        for seed in 0..256 {
            let mut rng = ChaCha8Rng::seed_from_u64(seed);
            for counts in [vec![1, 1], vec![3, 5], vec![7, 0, 13, 0, 5], vec![500; 40]] {
                let total_keys: usize = counts.iter().sum();
                for sample_size in [0, 1, 2, 7, 100, 100_000] {
                    let draws = apportion_draws(&counts, sample_size, &mut rng);
                    assert_eq!(draws.iter().sum::<usize>(), sample_size.min(total_keys));
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
                    apportion_draws(&counts, sample_size, &mut rng()),
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
            apportion_draws(&[10_000, 5_000, 5_000], 100, &mut rng()),
            vec![50, 25, 25]
        );
    }

    /// Sizes spanning orders of magnitude draw in proportion all the same, so
    /// the sample stays representative of the keys rather than of the batches.
    #[test]
    fn draws_track_key_counts_across_orders_of_magnitude() {
        let mut counts = vec![25_000_000; 10];
        counts.extend(std::iter::repeat_n(13_332, 190));
        let total_keys: usize = counts.iter().sum();

        let draws = apportion_draws(&counts, 1_000, &mut rng());

        assert_eq!(draws.iter().sum::<usize>(), 1_000);
        let large: usize = draws[..10].iter().sum();
        let large_share = 250_000_000 * 1_000 / total_keys;
        assert!(
            large.abs_diff(large_share) <= 10,
            "the ten large batches hold {large_share} draws' worth of keys and drew {large}"
        );
    }

    /// The draws reach across the whole run of batches.
    #[test]
    fn draws_reach_across_the_whole_spine() {
        const BATCHES: usize = 200;
        const SAMPLE_SIZE: usize = 144;

        let draws = apportion_draws(&vec![500; BATCHES], SAMPLE_SIZE, &mut rng());
        assert_eq!(draws.iter().sum::<usize>(), SAMPLE_SIZE);

        let drawn: Vec<usize> = draws
            .iter()
            .enumerate()
            .filter(|&(_, &draw)| draw > 0)
            .map(|(batch, _)| batch)
            .collect();
        let widest_gap = drawn
            .windows(2)
            .map(|pair| pair[1] - pair[0])
            .chain([drawn[0] + 1, BATCHES - drawn[drawn.len() - 1]])
            .max()
            .unwrap();
        assert!(
            widest_gap <= 3,
            "{widest_gap} batches in a row drew nothing: {drawn:?}"
        );
    }

    /// Empty batches never draw, and they do not consume anyone else's draw.
    #[test]
    fn empty_batches_do_not_draw() {
        assert_eq!(
            apportion_draws(&[50, 0, 50, 0], 10, &mut rng()),
            vec![5, 0, 5, 0]
        );
    }

    /// With more non-empty batches than draws, no batch draws more than once.
    #[test]
    fn more_batches_than_draws_still_sums_to_the_sample_size() {
        let draws = apportion_draws(&vec![1; 200], 100, &mut rng());

        assert_eq!(draws.iter().sum::<usize>(), 100);
        assert!(draws.iter().all(|&draw| draw <= 1));
    }

    /// A spine where every batch falls below one share still spends the whole
    /// sample, over enough batches for a caller to cut it into runs.
    #[test]
    fn a_spine_of_many_small_batches_still_draws_the_whole_sample() {
        /// Runs the sample is to be cut into, standing in for a caller's
        /// partition count.
        const RUNS: usize = 12;
        const SAMPLE_SIZE: usize = 144;

        // Batch counts and sizes spanning four orders of magnitude, as a spine's
        // levels do.
        const TIERS: [(usize, usize); 4] = [
            // (batches, keys per batch)
            (1_000, 10_000),
            (500, 100_000),
            (200, 1_000_000),
            (100, 5_000_000),
        ];
        let counts: Vec<usize> = TIERS
            .iter()
            .flat_map(|&(batches, keys)| std::iter::repeat_n(keys, batches))
            .collect();

        let total_keys: usize = counts.iter().sum();
        assert!(counts.len() > SAMPLE_SIZE);
        assert!(
            counts.iter().all(|&count| count < total_keys / SAMPLE_SIZE),
            "the shape under test is one where no batch reaches a full share"
        );

        let draws = apportion_draws(&counts, SAMPLE_SIZE, &mut rng());

        assert_eq!(draws.iter().sum::<usize>(), SAMPLE_SIZE);
        assert!(draws.iter().filter(|&&draw| draw > 0).count() >= RUNS);
    }

    /// A sample size of zero, and a spine holding no keys at all, draw nothing.
    #[test]
    fn degenerate_inputs_draw_nothing() {
        assert_eq!(apportion_draws(&[10, 20], 0, &mut rng()), vec![0, 0]);
        assert_eq!(apportion_draws(&[0, 0], 100, &mut rng()), vec![0, 0]);
        assert_eq!(apportion_draws(&[], 100, &mut rng()), Vec::<usize>::new());
    }
}
