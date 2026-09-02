//! Bit array, hash schedule and index reduction.
//!
//! # Provenance
//!
//! [`HashChain`] and [`index`] are derived from the `fastbloom` crate, version
//! 0.14.0, Copyright (c) 2023 Thomas Pendock, <https://github.com/tomtomwombat/fastbloom>,
//! dual licensed MIT OR Apache-2.0, which are this crate's terms as well.
//!
//! They are reproduced here rather than called for two reasons.
//!
//! `fastbloom` starts a fresh hash chain inside every `insert_hash` and
//! `contains_hash`. A modular filter wants one chain threaded across all of its
//! modules, so that module `j` consumes derived hashes `j*b .. (j+1)*b` and the
//! filter pays for one chain rather than one per module. The crate's API does
//! not allow that: its hasher and its index function are both private, its bit
//! array is readable but not writable, and `insert_hash` takes only a source
//! hash.
//!
//! Reproducing the schedule *exactly* is also what keeps monolithic filters
//! readable. Their bits were laid down by this schedule, and a
//! one-module filter walks it identically, so those files load and probe as they
//! always did. `fastbloom` is kept as a dev-dependency and the tests check our
//! output against it directly, which turns that compatibility from an assumption
//! into an assertion.

/// The derived-hash sequence a single key walks.
///
/// Derived from `fastbloom`'s `DoubleHasher`, which adapts Kirsch and
/// Mitzenmacher: one real hash is split into `h1` and `h2`, and further hashes
/// come from `h1 = (h1 + h2).rotate_left(5)`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct HashChain {
    h1: u64,
    h2: u64,
}

impl HashChain {
    /// Starts the chain for `hash`.
    #[inline(always)]
    pub(crate) fn new(hash: u64) -> Self {
        Self {
            h1: hash,
            h2: (hash >> 32).wrapping_mul(0x51_7c_c1_b7_27_22_0a_95),
        }
    }

    /// Advances the chain and returns the next derived hash.
    #[inline(always)]
    pub(crate) fn next(&mut self) -> u64 {
        self.h1 = self.h1.wrapping_add(self.h2).rotate_left(5);
        self.h1
    }
}

/// Reduces `hash` to a bit position within a module of `words` 64-bit words.
///
/// Derived from `fastbloom`'s `index`: Lemire's fastrange over the top 32 bits
/// of the hash.
///
/// `fastbloom` 0.14 evaluates `((hash >> 32) * bits) >> 32` in 64-bit
/// arithmetic, where the product wraps once `bits` passes 2^32 and the upper
/// part of the filter becomes unreachable (upstream issue 22, fixed in 0.16).
///
/// This form is exact at every size a module can reach, and still costs one
/// 64-bit multiply. A module holds `words * 64` bits, and multiplying by 64
/// before shifting right by 32 is the same as shifting right by 26, so the
/// factor of 64 never enters the product. Both factors are then below 2^32 and
/// the product cannot overflow. Widening to 128 bits would give the same answer
/// but costs a second multiply and blocks the compiler from folding the shifts
/// that follow.
#[inline(always)]
pub(crate) fn index(words: u64, hash: u64) -> u64 {
    ((hash >> 32) * words) >> 26
}

/// Sets bit `bit` of `words`.
#[inline(always)]
pub(crate) fn set(words: &mut [u64], bit: u64) {
    words[(bit >> 6) as usize] |= 1u64 << (bit & 63);
}

/// Returns whether bit `bit` of `words` is set.
#[inline(always)]
pub(crate) fn test(words: &[u64], bit: u64) -> bool {
    (words[(bit >> 6) as usize] >> (bit & 63)) & 1 == 1
}

#[cfg(test)]
mod tests {
    use super::{HashChain, index};

    /// 2^64 divided by the golden ratio, and odd, which is the multiplier
    /// Fibonacci hashing uses to spread sequential inputs across the whole
    /// word. These tests feed counters, so without it the low bits would barely
    /// move and the schedules under comparison would hardly be exercised.
    const GOLDEN_RATIO_64: u64 = 0x9E37_79B9_7F4A_7C15;

    /// The schedule must match `fastbloom`'s, or filters written before this
    /// crate stop reading correctly. This walks the two side by side over a
    /// whole filter rather than trusting that the transcription was faithful.
    #[test]
    fn schedule_matches_fastbloom() {
        for (words, hashes) in [(1usize, 1u32), (64, 3), (1_000, 7), (29_954, 13)] {
            let mut ours = vec![0u64; words];
            let mut theirs = fastbloom::BloomFilter::from_vec(vec![0u64; words])
                .seed(&0)
                .hashes(hashes);

            for i in 0..5_000u64 {
                let hash = i.wrapping_mul(GOLDEN_RATIO_64) ^ (i >> 7);
                let mut chain = HashChain::new(hash);
                for _ in 0..hashes {
                    super::set(&mut ours, index(words as u64, chain.next()));
                }
                theirs.insert_hash(hash);
            }

            assert_eq!(
                ours.as_slice(),
                theirs.as_slice(),
                "schedule diverges from fastbloom at {words} words, {hashes} hashes"
            );
        }
    }

    /// The reduction must agree with `fastbloom`'s wherever `fastbloom`'s is
    /// correct, which is what preserves compatibility with filters it wrote.
    #[test]
    fn index_matches_fastbloom_below_its_wrap() {
        for words in [1u64, 64, 1 << 14, 1 << 25, 1 << 26] {
            let bits = words * 64;
            for i in 0..200_000u64 {
                let hash = i.wrapping_mul(GOLDEN_RATIO_64);
                let original = ((hash >> 32).wrapping_mul(bits)) >> 32;
                assert_eq!(index(words, hash), original, "diverged at {bits} bits");
            }
        }
    }

    /// Above the wrap the original loses the top of the filter and this form
    /// does not. This pins the bug we are stepping around.
    #[test]
    fn index_reaches_bits_fastbloom_cannot() {
        let words = 1u64 << 28;
        let bits = words * 64;
        let reachable = (0..200_000u64)
            .map(|i| {
                let hash = i.wrapping_mul(GOLDEN_RATIO_64);
                index(words, hash)
            })
            .max()
            .unwrap();
        assert!(
            reachable > bits / 2,
            "index only reached {reachable} of {bits} bits"
        );

        let original_reach = (0..200_000u64)
            .map(|i| {
                let hash = i.wrapping_mul(GOLDEN_RATIO_64);
                ((hash >> 32).wrapping_mul(bits)) >> 32
            })
            .max()
            .unwrap();
        assert!(
            original_reach < bits / 2,
            "the 64-bit form was expected to lose the upper half, reached {original_reach}"
        );
    }
}
