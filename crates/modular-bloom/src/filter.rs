use crate::bits::{HashChain, index, set, test};
use crate::density::ModuleDensity;
use crate::error::LoadError;
use crate::layout::ModuleLayout;

/// Given a key hash and a module count, yields the bit positions that key
/// occupies, as offsets into the modules laid out back to back.
///
/// Every position comes from one hash chain seeded by the hash and advanced
/// once per position. Positions are grouped by module and yielded in module
/// order: `hashes_per_module` of them land in module 0, the next
/// `hashes_per_module` in module 1, and so on. Each is reduced into the
/// `words_per_module` words of the module it belongs to, so a position never
/// falls outside that module.
struct BitPositions {
    /// Source of the derived hashes, advanced once per position.
    chain: HashChain,
    /// Size of one module, in 64-bit words.
    words_per_module: u64,
    /// Size of one module, in bits.
    bits_per_module: u64,
    /// Positions yielded per module.
    hashes_per_module: u32,
    /// Bit offset of the module currently being yielded.
    base: u64,
    /// Modules still to yield after the current one.
    modules_left: u32,
    /// Positions still to yield within the current module.
    hashes_left: u32,
}

impl BitPositions {
    /// Yields `modules * layout.hashes_per_module()` positions, covering the
    /// first `modules` modules of `layout`.
    ///
    /// What a smaller `modules` yields is a prefix of what a larger one yields.
    /// Truncation depends on that: a filter that has dropped its trailing
    /// modules must probe the modules it kept at the positions insertion wrote.
    #[inline(always)]
    fn new(layout: &ModuleLayout, modules: u32, hash: u64) -> Self {
        Self {
            chain: HashChain::new(hash),
            words_per_module: u64::from(layout.words_per_module()),
            bits_per_module: layout.bits_per_module(),
            hashes_per_module: layout.hashes_per_module(),
            base: 0,
            modules_left: modules,
            hashes_left: if modules == 0 {
                0
            } else {
                layout.hashes_per_module()
            },
        }
    }
}

impl Iterator for BitPositions {
    type Item = u64;

    #[inline(always)]
    fn next(&mut self) -> Option<u64> {
        if self.hashes_left == 0 {
            self.modules_left = self.modules_left.checked_sub(1)?;
            if self.modules_left == 0 {
                return None;
            }
            self.base += self.bits_per_module;
            self.hashes_left = self.hashes_per_module;
        }
        self.hashes_left -= 1;
        Some(self.base + index(self.words_per_module, self.chain.next()))
    }
}

/// Accumulates keys into every module of a filter.
///
/// Keys can only be added here, before [`Self::finish`]. A finished filter may
/// drop modules, and a key added after that would be missing from the dropped
/// ones, so reloading them later would report it absent.
#[derive(Clone, Debug)]
pub struct ModularBloomFilterBuilder {
    words: Vec<u64>,
    layout: ModuleLayout,
}

impl ModularBloomFilterBuilder {
    /// Allocates every module of `layout`, zeroed.
    pub fn new(layout: ModuleLayout) -> Self {
        Self {
            words: vec![0u64; layout.total_words() as usize],
            layout,
        }
    }

    /// Shape of the filter under construction.
    pub fn layout(&self) -> &ModuleLayout {
        &self.layout
    }

    /// Records `hash` in every module.
    ///
    /// Keys may arrive in any order and may repeat.
    // Callers drive this one key at a time from another crate. Inline to avoid call overhead.
    #[inline]
    pub fn insert_hash(&mut self, hash: u64) {
        let words = self.words.as_mut_slice();
        for bit in BitPositions::new(&self.layout, self.layout.total_modules(), hash) {
            set(words, bit);
        }
    }

    /// Completes the filter with all modules resident.
    pub fn finish(self) -> ModularBloomFilter {
        let density = ModuleDensity::measure(&self.layout, &self.words);
        ModularBloomFilter {
            words: self.words,
            density,
            layout: self.layout,
        }
    }
}

/// A Bloom filter whose accuracy can be traded for memory by dropping modules.
///
/// A probe consults resident modules in order and stops at the first rejection,
/// so an absent key usually touches far fewer modules than are resident.
#[derive(Clone, Debug)]
pub struct ModularBloomFilter {
    /// Resident modules, laid out back to back starting at module 0.
    words: Vec<u64>,
    /// Bits set per module. Covers every module the layout describes when the
    /// caller supplied a recorded density, otherwise only the resident ones.
    density: ModuleDensity,
    layout: ModuleLayout,
}

impl ModularBloomFilter {
    /// Loads a filter from `words`, which hold whole modules laid out in order
    /// starting at module 0.
    ///
    /// Pass all `layout.total_modules()` modules to load the filter at the
    /// accuracy it was written with. Pass the first `n` of them, meaning
    /// `n * layout.words_per_module()` words, to load it at the accuracy `n`
    /// modules give; the result holds only those modules and occupies
    /// `n * layout.bytes_per_module()` bytes.
    ///
    /// Fails if `words` is not a whole number of modules, or holds more modules
    /// than `layout` describes.
    pub fn from_modules(layout: ModuleLayout, words: &[u64]) -> Result<Self, LoadError> {
        let words_per_module = layout.words_per_module() as usize;
        if !words.len().is_multiple_of(words_per_module) {
            return Err(LoadError::PartialModule {
                words: words.len(),
                words_per_module: layout.words_per_module(),
            });
        }

        let resident = words.len() / words_per_module;
        if resident > layout.total_modules() as usize {
            return Err(LoadError::TooManyModules {
                modules: resident as u32,
                total_modules: layout.total_modules(),
            });
        }

        Ok(Self {
            density: ModuleDensity::measure(&layout, words),
            words: words.to_vec(),
            layout,
        })
    }

    /// Loads a filter from `words` together with the density recorded when it
    /// was built.
    ///
    /// `density` may cover more modules than `words` holds, which is the point:
    /// rates and [`Self::modules_for`] then answer for residencies the filter is
    /// not currently at, including from a filter loaded with no modules at all.
    /// A `density` covering fewer modules than `words` holds is discarded in
    /// favour of measuring them, which is what [`Self::from_modules`] does.
    pub fn from_modules_with_density(
        layout: ModuleLayout,
        words: &[u64],
        density: ModuleDensity,
    ) -> Result<Self, LoadError> {
        let mut filter = Self::from_modules(layout, words)?;
        if density.modules() >= filter.resident_modules() {
            filter.density = density;
        }
        Ok(filter)
    }

    /// Bits set per module, for a caller that wants to record them alongside
    /// the words.
    pub fn density(&self) -> &ModuleDensity {
        &self.density
    }

    /// Appends one more module, raising the filter's accuracy by one rung.
    ///
    /// `words` must hold exactly `layout().words_per_module()` words, and
    /// must be the module at index `resident_modules()`, since modules are
    /// stored in order. Modules already resident are left untouched.
    ///
    /// Fails if `words` is the wrong length, or if every module the layout
    /// describes is already resident.
    pub fn push_module(&mut self, words: &[u64]) -> Result<(), LoadError> {
        if words.len() != self.layout.words_per_module() as usize {
            return Err(LoadError::PartialModule {
                words: words.len(),
                words_per_module: self.layout.words_per_module(),
            });
        }
        if self.resident_modules() >= self.layout.total_modules() {
            return Err(LoadError::TooManyModules {
                modules: self.resident_modules() + 1,
                total_modules: self.layout.total_modules(),
            });
        }
        let appended = self.resident_modules() + 1;
        self.words.extend_from_slice(words);
        if self.density.modules() < appended {
            self.density
                .push(words.iter().map(|w| u64::from(w.count_ones())).sum());
        }
        Ok(())
    }

    /// Returns `false` only when `hash` was definitely never inserted.
    #[inline]
    pub fn contains_hash(&self, hash: u64) -> bool {
        BitPositions::new(&self.layout, self.resident_modules(), hash)
            .all(|bit| test(&self.words, bit))
    }

    /// Shape the filter was built with, whatever its current residency.
    pub fn layout(&self) -> &ModuleLayout {
        &self.layout
    }

    /// Modules currently held in memory.
    pub fn resident_modules(&self) -> u32 {
        (self.words.len() / self.layout.words_per_module() as usize) as u32
    }

    /// Bytes the resident modules occupy.
    pub fn resident_bytes(&self) -> usize {
        self.words.len() * 8
    }

    /// Drops modules beyond the first `modules`, releasing their memory.
    ///
    /// Truncating to zero leaves a filter that admits every key, which is the
    /// same answer a batch with no filter gives. Truncating to at least the
    /// current residency does nothing.
    pub fn truncate(&mut self, modules: u32) {
        if modules >= self.resident_modules() {
            return;
        }
        self.words
            .truncate(modules as usize * self.layout.words_per_module() as usize);
        self.words.shrink_to_fit();
    }

    /// Drops every module the filter can spare while still meeting
    /// `target_false_positive_rate`, and returns the new residency.
    ///
    /// The rate is taken from the bits actually set, not from an assumed key
    /// count, so the result holds even for a filter whose layout was sized
    /// from a bad estimate. Never grows the filter: a target the current
    /// residency cannot meet leaves it unchanged.
    pub fn retune(&mut self, target_false_positive_rate: f64) -> u32 {
        let wanted = self
            .modules_for(target_false_positive_rate)
            .min(self.resident_modules());
        self.truncate(wanted);
        self.resident_modules()
    }

    /// Smallest residency that would still meet `target_false_positive_rate`.
    ///
    /// Answers the question without changing the filter, so a memory budget can
    /// cost several filters before committing to any of them.
    pub fn modules_for(&self, target_false_positive_rate: f64) -> u32 {
        self.density
            .modules_for(&self.layout, target_false_positive_rate)
    }

    /// Fraction of bits set, measured over the resident modules.
    ///
    /// Modules are all the same size, so the mean of their fills is exactly the
    /// set bits over the resident bits. Dividing by the layout's full size
    /// instead would make a truncated filter under-report its own rate.
    pub fn fill_ratio(&self) -> f64 {
        self.density
            .fill_ratio(&self.layout, self.resident_modules())
    }

    /// False positive rate at the current residency, from the measured fill.
    pub fn false_positive_rate(&self) -> f64 {
        self.false_positive_rate_with(self.resident_modules())
    }

    /// False positive rate this filter would have with `modules` resident.
    ///
    /// Accepts any `modules` up to `layout().total_modules()`, including
    /// counts above the current residency; the fill of the resident modules
    /// stands in for the ones not held. Larger values are clamped.
    pub fn false_positive_rate_with(&self, modules: u32) -> f64 {
        self.density.false_positive_rate(&self.layout, modules)
    }

    /// Words of module `index`, or `None` if it is not resident.
    ///
    /// This is the handle a caller writes to storage. Modules are independent,
    /// so they may be written and read individually.
    pub fn module(&self, index: u32) -> Option<&[u64]> {
        let per = self.layout.words_per_module() as usize;
        let start = index as usize * per;
        self.words.get(start..start + per)
    }

    /// Words of each resident module, in order.
    ///
    /// Allocates a vector of slice references; use [`Self::module`] to reach a
    /// single module without allocating.
    pub fn modules(&self) -> Vec<&[u64]> {
        self.words
            .chunks(self.layout.words_per_module() as usize)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BitPositions, ModularBloomFilter, ModularBloomFilterBuilder, ModuleDensity, ModuleLayout,
    };

    fn sample(i: u64) -> u64 {
        let mut x = i;
        x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^ (x >> 31)
    }

    /// Probing a prefix of the modules must reproduce exactly the bits that
    /// insertion wrote for them. One chain serves every module, so a mistake in
    /// how it is advanced would silently shift a truncated filter's bits and
    /// turn every stored key into a false negative.
    #[test]
    fn a_prefix_walk_reproduces_the_full_walk() {
        for (modules, hashes) in [(1u32, 1u32), (4, 3), (8, 2), (13, 1), (5, 4)] {
            let layout = ModuleLayout::new(modules, 64, hashes).unwrap();
            for i in 0..2_000u64 {
                let hash = sample(i);
                let full: Vec<u64> = BitPositions::new(&layout, modules, hash).collect();
                assert_eq!(full.len(), modules as usize * hashes as usize);
                for prefix in 0..=modules {
                    let partial: Vec<u64> = BitPositions::new(&layout, prefix, hash).collect();
                    assert_eq!(
                        partial,
                        full[..prefix as usize * hashes as usize],
                        "{modules} modules, prefix {prefix}"
                    );
                }
            }
        }
    }

    /// Every bit must land inside its own module's region, or truncation would
    /// drop bits a retained module still depends on.
    #[test]
    fn every_bit_lands_in_its_own_module() {
        let layout = ModuleLayout::new(6, 128, 3).unwrap();
        let bits = layout.bits_per_module();
        for i in 0..5_000u64 {
            let mut seen = 0usize;
            for bit in BitPositions::new(&layout, 6, sample(i)) {
                let module = (seen / 3) as u64;
                assert!(
                    bit >= module * bits && bit < (module + 1) * bits,
                    "bit {bit} escaped module {module}"
                );
                seen += 1;
            }
            assert_eq!(seen, 18);
        }
    }

    /// Modules must carry independent load, not merely differ. Correlated
    /// modules produce no false negatives, so every membership test still passes
    /// while the filter's accuracy collapses to that of a single module.
    #[test]
    fn modules_carry_independent_load() {
        let keys = 200_000u64;
        let layout = ModuleLayout::for_keys(keys, 1e-4, 4).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..keys {
            builder.insert_hash(sample(i));
        }
        let filter = builder.finish();

        let fills: Vec<f64> = (0..4)
            .map(|m| {
                ModuleDensity::measure(&layout, filter.module(m).unwrap()).fill_ratio(&layout, 1)
            })
            .collect();
        let mean = fills.iter().sum::<f64>() / 4.0;
        let spread =
            fills.iter().cloned().fold(0.0, f64::max) - fills.iter().cloned().fold(1.0, f64::min);
        let band = 8.0 * (mean * (1.0 - mean) / layout.bits_per_module() as f64).sqrt();
        assert!(
            spread <= band,
            "module fill spans {spread:.5} around {mean:.4}, band {band:.5}: {fills:?}"
        );

        for a in 0..4u32 {
            for b in (a + 1)..4u32 {
                assert_ne!(
                    filter.module(a).unwrap(),
                    filter.module(b).unwrap(),
                    "modules {a} and {b} hold identical bits"
                );
            }
        }
    }

    /// A recorded density that covers less than the filter holds is discarded,
    /// and the resident modules are measured instead.
    ///
    /// Trusting it would let the filter report a rate no module of it has.
    #[test]
    fn a_short_density_is_replaced_by_measurement() {
        let layout = ModuleLayout::new(4, 64, 3).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..200u64 {
            builder.insert_hash(sample(i));
        }
        let words: Vec<u64> = builder.finish().modules().concat();
        let per = layout.words_per_module() as usize;
        let prefix = &words[..2 * per];

        // One entry for two resident modules, and a false one at that: an empty
        // module would make the filter claim it rejects everything.
        let short = ModuleDensity::new(vec![0]);
        let loaded = ModularBloomFilter::from_modules_with_density(layout, prefix, short).unwrap();
        let measured = ModularBloomFilter::from_modules(layout, prefix).unwrap();

        assert_eq!(loaded.density(), measured.density());
        assert_eq!(
            loaded.false_positive_rate_with(2),
            measured.false_positive_rate_with(2)
        );
        assert!(
            loaded.false_positive_rate_with(2) > 0.0,
            "the discarded density claims a rate of zero"
        );
    }
}
