use std::sync::Arc;

use smallvec::SmallVec;

use crate::bits::{HashChain, index, set, test};
use crate::density::{ModuleDensity, count_set_bits};
use crate::error::LoadError;
use crate::layout::ModuleLayout;

/// Module storage of a built filter.
///
/// Each module is its own allocation, so a prefix of the modules can be shared
/// with another filter without copying, and dropping a module frees exactly its
/// memory. Up to four handles are stored inline, which keeps them in the same
/// cache line as the filter for every count a caller is expected to use.
type Modules = SmallVec<[Arc<[u64]>; 4]>;

/// The bit positions that one key occupies, one module at a time.
///
/// Every position comes from one hash chain seeded by the key hash and advanced
/// once per position. Module 0 takes the first `hashes_per_module` positions,
/// module 1 the next `hashes_per_module`, and so on. Each position is reduced
/// into the `words_per_module` words of its module, so it never falls outside
/// that module.
///
/// Truncation depends on this order: the positions of the first `n` modules do
/// not depend on how many modules follow, so a filter that has dropped its
/// trailing modules probes the modules it kept at the positions insertion
/// wrote.
struct KeyPositions {
    /// Source of the derived hashes, advanced once per position.
    chain: HashChain,
    /// Size of one module, in 64-bit words.
    words_per_module: u64,
    /// Positions per module.
    hashes_per_module: u32,
}

impl KeyPositions {
    #[inline(always)]
    fn new(layout: &ModuleLayout, hash: u64) -> Self {
        Self {
            chain: HashChain::new(hash),
            words_per_module: u64::from(layout.words_per_module()),
            hashes_per_module: layout.hashes_per_module(),
        }
    }

    /// Yields the positions of the next module. Call it once per module, in
    /// module order, and consume each result before the next call.
    #[inline(always)]
    fn next_module(&mut self) -> impl Iterator<Item = u64> + '_ {
        let words_per_module = self.words_per_module;
        let chain = &mut self.chain;
        (0..self.hashes_per_module).map(move |_| index(words_per_module, chain.next()))
    }
}

/// Accumulates keys into every module of a filter.
///
/// Keys can only be added here, before [`Self::finish`]. A finished filter may
/// drop modules, and a key added after that would be missing from the dropped
/// ones, so reloading them later would report it absent.
#[derive(Debug)]
pub struct ModularBloomFilterBuilder {
    /// Each module in the allocation the finished filter keeps, so
    /// [`Self::finish`] copies no data. The builder holds the only handle to
    /// each one, and gives none out.
    modules: Vec<Arc<[u64]>>,
    layout: ModuleLayout,
}

impl Clone for ModularBloomFilterBuilder {
    /// Copies the modules, so each builder keeps the only handle to its own.
    fn clone(&self) -> Self {
        Self {
            modules: self.modules.iter().map(|m| Arc::from(&m[..])).collect(),
            layout: self.layout,
        }
    }
}

impl ModularBloomFilterBuilder {
    /// Allocates every module of `layout`, zeroed.
    pub fn new(layout: ModuleLayout) -> Self {
        let words = layout.words_per_module() as usize;
        Self {
            modules: (0..layout.total_modules())
                // SAFETY: all zero bits is a valid `u64`.
                .map(|_| unsafe { Arc::<[u64]>::new_zeroed_slice(words).assume_init() })
                .collect(),
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
        let mut positions = KeyPositions::new(&self.layout, hash);
        for module in &mut self.modules {
            // SAFETY: the builder holds the only handle to `module`, and
            // `&mut self` keeps any other reference to it from being live.
            // `Arc::get_mut` would check this with an atomic operation per
            // module, which makes building up to several times slower.
            let words = unsafe {
                std::slice::from_raw_parts_mut(Arc::as_ptr(module) as *mut u64, module.len())
            };
            for bit in positions.next_module() {
                set(words, bit);
            }
        }
    }

    /// Completes the filter with all modules resident, without copying them.
    pub fn finish(self) -> ModularBloomFilter {
        let density = ModuleDensity::new(self.modules.iter().map(|m| count_set_bits(m)).collect());
        ModularBloomFilter {
            modules: self.modules.into_iter().collect(),
            density,
            layout: self.layout,
        }
    }
}

/// A Bloom filter whose accuracy can be traded for memory by dropping modules.
///
/// A probe consults resident modules in order and stops at the first rejection,
/// so an absent key usually touches far fewer modules than are resident.
///
/// Cloning shares the module storage instead of copying it.
#[derive(Clone, Debug)]
pub struct ModularBloomFilter {
    /// Resident modules, in order starting at module 0.
    modules: Modules,
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
            modules: words.chunks(words_per_module).map(Arc::from).collect(),
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
    /// stored in order. Modules already resident are left untouched. Passing
    /// an `Arc<[u64]>` adds the module without copying it.
    ///
    /// Fails if `words` is the wrong length, if every module the layout
    /// describes is already resident, or if the recorded density covers this
    /// module and `words` has a different number of bits set. That count stands
    /// in for a checksum when the module was read back on its own.
    pub fn push_module(&mut self, words: impl Into<Arc<[u64]>>) -> Result<(), LoadError> {
        let words = words.into();
        if words.len() != self.layout.words_per_module() as usize {
            return Err(LoadError::PartialModule {
                words: words.len(),
                words_per_module: self.layout.words_per_module(),
            });
        }
        let index = self.resident_modules();
        if index >= self.layout.total_modules() {
            return Err(LoadError::TooManyModules {
                modules: index + 1,
                total_modules: self.layout.total_modules(),
            });
        }
        let actual = count_set_bits(&words);
        match self.density.set_bits().get(index as usize) {
            Some(&expected) if expected != actual => {
                return Err(LoadError::WrongSetBits {
                    module: index,
                    expected,
                    actual,
                });
            }
            Some(_) => {}
            None => self.density.push(actual),
        }
        self.modules.push(words);
        Ok(())
    }

    /// Returns `false` only when `hash` was definitely never inserted.
    #[inline]
    pub fn contains_hash(&self, hash: u64) -> bool {
        // Gets each module's words once, not once per bit: indexing the module
        // list per bit doubles the work of each bit test.
        let mut positions = KeyPositions::new(&self.layout, hash);
        self.modules.iter().all(|words| {
            let words: &[u64] = words;
            positions.next_module().all(|bit| test(words, bit))
        })
    }

    /// Shape the filter was built with, whatever its current residency.
    pub fn layout(&self) -> &ModuleLayout {
        &self.layout
    }

    /// Modules currently held in memory.
    pub fn resident_modules(&self) -> u32 {
        self.modules.len() as u32
    }

    /// Bytes the resident modules occupy.
    pub fn resident_bytes(&self) -> usize {
        self.modules.len() * self.layout.bytes_per_module()
    }

    /// Drops modules beyond the first `modules`, releasing their memory once no
    /// other filter shares them.
    ///
    /// Truncating to zero leaves a filter that admits every key, which is the
    /// same answer a batch with no filter gives. Truncating to at least the
    /// current residency does nothing.
    pub fn truncate(&mut self, modules: u32) {
        self.modules.truncate(modules as usize);
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
        self.modules.get(index as usize).map(|module| &**module)
    }

    /// Words of each resident module, in order.
    ///
    /// Allocates a vector of slice references; use [`Self::module`] to reach a
    /// single module without allocating.
    pub fn modules(&self) -> Vec<&[u64]> {
        self.modules.iter().map(|module| &**module).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{
        KeyPositions, ModularBloomFilter, ModularBloomFilterBuilder, ModuleDensity, ModuleLayout,
    };

    /// A clone of a builder has its own modules, so keys added to one are not
    /// in the other, and `finish` keeps the modules it built.
    #[test]
    fn a_cloned_builder_has_its_own_modules() {
        let layout = ModuleLayout::for_keys(1_000, 1e-4, 4).unwrap();
        let mut original = ModularBloomFilterBuilder::new(layout);
        original.insert_hash(1);
        let mut clone = original.clone();
        clone.insert_hash(2);
        let original = original.finish();
        let clone = clone.finish();
        assert!(original.contains_hash(1) && clone.contains_hash(1));
        assert!(clone.contains_hash(2));
        assert!(
            !original.contains_hash(2),
            "the clone wrote to the original"
        );
    }

    /// The positions of `hash` in the first `modules` modules, each as a
    /// module index and a bit offset into that module.
    fn bit_positions(layout: &ModuleLayout, modules: u32, hash: u64) -> Vec<(usize, u64)> {
        let mut positions = KeyPositions::new(layout, hash);
        (0..modules as usize)
            .flat_map(|module| {
                positions
                    .next_module()
                    .map(|bit| (module, bit))
                    .collect::<Vec<_>>()
            })
            .collect()
    }

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
                let full = bit_positions(&layout, modules, hash);
                assert_eq!(full.len(), modules as usize * hashes as usize);
                for prefix in 0..=modules {
                    let partial = bit_positions(&layout, prefix, hash);
                    assert_eq!(
                        partial,
                        full[..prefix as usize * hashes as usize],
                        "{modules} modules, prefix {prefix}"
                    );
                }
            }
        }
    }

    /// Every bit must land inside its own module, in module order, or
    /// truncation would drop bits a retained module still depends on.
    #[test]
    fn every_bit_lands_in_its_own_module() {
        let layout = ModuleLayout::new(6, 128, 3).unwrap();
        let bits = layout.bits_per_module();
        for i in 0..5_000u64 {
            let mut seen = 0usize;
            for (module, bit) in bit_positions(&layout, 6, sample(i)) {
                assert_eq!(module, seen / 3, "position {seen} is in the wrong module");
                assert!(bit < bits, "bit {bit} escaped module {module}");
                seen += 1;
            }
            assert_eq!(seen, 18);
        }
    }

    /// A clone shares its modules instead of copying them, so truncating a
    /// clone frees nothing that the original still holds.
    #[test]
    fn a_clone_shares_modules() {
        let layout = ModuleLayout::new(4, 64, 3).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..200u64 {
            builder.insert_hash(sample(i));
        }
        let full = builder.finish();
        let mut truncated = full.clone();
        truncated.truncate(2);

        assert_eq!(full.resident_modules(), 4);
        for m in 0..2 {
            assert!(
                std::ptr::eq(truncated.module(m).unwrap(), full.module(m).unwrap()),
                "module {m} was copied"
            );
        }
        for i in 0..200u64 {
            assert!(truncated.contains_hash(sample(i)), "false negative");
        }
    }

    /// A module whose bit count differs from the recorded density is refused,
    /// and the filter keeps what it had.
    #[test]
    fn a_module_that_does_not_match_its_density_is_refused() {
        let layout = ModuleLayout::new(4, 64, 3).unwrap();
        let mut builder = ModularBloomFilterBuilder::new(layout);
        for i in 0..200u64 {
            builder.insert_hash(sample(i));
        }
        let full = builder.finish();
        let mut filter = full.clone();
        filter.truncate(2);

        let mut wrong = full.module(2).unwrap().to_vec();
        wrong[0] ^= 1;
        assert!(matches!(
            filter.push_module(wrong),
            Err(super::LoadError::WrongSetBits { module: 2, .. })
        ));
        assert_eq!(filter.resident_modules(), 2);
        filter.push_module(full.module(2).unwrap()).unwrap();
        assert_eq!(filter.resident_modules(), 3);
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
