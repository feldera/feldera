use crate::error::LayoutError;

/// The shape of a modular Bloom filter.
///
/// This is the metadata a caller persists alongside the module words and hands
/// back when reloading. It describes the filter as it was built, independently
/// of how many modules a particular reader chooses to keep resident.
///
/// All modules are the same size. Equal sizing is what keeps the ratio of
/// hashes to bits constant as modules are dropped, and therefore what keeps
/// every rung of the ladder optimally configured.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ModuleLayout {
    total_modules: u32,
    words_per_module: u32,
    hashes_per_module: u32,
}

impl ModuleLayout {
    /// Describes a filter of `total_modules` modules, each `words_per_module`
    /// 64-bit words wide, with `hashes_per_module` hashes set per key in each.
    ///
    /// The filter's effective hash count is `total_modules * hashes_per_module`.
    pub fn new(
        total_modules: u32,
        words_per_module: u32,
        hashes_per_module: u32,
    ) -> Result<Self, LayoutError> {
        if total_modules == 0 {
            return Err(LayoutError::NoModules);
        }
        if hashes_per_module == 0 {
            return Err(LayoutError::NoHashes);
        }
        if words_per_module == 0 {
            return Err(LayoutError::NoBits);
        }

        let overflow = || LayoutError::Overflow {
            modules: total_modules,
            words_per_module,
        };
        total_modules
            .checked_mul(hashes_per_module)
            .ok_or_else(overflow)?;
        let total_words = u64::from(total_modules) * u64::from(words_per_module);
        // Bit positions are computed in `u64`, and the words are allocated as
        // one contiguous block.
        total_words.checked_mul(64).ok_or_else(overflow)?;
        // On a 32-bit target the word count can exceed `usize`, which is the
        // filter not fitting in memory rather than a conversion mishap.
        usize::try_from(total_words)
            .map_err(|_| overflow())?
            .checked_mul(8)
            .ok_or_else(overflow)?;

        Ok(Self {
            total_modules,
            words_per_module,
            hashes_per_module,
        })
    }

    /// Chooses a layout that holds `estimated_keys` keys at
    /// `target_false_positive_rate` when all `modules` modules are resident.
    ///
    /// The hash count is rounded to a multiple of `modules` so that dropping a
    /// module removes a whole number of hashes. Bits are then sized to hit the
    /// requested rate exactly at that rounded hash count, which costs a fraction
    /// of a percent of memory against the unrounded optimum.
    pub fn for_keys(
        estimated_keys: u64,
        target_false_positive_rate: f64,
        modules: u32,
    ) -> Result<Self, LayoutError> {
        if !(target_false_positive_rate > 0.0 && target_false_positive_rate < 1.0) {
            return Err(LayoutError::InvalidFalsePositiveRate {
                rate: target_false_positive_rate,
            });
        }
        if modules == 0 {
            return Err(LayoutError::NoModules);
        }

        // Optimal hashes for the target rate is -log2(fp). Round to a whole
        // number per module, never below one, so a dropped module always sheds
        // hashes as well as bits.
        let ideal_hashes = -target_false_positive_rate.log2();
        let hashes_per_module = (ideal_hashes / f64::from(modules)).round().max(1.0);
        let hashes_per_module = hashes_per_module.min(f64::from(u32::MAX)) as u32;
        let sizing_overflow = || LayoutError::SizingOverflow {
            estimated_keys,
            target_false_positive_rate,
            modules,
        };
        let total_hashes = modules
            .checked_mul(hashes_per_module)
            .ok_or_else(sizing_overflow)?;

        // Solve for bits per key based on the rounded hash count, so the requested rate is met
        // exactly despite the rounding.
        //
        // 1. fp = fill^total_hashes
        // 2. fill = 1 - e^(-total_hashes/bits_per_key)
        //
        // =>
        //
        // fill = fp^(1/total_hashes)
        // bits_per_key = -total_hashes / ln(1.0-fill)
        let fill = target_false_positive_rate.powf(1.0 / f64::from(total_hashes));
        let bits_per_key = -f64::from(total_hashes) / (1.0 - fill).ln();

        let total_bits = (estimated_keys as f64 * bits_per_key).ceil();
        if !total_bits.is_finite() {
            return Err(sizing_overflow());
        }

        // Round the module up, never down: a module short of its computed size
        // silently misses the target rate.
        let total_words = (total_bits / 64.0).ceil().max(0.0);
        let words_per_module = (total_words / f64::from(modules)).ceil().max(1.0);
        if words_per_module > f64::from(u32::MAX) {
            return Err(sizing_overflow());
        }

        Self::new(modules, words_per_module as u32, hashes_per_module)
    }

    /// Describes a monolithic `fastbloom` filter of `words` words using
    /// `num_hashes` hashes as a one-module filter.
    pub fn monolithic(num_hashes: u32, words: usize) -> Result<Self, LayoutError> {
        let words = u32::try_from(words).map_err(|_| LayoutError::Overflow {
            modules: 1,
            words_per_module: u32::MAX,
        })?;
        Self::new(1, words, num_hashes)
    }

    /// Returns the same layout with `modules` modules.
    ///
    /// Useful for costing a hypothetical residency without holding the bits.
    pub fn with_modules(&self, modules: u32) -> Result<Self, LayoutError> {
        Self::new(modules, self.words_per_module, self.hashes_per_module)
    }

    /// Modules the filter was built with.
    pub fn total_modules(&self) -> u32 {
        self.total_modules
    }

    /// 64-bit words each module occupies.
    pub fn words_per_module(&self) -> u32 {
        self.words_per_module
    }

    /// Hashes set per key within each module.
    pub fn hashes_per_module(&self) -> u32 {
        self.hashes_per_module
    }

    /// Bits each module occupies.
    pub fn bits_per_module(&self) -> u64 {
        u64::from(self.words_per_module) * 64
    }

    /// Bytes each module occupies.
    pub fn bytes_per_module(&self) -> usize {
        self.words_per_module as usize * 8
    }

    /// Words the complete filter occupies.
    pub fn total_words(&self) -> u64 {
        u64::from(self.total_modules) * u64::from(self.words_per_module)
    }

    /// Bytes the complete filter occupies.
    pub fn total_bytes(&self) -> usize {
        self.total_words() as usize * 8
    }

    /// Hashes set per key across the complete filter.
    pub fn total_hashes(&self) -> u32 {
        self.total_modules * self.hashes_per_module
    }

    /// Bits per key the complete filter spends on `keys` keys.
    pub fn bits_per_key(&self, keys: u64) -> f64 {
        if keys == 0 {
            return f64::INFINITY;
        }
        (self.total_words() * 64) as f64 / keys as f64
    }
}
