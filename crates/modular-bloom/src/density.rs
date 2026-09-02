use crate::layout::ModuleLayout;

/// How many bits are set in each module of a filter.
///
/// Recorded when the filter is built so that a later reader can work out what
/// accuracy a given number of modules buys without holding any of them. That is
/// what lets a caller decide how much of a stored filter to read before reading
/// any of it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ModuleDensity {
    set_bits: Vec<u64>,
}

impl ModuleDensity {
    /// Records `set_bits[i]` bits set in module `i`.
    pub fn new(set_bits: Vec<u64>) -> Self {
        Self { set_bits }
    }

    /// Counts the bits set in each module of `words`, which must hold whole
    /// modules of `layout` laid out in order.
    pub fn measure(layout: &ModuleLayout, words: &[u64]) -> Self {
        Self::new(
            words
                .chunks(layout.words_per_module() as usize)
                .map(|module| module.iter().map(|w| u64::from(w.count_ones())).sum())
                .collect(),
        )
    }

    /// Bits set in each module, in order.
    pub fn set_bits(&self) -> &[u64] {
        &self.set_bits
    }

    /// Modules this covers, which may be fewer than the layout describes.
    pub fn modules(&self) -> u32 {
        self.set_bits.len() as u32
    }

    /// Appends the count for one more module.
    pub fn push(&mut self, set_bits: u64) {
        self.set_bits.push(set_bits);
    }

    /// Fraction of bits set across the first `modules` modules.
    ///
    /// Returns 0.0 when nothing is covered.
    pub fn fill_ratio(&self, layout: &ModuleLayout, modules: u32) -> f64 {
        let covered = modules.min(self.modules()) as usize;
        if covered == 0 {
            return 0.0;
        }
        let set: u64 = self.set_bits[..covered].iter().sum();
        set as f64 / (covered as u64 * layout.bits_per_module()) as f64
    }

    /// False positive rate of a filter holding the first `modules` modules.
    ///
    /// `modules` above `layout.total_modules()` is clamped. Modules this does
    /// not cover are charged the mean fill of the ones it does, which is what a
    /// filter measured from a prefix falls back on; zero coverage gives 1.0,
    /// since nothing is known to reject anything.
    pub fn false_positive_rate(&self, layout: &ModuleLayout, modules: u32) -> f64 {
        let modules = modules.min(layout.total_modules()) as usize;
        if modules == 0 || self.set_bits.is_empty() {
            return 1.0;
        }

        // Multiply per-module rates rather than raising a mean fill to a power:
        // the mean form is biased by Jensen's inequality.
        let bits = layout.bits_per_module() as f64;
        let hashes = i32::try_from(layout.hashes_per_module()).unwrap_or(i32::MAX);
        let mean = self.fill_ratio(layout, self.modules());
        (0..modules)
            .map(|module| match self.set_bits.get(module) {
                Some(&set) => (set as f64 / bits).powi(hashes),
                None => mean.powi(hashes),
            })
            .product()
    }

    /// Smallest number of modules whose rate is at or below `target`.
    ///
    /// Returns `layout.total_modules()` when even the whole filter misses `target`,
    /// and 0 when `target` is 1.0 or above.
    pub fn modules_for(&self, layout: &ModuleLayout, target: f64) -> u32 {
        (0..=layout.total_modules())
            .find(|&modules| self.false_positive_rate(layout, modules) <= target)
            .unwrap_or_else(|| layout.total_modules())
    }
}

#[cfg(test)]
mod tests {
    use super::ModuleDensity;
    use crate::layout::ModuleLayout;

    fn layout() -> ModuleLayout {
        ModuleLayout::new(4, 1_000, 3).unwrap()
    }

    /// Half the bits set in every module gives 0.5^hashes per module.
    #[test]
    fn rate_is_the_product_of_the_per_module_rates() {
        let g = layout();
        let half = g.bits_per_module() / 2;
        let density = ModuleDensity::new(vec![half; 4]);

        assert_eq!(density.false_positive_rate(&g, 0), 1.0);
        for modules in 1..=4u32 {
            let expected = 0.5f64.powi(3 * modules as i32);
            let got = density.false_positive_rate(&g, modules);
            assert!(
                (got - expected).abs() < 1e-12,
                "{modules}: {got} vs {expected}"
            );
        }
        // A module count above the layout's is clamped to it.
        assert_eq!(
            density.false_positive_rate(&g, 99),
            density.false_positive_rate(&g, 4)
        );
    }

    /// A density that records fewer modules than it is asked about uses the
    /// mean of the modules it does record for the rest.
    #[test]
    fn partial_coverage_falls_back_to_the_mean() {
        let g = layout();
        let half = g.bits_per_module() / 2;
        let full = ModuleDensity::new(vec![half; 4]);
        let prefix = ModuleDensity::new(vec![half]);
        assert_eq!(
            prefix.false_positive_rate(&g, 4),
            full.false_positive_rate(&g, 4)
        );
    }

    /// With nothing recorded there is nothing to reject with.
    #[test]
    fn empty_density_admits_everything() {
        let g = layout();
        let empty = ModuleDensity::new(Vec::new());
        for modules in 0..=4u32 {
            assert_eq!(empty.false_positive_rate(&g, modules), 1.0);
        }
        assert_eq!(empty.modules_for(&g, 1e-4), 4);
    }

    #[test]
    fn modules_for_picks_the_smallest_sufficient_count() {
        let g = layout();
        let half = g.bits_per_module() / 2;
        let density = ModuleDensity::new(vec![half; 4]);

        // Each module contributes 0.5^3 = 1/8.
        assert_eq!(density.modules_for(&g, 1.0), 0);
        assert_eq!(density.modules_for(&g, 0.125), 1);
        assert_eq!(density.modules_for(&g, 0.02), 2);
        assert_eq!(density.modules_for(&g, 0.002), 3);
        assert_eq!(density.modules_for(&g, 0.0003), 4);
        // Unreachable targets give the whole filter rather than failing.
        assert_eq!(density.modules_for(&g, 1e-12), 4);
    }

    #[test]
    fn measure_counts_each_module() {
        let g = ModuleLayout::new(3, 2, 1).unwrap();
        // Module 0 has 3 bits set, module 1 has 1, module 2 has none.
        let words = vec![0b111u64, 0, 0b1, 0, 0, 0];
        let density = ModuleDensity::measure(&g, &words);
        assert_eq!(density.set_bits(), &[3, 1, 0]);
        assert_eq!(density.modules(), 3);
    }
}
