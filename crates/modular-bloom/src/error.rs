use thiserror::Error;

/// Rejects a [`ModuleLayout`](crate::ModuleLayout) that cannot describe a
/// usable filter.
#[derive(Clone, Copy, Debug, Error, PartialEq)]
pub enum LayoutError {
    /// A filter must have at least one module.
    #[error("modular bloom filter needs at least one module")]
    NoModules,

    /// Each module must hash every key at least once.
    #[error("modular bloom filter needs at least one hash per module")]
    NoHashes,

    /// Each module must hold at least one word of bits.
    #[error("modular bloom filter needs at least one word per module")]
    NoBits,

    /// A false positive rate is only meaningful strictly between 0 and 1.
    #[error("target false positive rate {rate} is not in the open interval (0, 1)")]
    InvalidFalsePositiveRate {
        /// The rejected rate.
        rate: f64,
    },

    /// The filter is too large to describe or to address.
    ///
    /// A module's own size is bounded by the `u32` it is counted in, so this
    /// reports the whole filter overflowing rather than one module.
    #[error("filter of {modules} modules of {words_per_module} words each does not fit")]
    Overflow {
        /// Requested module count.
        modules: u32,
        /// Requested words per module.
        words_per_module: u32,
    },

    /// Returned by [`ModuleLayout::for_keys`](crate::ModuleLayout::for_keys)
    /// when the module size it derives from these three values is too large to
    /// describe. Raising `modules`, or the target rate, brings it back in range.
    #[error(
        "cannot size a filter for {estimated_keys} keys at rate {target_false_positive_rate:e} \
         across {modules} modules: the module it needs is larger than the maximum; \
         raise `modules` or the target rate"
    )]
    SizingOverflow {
        /// Keys the layout was asked to hold.
        estimated_keys: u64,
        /// Rate the layout was asked to meet.
        target_false_positive_rate: f64,
        /// Modules the layout was asked to use.
        modules: u32,
    },
}

/// Rejects module words that do not match the layout they are loaded against.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum LoadError {
    /// Words must cover a whole number of modules, since a partial module
    /// cannot be probed.
    #[error("{words} words is not a multiple of the {words_per_module}-word module size")]
    PartialModule {
        /// Words supplied by the caller.
        words: usize,
        /// Words each module occupies.
        words_per_module: u32,
    },

    /// The caller supplied more modules than the layout declares.
    #[error("{modules} modules supplied but the layout declares {total_modules}")]
    TooManyModules {
        /// Modules implied by the supplied words.
        modules: u32,
        /// Modules the layout declares.
        total_modules: u32,
    },
}
