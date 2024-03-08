use crate::{
    dynamic::DynPair,
    operator::dynamic::sample::StreamSampleUniqueKeyValsFactories,
    trace::BatchReaderFactories,
    typed_batch::{DynOrdZSet, IndexedZSetReader, TypedBatch},
    RootCircuit, Stream, ZWeight,
};

pub use crate::operator::dynamic::sample::{default_quantiles, MAX_QUANTILES, MAX_SAMPLE_SIZE};
use crate::utils::Tup2;

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSetReader,
    B::Inner: Clone,
{
    /// Generates a uniform random sample of keys from `self`.
    ///
    /// At every clock tick, computes a random sample of the input batch
    /// using [`BatchReader::sample_keys`](`crate::trace::BatchReader::sample_keys`).
    /// The `sample_size` stream
    /// specifies the size of the sample to compute (use `0` when no sample
    /// is needed at the current clock cycle to make sure the operator
    /// doesn't waste CPU cycles).
    ///
    /// Maximal supported sample size is [`MAX_SAMPLE_SIZE`].  If the operator
    /// receives a larger `sample_size` value, it treats it as
    /// `MAX_SAMPLE_SIZE`.
    ///
    /// Outputs a Z-set containing randomly sampled keys.  Each key is output
    /// with weight `1` regardless of its weight or the number of associated
    /// values in the input batch.
    ///
    /// This is not an incremental operator.  It samples the input
    /// batch received at the current clock cycle and not the integral
    /// of the input stream.  Prefix the call to `stream_sample_keys()` with
    /// `integrate_trace()` to sample the integral of the input.
    ///
    /// WARNING: This operator (by definition) returns non-deterministic
    /// outputs.  As such it may not play well with most other DBSP operators
    /// and must be used with care.
    pub fn stream_sample_keys(
        &self,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, TypedBatch<B::Key, (), ZWeight, DynOrdZSet<B::DynK>>> {
        let factories = BatchReaderFactories::new::<B::Key, (), ZWeight>();

        self.inner()
            .dyn_stream_sample_keys(&factories, sample_size)
            .typed()
    }

    /// Generates a uniform random sample of (key,value) pairs from `self`,
    /// assuming that `self` contains exactly one value per key.
    ///
    /// Equivalent to `self.map(|(k, v)| (k, v)).stream_sample_keys()`,
    /// but is more efficient.
    #[allow(clippy::type_complexity)]
    pub fn stream_sample_unique_key_vals(
        &self,
        sample_size: &Stream<RootCircuit, usize>,
    ) -> Stream<
        RootCircuit,
        TypedBatch<Tup2<B::Key, B::Val>, (), ZWeight, DynOrdZSet<DynPair<B::DynK, B::DynV>>>,
    > {
        let factories = StreamSampleUniqueKeyValsFactories::new::<B::Key, B::Val>();

        self.inner()
            .dyn_stream_sample_unique_key_vals(&factories, sample_size)
            .typed()
    }

    /// Generates a subset of keys that partition the set of all keys in `self`
    /// into `num_quantiles + 1` approximately equal-size quantiles.
    ///
    /// Internally, this operator uses the
    /// [`stream_sample_keys`](`Self::stream_sample_keys`) operator to compute a
    /// uniform random sample of size `num_quantiles ^ 2` and then picks
    /// every `num_quantile`'s element of the sample.
    ///
    /// Maximal supported `num_quantiles` value is [`MAX_QUANTILES`].  If the
    /// operator receives a larger `num_quantiles` value, it treats it as
    /// `MAX_QUANTILES`.
    ///
    /// Outputs a Z-set containing `<=num_quantiles` keys.  Each key is output
    /// with weight `1` regardless of its weight or the number of associated
    /// values in the input batch.
    ///
    /// This is not an incremental operator.  It samples the input
    /// batch received at the current clock cycle and not the integral
    /// of the input stream.  Prefix the call to `stream_key_quantiles()` with
    /// `integrate_trace()` to sample the integral of the input.
    ///
    /// WARNING: This operator returns non-deterministic outputs, i.e.,
    /// feeding the same input twice can produce different outputs.  As such it
    /// may not play well with most other DBSP operators and must be used with
    /// care.
    pub fn stream_key_quantiles(
        &self,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<RootCircuit, TypedBatch<B::Key, (), ZWeight, DynOrdZSet<B::DynK>>> {
        let factories = BatchReaderFactories::new::<B::Key, (), ZWeight>();

        self.inner()
            .dyn_stream_key_quantiles(&factories, num_quantiles)
            .typed()
    }

    /// Generates a subset of (key, value) pairs that partition the set of all
    /// tuples in `self` `num_quantiles + 1` approximately equal-size quantiles,
    /// assuming that `self` contains exactly one value per key.
    ///
    /// Equivalent to `self.map(|(k, v)| (k,
    /// v)).stream_unique_key_val_quantiles()`, but is more efficient.
    #[allow(clippy::type_complexity)]
    pub fn stream_unique_key_val_quantiles(
        &self,
        num_quantiles: &Stream<RootCircuit, usize>,
    ) -> Stream<
        RootCircuit,
        TypedBatch<Tup2<B::Key, B::Val>, (), ZWeight, DynOrdZSet<DynPair<B::DynK, B::DynV>>>,
    > {
        let factories = StreamSampleUniqueKeyValsFactories::new::<B::Key, B::Val>();

        self.inner()
            .dyn_stream_unique_key_val_quantiles(&factories, num_quantiles)
            .typed()
    }
}
