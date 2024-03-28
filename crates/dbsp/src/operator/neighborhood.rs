use crate::{
    dynamic::{DynData, DynDataTyped, DynPair},
    operator::dynamic::neighborhood::{
        DynNeighborhoodDescr, NeighborhoodDescr, NeighborhoodFactories,
    },
    typed_batch::{BatchReader, DynBatchReader, DynOrdZSet, IndexedZSet, TypedBatch, TypedBox},
    utils::Tup2,
    RootCircuit, Stream, ZWeight,
};

pub type NeighborhoodDescrBox<K, V> =
    TypedBox<NeighborhoodDescr<K, V>, DynNeighborhoodDescr<DynData, DynData>>;

pub type NeighborhoodDescrStream<K, V> = Stream<RootCircuit, Option<NeighborhoodDescrBox<K, V>>>;

/// See [`crate::operator::DynNeighborhood`].
pub type Neighborhood<B> = TypedBatch<
    Tup2<i64, Tup2<<B as BatchReader>::Key, <B as BatchReader>::Val>>,
    (),
    ZWeight,
    DynOrdZSet<
        DynPair<
            DynDataTyped<i64>,
            DynPair<
                <<B as BatchReader>::Inner as DynBatchReader>::Key,
                <<B as BatchReader>::Inner as DynBatchReader>::Val,
            >,
        >,
    >,
>;

pub type NeighborhoodStream<B> = Stream<
    RootCircuit,
    TypedBatch<
        Tup2<i64, Tup2<<B as BatchReader>::Key, <B as BatchReader>::Val>>,
        (),
        ZWeight,
        DynOrdZSet<
            DynPair<
                DynDataTyped<i64>,
                DynPair<
                    <<B as BatchReader>::Inner as DynBatchReader>::Key,
                    <<B as BatchReader>::Inner as DynBatchReader>::Val,
                >,
            >,
        >,
    >,
>;

impl<B> Stream<RootCircuit, B>
where
    B: IndexedZSet,
{
    /// Returns a small contiguous range of rows ([`Neighborhood`]) of the input
    /// table.
    ///
    /// This operator helps to visualize the contents of the input table in a
    /// UI.  The UI client may not have enough throughput/memory to store the
    /// entire table, and will instead limit its state to a small range of
    /// rows that fit on the screen.  We specify such a range, or
    /// _neighborhood_, in terms of its center (or "anchor"), and the number
    /// of rows preceding and following the anchor (see
    /// [`NeighborhoodDescr`]).  The user may be interested in a static
    /// snapshot of the neighborhood or in a changing view.  Both modes are
    /// supported by this operator (see the `reset` argument).  The output of
    /// the operator is a stream of [`Neighborhood`]s.
    ///
    /// NOTE: This operator assumes that the integral of the input stream does
    /// not contain negative weights (which should normally be the case) and
    /// may produce incorrect outputs otherwise.
    ///
    /// # Arguments
    ///
    /// * `self` - a stream of changes to an indexed Z-set.
    ///
    /// * `neighborhood_descr` - contains the neighborhood descriptor to
    ///   evaluate at every clock tick.  Set to `None` to disable the operator
    ///   (it will output an empty neighborhood).
    ///
    /// # Output
    ///
    /// Outputs a stream of changes to the neighborhood.
    ///
    /// The output neighborhood will contain rows with indexes between
    /// `-descr.before` and `descr.after - 1`.  Row 0 is the anchor row, i.e.,
    /// is the first row in the input stream greater than or equal to
    /// `descr.anchor`.  If there is no such row (i.e., all rows in the input
    /// stream are smaller than the anchor), then the neighborhood will only
    /// contain negative indexes.
    ///
    /// The first index in the neighborhood may be greater
    /// than `-descr.before` if the input stream doesn't contain enough rows
    /// preceding the specified anchor.  The last index may be smaller than
    /// `descr.after - 1` if the input stream doesn't contain `descr.after`
    /// rows following the anchor point.
    pub fn neighborhood(
        &self,
        neighborhood_descr: &NeighborhoodDescrStream<B::Key, B::Val>,
    ) -> NeighborhoodStream<B> {
        let factories: NeighborhoodFactories<B::Inner> =
            NeighborhoodFactories::new::<B::Key, B::Val>();

        self.inner()
            .dyn_neighborhood(&factories, unsafe {
                &neighborhood_descr.transmute_payload()
            })
            .typed()
    }
}
