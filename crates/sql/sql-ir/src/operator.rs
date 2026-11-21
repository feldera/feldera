//! Concrete operator tags accepted at the compiler circuit boundary.

macro_rules! operator_kinds {
    ($($kind:ident),+ $(,)?) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        pub enum OperatorKind { $($kind),+ }

        impl OperatorKind {
            pub fn wire_name(self) -> &'static str {
                match self { $(Self::$kind => concat!("DBSP", stringify!($kind), "Operator")),+ }
            }

            pub(crate) fn from_wire(name: &str) -> Option<Self> {
                match name {
                    $(concat!("DBSP", stringify!($kind), "Operator") => Some(Self::$kind)),+,
                    _ => None,
                }
            }
        }
    };
}

operator_kinds! {
    Aggregate,
    AggregateLinearPostprocess,
    AggregateLinearPostprocessRetainKeys,
    AggregateZero,
    AntiJoin,
    Apply,
    Apply2,
    ApplyN,
    AsofJoin,
    AtomicSum,
    BinaryDistinct,
    Chain,
    ChainAggregate,
    ConcreteAsofJoin,
    Constant,
    ControlledKeyFilter,
    Deindex,
    Delay,
    DelayedIntegral,
    Delta,
    Differentiate,
    Distinct,
    Filter,
    FlatMap,
    FlatMapIndex,
    Hop,
    IncrementalConstant,
    IndexedTopK,
    InputMapWithWaterline,
    Integrate,
    IntegrateTraceRetainKeys,
    IntegrateTraceRetainNValues,
    IntegrateTraceRetainValues,
    Join,
    JoinFilterMap,
    JoinIndex,
    Lag,
    LeftJoin,
    LeftJoinFilterMap,
    LeftJoinIndex,
    Map,
    MapIndex,
    Negate,
    Nested,
    Noop,
    Now,
    PartitionedRollingAggregate,
    PartitionedRollingAggregateWithWaterline,
    Positive,
    PrimitiveAggregate,
    Rank,
    RowNumber,
    Sink,
    SourceLazyMap,
    SourceMap,
    SourceMultiset,
    StarJoin,
    StarJoinFilterMap,
    StarJoinIndex,
    StreamAggregate,
    StreamAntiJoin,
    StreamDistinct,
    StreamJoin,
    StreamJoinIndex,
    Subtract,
    Sum,
    UpsertFeedback,
    View,
    ViewDeclaration,
    Waterline,
    Weigh,
    WeightValidator,
    Window,
}

impl OperatorKind {
    /// Whether the operator owns state across steps of its circuit.
    pub fn is_stateful(self) -> bool {
        matches!(
            self,
            Self::Aggregate
                | Self::AggregateLinearPostprocess
                | Self::AggregateLinearPostprocessRetainKeys
                | Self::AntiJoin
                | Self::AsofJoin
                | Self::BinaryDistinct
                | Self::ChainAggregate
                | Self::ConcreteAsofJoin
                | Self::Delay
                | Self::DelayedIntegral
                | Self::Differentiate
                | Self::Distinct
                | Self::IncrementalConstant
                | Self::IndexedTopK
                | Self::InputMapWithWaterline
                | Self::Integrate
                | Self::Join
                | Self::JoinFilterMap
                | Self::JoinIndex
                | Self::Lag
                | Self::LeftJoin
                | Self::LeftJoinFilterMap
                | Self::LeftJoinIndex
                | Self::PartitionedRollingAggregate
                | Self::PartitionedRollingAggregateWithWaterline
                | Self::Positive
                | Self::PrimitiveAggregate
                | Self::Rank
                | Self::RowNumber
                | Self::SourceLazyMap
                | Self::SourceMap
                | Self::StarJoin
                | Self::StarJoinFilterMap
                | Self::StarJoinIndex
                | Self::StreamAggregate
                | Self::StreamJoin
                | Self::StreamJoinIndex
                | Self::UpsertFeedback
                | Self::Waterline
                | Self::Window
        )
    }

    pub fn supports_balanced_join(self) -> bool {
        matches!(
            self,
            Self::Join
                | Self::JoinIndex
                | Self::JoinFilterMap
                | Self::LeftJoin
                | Self::LeftJoinIndex
                | Self::LeftJoinFilterMap
        )
    }

    /// Deindex is implemented by a map with a projection closure. Both kinds
    /// participate in the same Java operator-equivalence comparison.
    pub fn cse_family(self) -> Self {
        if self == Self::Deindex {
            Self::Map
        } else {
            self
        }
    }
}
