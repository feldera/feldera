//! Typed circuit facts and graph edits used by the supported optimizer passes.
//! Wire metadata and JVM identities belong to the decoder, not this module.

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct OperatorId(pub usize);

#[derive(Clone, Copy, Debug)]
pub(crate) struct ExprId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct Port {
    pub operator: OperatorId,
    pub output: usize,
}

pub(crate) struct Circuit {
    /// Topological order; every input names an earlier operator.
    pub operators: Vec<Operator>,
}

pub(crate) struct Operator {
    pub inputs: Vec<Port>,
    pub kind: OperatorKind,
}

pub(crate) enum OperatorKind {
    Source {
        primary_key: bool,
    },
    Now,
    Constant {
        weights: Vec<i64>,
        indexed: bool,
        stream: CollectionKind,
    },
    Distinct(CollectionKind),
    Sum,
    Negate,
    Positive,
    Noop,
    WeightValidator,
    Filter(FilterPredicate),
    View {
        preserve: bool,
    },
    Sink,
    Delay,
    Differentiate,
    Integrate,
    RetainTrace,
    Map(Closure),
    Deindex(Closure),
    FlatMap,
    FlatMapIndex,
    Window,
    ChainAggregate,
    InnerJoin(Closure, CollectionKind),
    SingleValue,
    ControlledFilter,
    StreamOperator {
        outputs: usize,
        empty: EmptyInputRule,
        collection: Option<CollectionKind>,
    },
}

#[derive(Clone, Copy)]
pub(crate) enum EmptyInputRule {
    Keep,
    First,
    Any,
    AtomicSum,
    Subtract,
    StreamAntiJoin,
    LeftJoin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CollectionKind {
    Snapshot,
    Delta,
}

pub(crate) enum FilterPredicate {
    Dynamic,
    Literal(bool),
    RowField(usize),
    IsNull {
        operand: ScalarOperand,
        nullable: bool,
    },
    Not(Box<FilterPredicate>),
    WrapBool(Box<FilterPredicate>),
    Comparison {
        kind: Comparison,
        left: ScalarOperand,
        right: ScalarOperand,
    },
    Null,
}

pub(crate) enum ScalarOperand {
    Row,
    Field(usize),
    Literal(u64),
    Arithmetic(Box<Arithmetic>),
}

pub(crate) struct Arithmetic {
    pub kind: ArithmeticKind,
    pub left: ScalarOperand,
    pub right: ScalarOperand,
    pub left_nullable: bool,
    pub right_nullable: bool,
}

pub(crate) enum ArithmeticKind {
    Add,
    Subtract,
    Multiply,
}

pub(crate) enum Comparison {
    Equal,
    NotEqual,
    Less,
    LessOrEqual,
    Greater,
    GreaterOrEqual,
}

pub(crate) enum Edit {
    Bypass,
    Retain(Vec<Port>),
    ReplaceIdentity(Port),
    Empty(CollectionKind),
    SnapshotConstant(Port),
    FoldedConstant(Constant),
    ReplaceDeindex(Vec<Port>),
    MapUnmatchedLeft(Port),
    MoveDifferentiate { map: OperatorId, input: Port },
}

#[derive(Clone)]
pub(crate) struct Constant {
    pub origin: OperatorId,
    pub value: ConstantValue,
}

#[derive(Clone)]
pub(crate) enum ConstantValue {
    Indexed {
        output_type: u64,
        function: u64,
    },
    Rows {
        element_type: u64,
        rows: Vec<(u64, i64)>,
    },
}

pub(crate) struct ConstantInput {
    pub value: Option<Constant>,
    pub map: Option<ConstantMap>,
}

pub(crate) struct ConstantMap {
    pub element_type: u64,
    pub body: ConstantMapBody,
}

pub(crate) enum ConstantMapBody {
    Identity,
    Literal(u64),
    Field(usize),
    Arithmetic(Arithmetic),
    Other,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum ScalarLiteral {
    Bool(Option<bool>),
    Int8(Option<i8>),
    Int32(Option<i32>),
    Int64(Option<i64>),
}

impl ScalarLiteral {
    pub fn is_null(self) -> bool {
        matches!(
            self,
            Self::Bool(None) | Self::Int8(None) | Self::Int32(None) | Self::Int64(None)
        )
    }
}

#[derive(Default)]
pub(crate) struct ConstantLiterals {
    pub scalars: std::collections::HashMap<u64, ScalarLiteral>,
    pub tuples: std::collections::HashMap<u64, Vec<u64>>,
    pub generated: std::collections::HashMap<u64, GeneratedInteger>,
    pub last_id: u64,
}

pub(crate) struct GeneratedInteger {
    pub template: u64,
    pub value: i32,
}
impl OperatorKind {
    pub fn outputs(&self) -> usize {
        match self {
            Self::ControlledFilter => 2,
            Self::StreamOperator { outputs, .. } => *outputs,
            _ => 1,
        }
    }
}

pub(crate) struct Closure {
    pub parameters: Vec<Type>,
    /// Postorder arena: expression edges always point to an earlier slot.
    pub expressions: Vec<Expr>,
    pub body: ExprId,
    /// Java's lineage analysis refuses control flow with returns or assignments.
    pub has_unhandled_control_flow: bool,
}

pub(crate) struct Expr {
    pub kind: ExprKind,
}

pub(crate) enum ExprKind {
    Unknown,
    Parameter(usize),
    Copy(ExprId),
    Borrow(ExprId),
    Deref(ExprId),
    Field(ExprId, usize),
    Tuple(Vec<ExprId>),
    Intersection(ExprId, ExprId),
    Cast {
        source: ExprId,
        from: Type,
        to: Type,
        safe: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Type {
    pub nullable: bool,
    pub kind: TypeKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TypeKind {
    Null,
    Ref(Box<Type>),
    Tuple(Vec<Type>),
    RawTuple(Vec<Type>),
    Integer { width: u64, signed: bool },
    String { precision: i64, fixed: bool },
    Decimal { precision: u64, scale: u64 },
    Scalar(Scalar),
    Result(Box<Type>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Scalar {
    Bool,
    Real,
    Double,
    Date,
    Time,
    Timestamp,
    TimestampTz,
    Variant,
    Uuid,
    USize,
    Void,
    Any,
    Str,
}

impl Type {
    pub fn has_single_value(&self) -> bool {
        if self.kind == TypeKind::Null {
            return true;
        }
        if self.nullable {
            return false;
        }
        match &self.kind {
            TypeKind::Ref(inner) => inner.has_single_value(),
            TypeKind::Tuple(fields) | TypeKind::RawTuple(fields) => fields.is_empty(),
            _ => false,
        }
    }
}
