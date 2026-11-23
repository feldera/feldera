//! Concrete type and expression families in the DBSP circuit format. Other
//! inner objects include parameters, declarations, statements and struct fields.

macro_rules! wire_kinds {
    ($family:ident { $($kind:ident => $wire:literal),+ $(,)? }) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
        pub enum $family { $($kind),+ }

        impl $family {
            pub fn wire_name(self) -> &'static str {
                match self { $(Self::$kind => $wire),+ }
            }

            pub fn from_wire(name: &str) -> Option<Self> {
                match name { $($wire => Some(Self::$kind),)+ _ => None }
            }
        }
    };
}

wire_kinds! { ExpressionKind {
    Apply => "DBSPApplyExpression",
    ApplyMethod => "DBSPApplyMethodExpression",
    Array => "DBSPArrayExpression",
    Assignment => "DBSPAssignmentExpression",
    AsymmetricFieldComparator => "DBSPAsymmetricFieldComparatorExpression",
    Binary => "DBSPBinaryExpression",
    BinaryLiteral => "DBSPBinaryLiteral",
    Block => "DBSPBlockExpression",
    BoolLiteral => "DBSPBoolLiteral",
    Borrow => "DBSPBorrowExpression",
    Cast => "DBSPCastExpression",
    Clone => "DBSPCloneExpression",
    Closure => "DBSPClosureExpression",
    ConditionalIncrement => "DBSPConditionalIncrementExpression",
    Constructor => "DBSPConstructorExpression",
    CustomOrd => "DBSPCustomOrdExpression",
    CustomOrdField => "DBSPCustomOrdField",
    DateLiteral => "DBSPDateLiteral",
    DecimalLiteral => "DBSPDecimalLiteral",
    Deref => "DBSPDerefExpression",
    DirectComparator => "DBSPDirectComparatorExpression",
    DoubleLiteral => "DBSPDoubleLiteral",
    EnumValue => "DBSPEnumValue",
    EqualityComparator => "DBSPEqualityComparatorExpression",
    Fail => "DBSPFailExpression",
    FieldComparator => "DBSPFieldComparatorExpression",
    Field => "DBSPFieldExpression",
    Flatmap => "DBSPFlatmap",
    Fold => "DBSPFold",
    For => "DBSPForExpression",
    GeoPointConstructor => "DBSPGeoPointConstructor",
    HandleError => "DBSPHandleErrorExpression",
    I128Literal => "DBSPI128Literal",
    I16Literal => "DBSPI16Literal",
    I32Literal => "DBSPI32Literal",
    I64Literal => "DBSPI64Literal",
    I8Literal => "DBSPI8Literal",
    ISizeLiteral => "DBSPISizeLiteral",
    If => "DBSPIfExpression",
    IndexedZSet => "DBSPIndexedZSetExpression",
    IsNull => "DBSPIsNullExpression",
    KeywordLiteral => "DBSPKeywordLiteral",
    Lazy => "DBSPLazyExpression",
    Let => "DBSPLetExpression",
    LongIntervalLiteral => "DBSPLongIntervalLiteral",
    Map => "DBSPMapExpression",
    MinMax => "DBSPMinMax",
    NoComparator => "DBSPNoComparatorExpression",
    NullLiteral => "DBSPNullLiteral",
    Path => "DBSPPathExpression",
    QualifyType => "DBSPQualifyTypeExpression",
    Question => "DBSPQuestionExpression",
    RawTuple => "DBSPRawTupleExpression",
    RealLiteral => "DBSPRealLiteral",
    Return => "DBSPReturnExpression",
    ShortIntervalLiteral => "DBSPShortIntervalLiteral",
    Some => "DBSPSomeExpression",
    Sort => "DBSPSortExpression",
    Static => "DBSPStaticExpression",
    StrLiteral => "DBSPStrLiteral",
    StringLiteral => "DBSPStringLiteral",
    TimeAddSub => "DBSPTimeAddSub",
    TimeLiteral => "DBSPTimeLiteral",
    TimestampLiteral => "DBSPTimestampLiteral",
    TimestampTzLiteral => "DBSPTimestampTzLiteral",
    Tuple => "DBSPTupleExpression",
    U128Literal => "DBSPU128Literal",
    U16Literal => "DBSPU16Literal",
    U32Literal => "DBSPU32Literal",
    U64Literal => "DBSPU64Literal",
    U8Literal => "DBSPU8Literal",
    USizeLiteral => "DBSPUSizeLiteral",
    Unary => "DBSPUnaryExpression",
    UnsignedUnwrap => "DBSPUnsignedUnwrapExpression",
    UnsignedWrap => "DBSPUnsignedWrapExpression",
    UnwrapCustomOrd => "DBSPUnwrapCustomOrdExpression",
    Unwrap => "DBSPUnwrapExpression",
    UuidLiteral => "DBSPUuidLiteral",
    VariablePath => "DBSPVariablePath",
    Variant => "DBSPVariantExpression",
    VariantNullLiteral => "DBSPVariantNullLiteral",
    VoidLiteral => "DBSPVoidLiteral",
    WindowBound => "DBSPWindowBoundExpression",
    ZSet => "DBSPZSetExpression",
    LinearAggregate => "LinearAggregate",
    MinMaxAggregate => "MinMaxAggregate",
    NoExpression => "NoExpression",
    NonLinearAggregate => "NonLinearAggregate",
}}

impl ExpressionKind {
    /// The DBSP expressions that implement IsNumericLiteral, including interval offsets.
    pub fn is_numeric_literal(self) -> bool {
        matches!(
            self,
            Self::I8Literal
                | Self::I16Literal
                | Self::I32Literal
                | Self::I64Literal
                | Self::I128Literal
                | Self::U8Literal
                | Self::U16Literal
                | Self::U32Literal
                | Self::U64Literal
                | Self::U128Literal
                | Self::ISizeLiteral
                | Self::USizeLiteral
                | Self::DecimalLiteral
                | Self::RealLiteral
                | Self::DoubleLiteral
                | Self::ShortIntervalLiteral
                | Self::LongIntervalLiteral
        )
    }
}

wire_kinds! { TypeKind {
    Comparator => "DBSPComparatorType",
    Any => "DBSPTypeAny",
    Array => "DBSPTypeArray",
    BTreeMap => "DBSPTypeBTreeMap",
    Binary => "DBSPTypeBinary",
    Bool => "DBSPTypeBool",
    Date => "DBSPTypeDate",
    Decimal => "DBSPTypeDecimal",
    Double => "DBSPTypeDouble",
    Function => "DBSPTypeFunction",
    GeoPoint => "DBSPTypeGeoPoint",
    ISize => "DBSPTypeISize",
    IndexedZSet => "DBSPTypeIndexedZSet",
    Integer => "DBSPTypeInteger",
    Keyword => "DBSPTypeKeyword",
    Lazy => "DBSPTypeLazy",
    LongInterval => "DBSPTypeLongInterval",
    Map => "DBSPTypeMap",
    Null => "DBSPTypeNull",
    Option => "DBSPTypeOption",
    RawTuple => "DBSPTypeRawTuple",
    Real => "DBSPTypeReal",
    Ref => "DBSPTypeRef",
    Result => "DBSPTypeResult",
    Semigroup => "DBSPTypeSemigroup",
    ShortInterval => "DBSPTypeShortInterval",
    SqlResult => "DBSPTypeSqlResult",
    Str => "DBSPTypeStr",
    Stream => "DBSPTypeStream",
    String => "DBSPTypeString",
    Struct => "DBSPTypeStruct",
    Time => "DBSPTypeTime",
    Timestamp => "DBSPTypeTimestamp",
    TimestampTz => "DBSPTypeTimestampTz",
    Tuple => "DBSPTypeTuple",
    TypedBox => "DBSPTypeTypedBox",
    USize => "DBSPTypeUSize",
    User => "DBSPTypeUser",
    Uuid => "DBSPTypeUuid",
    Variant => "DBSPTypeVariant",
    Vec => "DBSPTypeVec",
    Void => "DBSPTypeVoid",
    Weight => "DBSPTypeWeight",
    WithCustomOrd => "DBSPTypeWithCustomOrd",
    ZSet => "DBSPTypeZSet",
}}

#[derive(Clone, Debug)]
pub enum InnerKind {
    Expression(ExpressionKind),
    Type(TypeKind),
    Parameter,
    AggregateList,
    Other(String),
}

impl InnerKind {
    pub(crate) fn decode(class: std::borrow::Cow<'_, str>) -> Self {
        if let Some(kind) = ExpressionKind::from_wire(&class) {
            Self::Expression(kind)
        } else if let Some(kind) = TypeKind::from_wire(&class) {
            Self::Type(kind)
        } else if class == "DBSPAggregateList" {
            Self::AggregateList
        } else if class == "DBSPParameter" {
            Self::Parameter
        } else {
            Self::Other(class.into_owned())
        }
    }

    pub fn wire_name(&self) -> &str {
        match self {
            Self::Expression(kind) => kind.wire_name(),
            Self::Type(kind) => kind.wire_name(),
            Self::Parameter => "DBSPParameter",
            Self::AggregateList => "DBSPAggregateList",
            Self::Other(name) => name,
        }
    }
}
