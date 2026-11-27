//! Ordered type arenas preserve sharing and named-type identity. Equal types
//! may occupy different slots; decoding must not intern or reorder them.

use crate::source::{Identifier, Origin, Source};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct TypeId(pub u32);

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum TypeSchemaVersion {
    #[serde(rename = "types-v0")]
    V0,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TypeTable {
    pub schema_version: TypeSchemaVersion,
    pub sources: Vec<Source>,
    pub types: Vec<SqlType>,
    pub roots: Vec<TypeId>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqlType {
    pub nullable: bool,
    pub origin: Origin,
    pub kind: TypeKind,
}

impl SqlType {
    /// Scalar column descriptor for the public schema. Keep the key order
    /// used by the Java writer because classic workspaces embed the schema.
    pub fn scalar_column_type(&self) -> Option<serde_json::Value> {
        use serde_json::json;
        let mut result = json!({"nullable":self.nullable});
        let name = match self.kind {
            TypeKind::Null {} if self.nullable => "NULL",
            TypeKind::Boolean {} => "BOOLEAN",
            TypeKind::Integer { width, signed } => match (width, signed) {
                (IntegerWidth::I8, true) => "TINYINT",
                (IntegerWidth::I8, false) => "UTINYINT",
                (IntegerWidth::I16, true) => "SMALLINT",
                (IntegerWidth::I16, false) => "USMALLINT",
                (IntegerWidth::I32, true) => "INTEGER",
                (IntegerWidth::I32, false) => "UINTEGER",
                (IntegerWidth::I64, true) => "BIGINT",
                (IntegerWidth::I64, false) => "UBIGINT",
            },
            TypeKind::Float {
                flavor: FloatFlavor::Real,
            } => "REAL",
            TypeKind::Float {
                flavor: FloatFlavor::Double,
            } => "DOUBLE",
            TypeKind::Decimal {
                precision, scale, ..
            } => {
                result["precision"] = precision.into();
                result["scale"] = scale.into();
                "DECIMAL"
            }
            TypeKind::Character { fixed, length, .. } | TypeKind::Binary { fixed, length } => {
                result["precision"] = match length {
                    Precision::Unspecified => (-1).into(),
                    Precision::Specified(n) => n.into(),
                };
                match (&self.kind, fixed) {
                    (TypeKind::Character { .. }, true) => "CHAR",
                    (TypeKind::Character { .. }, false) => "VARCHAR",
                    (_, true) => "BINARY",
                    (_, false) => "VARBINARY",
                }
            }
            TypeKind::Date {} => "DATE",
            TypeKind::Time {
                precision: Precision::Specified(n),
                zone: TimeZone::None,
            } => {
                result["precision"] = n.into();
                "TIME"
            }
            TypeKind::Timestamp {
                precision: Precision::Specified(n),
                zone: TimeZone::None,
            } => {
                result["precision"] = n.into();
                "TIMESTAMP"
            }
            _ => return None,
        };
        result["type"] = name.into();
        Some(result)
    }
}

/// An omitted precision differs from an explicitly specified precision. This
/// tagged representation also makes an accidental missing field an error.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Precision {
    Unspecified,
    Specified(#[schemars(range(max = 2147483647))] u32),
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TypeKind {
    Boolean {},
    Integer {
        width: IntegerWidth,
        signed: bool,
    },
    Decimal {
        #[schemars(range(max = 2147483647))]
        precision: u32,
        #[schemars(range(max = 2147483647))]
        scale: u32,
        /// Distinguish DECIMAL(p) from DECIMAL(p, 0) while retaining resolved scale.
        explicit_scale: bool,
    },
    /// FLOAT and DOUBLE lower alike but remain distinct SQL type names.
    Float {
        flavor: FloatFlavor,
    },
    Character {
        fixed: bool,
        length: Precision,
        metadata: CharacterMetadata,
    },
    Binary {
        fixed: bool,
        length: Precision,
    },
    Null {},
    Array {
        element: TypeId,
        cardinality: Cardinality,
    },
    Multiset {
        element: TypeId,
        cardinality: Cardinality,
    },
    Map {
        key: TypeId,
        value: TypeId,
    },
    Row {
        fields: Vec<Field>,
        struct_kind: StructKind,
    },
    Named {
        name: Vec<Identifier>,
        representation: TypeId,
    },
    Interval {
        start: IntervalUnit,
        end: IntervalEnd,
        leading_precision: Precision,
        fractional_precision: Precision,
    },
    Date {},
    Time {
        precision: Precision,
        zone: TimeZone,
    },
    Timestamp {
        precision: Precision,
        zone: TimeZone,
    },
    Geometry {
        representation: GeometryRepresentation,
    },
    Uuid {},
    Variant {},
    /// These planner forms are represented explicitly, but are not a promise
    /// that arbitrary values of them can pass the lowering verifier.
    Symbol {},
    Any {},
    Unknown {},
    /// Planner descriptors, not ordinary runtime scalar values.
    ColumnList {},
    Cursor {},
    Function {
        parameters: Vec<TypeId>,
        result: TypeId,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GeometryRepresentation {
    Sql,
    /// A planner host value. The adapter permits only its known geometry class.
    Host,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntegerWidth {
    I8,
    I16,
    I32,
    I64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FloatFlavor {
    Real,
    Float,
    Double,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum Cardinality {
    Unbounded,
    Bounded(u64),
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StructKind {
    FullyQualified,
    PeekFields,
    PeekFieldsDefault,
    PeekFieldsNoExpand,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Field {
    pub name: Identifier,
    pub ty: TypeId,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum CharacterMetadata {
    Absent {},
    Present {
        charset: String,
        collation: String,
        coercibility: Coercibility,
        identity: CollationIdentity,
    },
}

/// Calcite omits built-in singleton collations from type digests, but includes
/// separately constructed named collations with the same name and coercibility.
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CollationIdentity {
    BuiltinImplicit,
    BuiltinCoercible,
    Named,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Coercibility {
    Explicit,
    Implicit,
    Coercible,
    None,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeZone {
    None,
    Local,
    WithTimeZone,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum IntervalUnit {
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum IntervalEnd {
    Single,
    Through(IntervalUnit),
}

impl Precision {
    pub(crate) fn contains_fraction(self, nanoseconds: u64) -> bool {
        match self {
            Self::Specified(digits) if digits < 9 => {
                nanoseconds.is_multiple_of(10_u64.pow(9 - digits))
            }
            _ => true,
        }
    }

    pub(crate) fn contains_length(self, length: usize) -> bool {
        match self {
            Self::Unspecified => true,
            Self::Specified(maximum) => length <= maximum as usize,
        }
    }
}

impl CharacterMetadata {
    /// Default charset and collation of Calcite character columns and VALUES literals.
    pub fn implicit() -> Self {
        Self::Present {
            charset: "UTF-8".into(),
            collation: "ISO-8859-1$en_US$primary".into(),
            coercibility: Coercibility::Implicit,
            identity: CollationIdentity::BuiltinImplicit,
        }
    }
}

impl TypeKind {
    pub(crate) fn references(&self) -> Vec<TypeId> {
        match self {
            Self::Array { element, .. } | Self::Multiset { element, .. } => vec![*element],
            Self::Map { key, value } => vec![*key, *value],
            Self::Row { fields, .. } => fields.iter().map(|field| field.ty).collect(),
            Self::Named { representation, .. } => vec![*representation],
            Self::Function { parameters, result } => {
                parameters.iter().copied().chain([*result]).collect()
            }
            _ => Vec::new(),
        }
    }
}
