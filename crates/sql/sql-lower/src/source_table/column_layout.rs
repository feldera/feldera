//! A SQL row has a named-field struct in metadata and a positional tuple in the data stream.
//! Collection constructors carry the chosen representation through to their elements.
use super::{name, range, Error, Ids};
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::{
    catalog::Name,
    column_type::ColumnType,
    types::{FloatFlavor, Precision, SqlType, TimeZone, TypeKind},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{borrow::Cow, fmt::Write};

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Repr {
    Tuple,
    Struct,
}

enum Children {
    None,
    Collection(Vec<ColumnLayout>),
    Row {
        fields: Vec<(Name, ColumnLayout)>,
        hash: String,
    },
}

pub(crate) struct ColumnLayout {
    class: CircuitType,
    code: String,
    pub(super) short: String,
    properties: Value,
    children: Children,
    nullable: bool,
}

impl ColumnLayout {
    pub(crate) fn lower(ty: &ColumnType<'_>) -> Result<Self, Error> {
        let mut budget = feldera_sql_rel_model::ValidationLimits::default().references;
        Self::lower_field(ty, 1, &mut budget)
    }

    fn lower_field(ty: &ColumnType<'_>, copies: usize, budget: &mut usize) -> Result<Self, Error> {
        *budget = budget
            .checked_sub(copies)
            .ok_or_else(|| Error("expanded DBSP row exceeds the type reference limit".into()))?;
        let (class, code, name, arguments, nullable) = match ty {
            ColumnType::Row { nullable, fields } => {
                let mut signature = String::from("[");
                let mut columns = Vec::with_capacity(fields.len());
                for (name, ty) in fields {
                    // A stream tuple retains both positional types and originalStruct field types.
                    let layout = Self::lower_field(ty, copies.saturating_mul(2), budget)?;
                    write!(signature, "{name}:{},", layout.short).expect("string write");
                    columns.push((
                        Name {
                            name: (*name).into(),
                            quoted: *name != name.to_lowercase(),
                        },
                        layout,
                    ));
                }
                signature.push(']');
                let hash = struct_hash("", &signature);
                return Ok(Self {
                    class: CircuitType::Struct,
                    code: "STRUCT".into(),
                    short: struct_display("", &hash, *nullable),
                    properties: json!({}),
                    children: Children::Row {
                        fields: columns,
                        hash,
                    },
                    nullable: *nullable,
                });
            }
            ColumnType::Scalar(ty) => return Self::scalar(ty),
            ColumnType::Array { nullable, element } => (
                CircuitType::Array,
                "ARRAY",
                "Array",
                vec![Self::lower_field(element, copies, budget)?],
                *nullable,
            ),
            ColumnType::Map {
                nullable,
                key,
                value,
            } => (
                CircuitType::Map,
                "MAP",
                "Map",
                vec![
                    Self::lower_field(key, copies, budget)?,
                    Self::lower_field(value, copies, budget)?,
                ],
                *nullable,
            ),
        };
        let parameters = arguments
            .iter()
            .map(|arg| arg.type_argument(Repr::Struct))
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if nullable { "?" } else { "" };
        Ok(Self {
            class,
            code: code.into(),
            short: format!("{name}<{parameters}>{suffix}"),
            properties: json!({"name":name}),
            children: Children::Collection(arguments),
            nullable,
        })
    }

    pub(crate) fn scalar(ty: &SqlType) -> Result<Self, Error> {
        let (class, code, mut short, properties) = match ty.kind {
            TypeKind::Integer { width, signed } => {
                let bits = width.bits();
                let prefix = if signed { "" } else { "U" };
                (
                    CircuitType::Integer,
                    format!("{prefix}INT{bits}"),
                    format!("{}{bits}", if signed { "i" } else { "u" }),
                    json!({"width":bits,"signed":signed}),
                )
            }
            TypeKind::Null {} if ty.nullable => {
                (CircuitType::Null, "NULL".into(), "null".into(), json!({}))
            }
            TypeKind::Uuid {} => (CircuitType::Uuid, "UUID".into(), "Uuid".into(), json!({})),
            TypeKind::Variant {} => (
                CircuitType::Variant,
                "VARIANT".into(),
                "VARIANT".into(),
                json!({}),
            ),
            TypeKind::Date {} => (CircuitType::Date, "DATE".into(), "Date".into(), json!({})),
            TypeKind::Time {
                precision: Precision::Specified(_),
                zone: TimeZone::None,
            } => (CircuitType::Time, "TIME".into(), "Time".into(), json!({})),
            TypeKind::Boolean {} => (CircuitType::Bool, "BOOL".into(), "b".into(), json!({})),
            TypeKind::Decimal {
                precision, scale, ..
            } => (
                CircuitType::Decimal,
                "DECIMAL".into(),
                format!("DECIMAL({precision}, {scale})"),
                json!({"precision":precision, "scale":scale}),
            ),
            TypeKind::Timestamp {
                precision: Precision::Specified(_),
                zone: TimeZone::None,
            } => (
                CircuitType::Timestamp,
                "TIMESTAMP".into(),
                "Timestamp".into(),
                json!({}),
            ),
            TypeKind::Float {
                flavor: FloatFlavor::Double,
            } => (CircuitType::Double, "DOUBLE".into(), "d".into(), json!({})),
            TypeKind::Float {
                flavor: FloatFlavor::Real,
            } => (CircuitType::Real, "REAL".into(), "f".into(), json!({})),
            TypeKind::Binary { fixed, length } => {
                let precision = match length {
                    Precision::Unspecified => -1,
                    Precision::Specified(n) => i64::from(n),
                };
                let short = if precision < 0 {
                    "bytes".into()
                } else {
                    format!("bytes({precision})")
                };
                (
                    CircuitType::Binary,
                    "BYTES".into(),
                    short,
                    json!({"precision":precision,"fixed":fixed}),
                )
            }
            TypeKind::Character { fixed, length, .. } => {
                let precision = match length {
                    Precision::Unspecified => -1,
                    Precision::Specified(n) => i64::from(n),
                };
                let short = if precision < 0 {
                    "s".into()
                } else {
                    format!("s({precision},{fixed})")
                };
                (
                    CircuitType::String,
                    "STRING".into(),
                    short,
                    json!({"fixed":fixed,"precision":precision}),
                )
            }
            _ => {
                return Err(Error(format!(
                    "unsupported table column type {:?}",
                    ty.kind
                )));
            }
        };
        if ty.nullable && class != CircuitType::Null {
            short.push('?');
        }
        Ok(Self {
            class,
            code,
            short,
            properties,
            children: Children::None,
            nullable: ty.nullable,
        })
    }

    // Collection constructors inherit the source of the element or map key.
    // Floating-point and NULL singletons have no source.
    fn source_empty(&self) -> bool {
        match &self.children {
            Children::Collection(args) => args[0].source_empty(),
            _ => matches!(
                self.class,
                CircuitType::Double | CircuitType::Real | CircuitType::Null
            ),
        }
    }

    // DBSPTypeNull prints "null" as a field and "null?" inside a generic type.
    fn type_argument(&self, repr: Repr) -> Cow<'_, str> {
        if self.class == CircuitType::Null {
            Cow::Borrowed("null?")
        } else {
            self.display(repr)
        }
    }

    fn display(&self, repr: Repr) -> Cow<'_, str> {
        if repr == Repr::Struct {
            return Cow::Borrowed(&self.short);
        }
        let suffix = if self.nullable { "?" } else { "" };
        match &self.children {
            Children::None => Cow::Borrowed(&self.short),
            Children::Collection(args) => {
                let args = args
                    .iter()
                    .map(|arg| arg.type_argument(repr))
                    .collect::<Vec<_>>()
                    .join(", ");
                let name = if self.class == CircuitType::Array {
                    "Array"
                } else {
                    "Map"
                };
                Cow::Owned(format!("{name}<{args}>{suffix}"))
            }
            Children::Row { fields, .. } => {
                let args = fields
                    .iter()
                    .map(|(_, ty)| ty.display(repr))
                    .collect::<Vec<_>>()
                    .join(", ");
                Cow::Owned(format!("Tup{}{suffix}<{args}>", fields.len()))
            }
        }
    }

    pub(crate) fn node(&self, ids: &mut Ids) -> Value {
        self.emit(ids, Repr::Tuple)
    }

    pub(super) fn emit(&self, ids: &mut Ids, repr: Repr) -> Value {
        let arguments: Vec<_> = match &self.children {
            Children::None => Vec::new(),
            Children::Collection(args) => args.iter().map(|child| child.emit(ids, repr)).collect(),
            Children::Row { fields, hash } => {
                let position = range(0, 0, 0, 0);
                // The wire reader sees the tuple's fields before originalStruct.
                let tuple_fields = (repr == Repr::Tuple).then(|| {
                    fields
                        .iter()
                        .map(|(_, ty)| ty.node(ids))
                        .collect::<Vec<_>>()
                });
                let members = fields
                    .iter()
                    .enumerate()
                    .map(|(index, (name, layout))| {
                        let ty = layout.emit(ids, repr);
                        field(ids, &position, name, index as u32, ty)
                    })
                    .collect();
                let hash = if repr == Repr::Struct {
                    hash.clone()
                } else {
                    field_hash(
                        "",
                        fields.iter().map(|(name, ty)| (name.name.as_str(), ty)),
                        repr,
                    )
                };
                let row = structure(
                    ids,
                    &position,
                    &Name {
                        name: String::new(),
                        quoted: false,
                    },
                    members,
                    hash,
                    self.nullable,
                );
                return match tuple_fields {
                    None => row,
                    Some(fields) => {
                        let mut tuple = ids.node("DBSPTypeTuple", &position);
                        tuple["tupFields"] = fields.into();
                        tuple["originalStruct"] = row;
                        tuple["code"] = "TUPLE".into();
                        tuple["mayBeNull"] = self.nullable.into();
                        tuple
                    }
                };
            }
        };
        if matches!(
            self.class,
            CircuitType::Double | CircuitType::Real | CircuitType::Null
        ) {
            return ids.singleton(self.class, &self.code, self.nullable);
        }
        let mut node = self.type_node(ids);
        if !arguments.is_empty() {
            node["typeArgs"] = arguments.into();
        }
        node
    }

    pub(crate) fn type_node(&self, ids: &mut Ids) -> Value {
        let mut node = if self.source_empty() {
            ids.generated(self.class.wire_name())
        } else {
            ids.node(self.class.wire_name(), &range(0, 0, 0, 0))
        };
        node.as_object_mut()
            .unwrap()
            .extend(self.properties.as_object().unwrap().clone());
        node["code"] = self.code.clone().into();
        node["mayBeNull"] = self.nullable.into();
        node
    }
}

pub(super) fn struct_display(name: &str, hash: &str, nullable: bool) -> String {
    format!("struct {} {name} {hash}", if nullable { "?" } else { "" })
}

pub(super) fn field_hash<'a>(
    name: &str,
    fields: impl Iterator<Item = (&'a str, &'a ColumnLayout)>,
    repr: Repr,
) -> String {
    let mut signature = String::from("[");
    for (name, ty) in fields {
        write!(signature, "{name}:{},", ty.display(repr)).expect("string write");
    }
    signature.push(']');
    struct_hash(name, &signature)
}

pub(super) fn struct_hash(name: &str, signature: &str) -> String {
    let digest = Sha256::digest(format!("{name}{signature}"));
    format!("struct_{}", hex::encode(&digest[..8]))
}

pub(super) fn field(
    ids: &mut Ids,
    position: &Value,
    identifier: &Name,
    index: u32,
    ty: Value,
) -> Value {
    let mut field = ids.node("Field", position);
    field["type"] = ty;
    field["index"] = index.into();
    field["name"] = name(identifier);
    field
}

pub(super) fn structure(
    ids: &mut Ids,
    position: &Value,
    identifier: &Name,
    fields: Vec<Value>,
    hash: String,
    nullable: bool,
) -> Value {
    let mut node = ids.node("DBSPTypeStruct", position);
    node["fields"] = fields.into();
    node["hashName"] = hash.into();
    node["name"] = name(identifier);
    node["code"] = "STRUCT".into();
    node["mayBeNull"] = nullable.into();
    node
}
