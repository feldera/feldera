//! Lower SQL table rows to the DBSP named struct and its positional tuple.
//! The compiler keeps separate type objects for declarations, streams and
//! column metadata. Those identities remain distinct at the replay boundary.

use super::Error;
use feldera_sql_ir::build::CollectionType;
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::{
    catalog::{CatalogSchema, Name},
    column_type::ColumnType,
    operators::DiagnosticSource,
    source::Origin,
    statements::StatementSource,
    types::{FloatFlavor, IntegerWidth, Precision, SqlType, TimeZone, TypeKind, TypeTable},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::fmt::Write;

pub(super) struct Ids {
    inner: u64,
    operator: u64,
    relations: Vec<usize>,
    origin: u64,
    doubles: [Option<u64>; 2],
    reals: [Option<u64>; 2],
    null: Option<u64>,
    bools: [Option<u64>; 2],
}

impl Ids {
    pub(super) fn after(graph: &Value, program: &feldera_sql_rel_model::program::Program) -> Self {
        fn next(value: &Value) -> u64 {
            match value {
                Value::Object(obj) => obj
                    .get("id")
                    .and_then(Value::as_u64)
                    .map_or(0, |n| n + 1)
                    .max(obj.values().map(next).max().unwrap_or(0)),
                Value::Array(values) => values.iter().map(next).max().unwrap_or(0),
                _ => 0,
            }
        }
        let ops = graph["allOperators"].as_array().expect("system operators");
        Self {
            inner: next(graph),
            relations: relation_ids(program),
            doubles: [None; 2],
            reals: [None; 2],
            null: None,
            bools: [None; 2],
            operator: ops
                .iter()
                .filter_map(|n| n["id"].as_u64())
                .max()
                .unwrap_or(0)
                + 1,
            origin: ops
                .iter()
                .map(|n| next(&n["provenance"]))
                .max()
                .unwrap_or(0),
        }
    }

    pub(super) fn relation(&self, id: u32) -> usize {
        self.relations[id as usize]
    }

    pub(super) fn next_operator(&mut self) -> u64 {
        let id = self.operator;
        self.operator += 1;
        id
    }

    pub(super) fn next_origin(&mut self) -> u64 {
        let id = self.origin;
        self.origin += 1;
        id
    }

    pub(super) fn boolean(&mut self, nullable: bool) -> Value {
        if let Some(id) = self.bools[usize::from(nullable)] {
            return json!({"node":id});
        }
        self.bools[usize::from(nullable)] = Some(self.inner);
        let mut ty = self.generated("DBSPTypeBool");
        ty["singleton"] = true.into();
        ty["code"] = "BOOL".into();
        ty["mayBeNull"] = nullable.into();
        ty
    }

    fn next_inner(&mut self) -> u64 {
        let id = self.inner;
        self.inner += 1;
        id
    }

    pub(super) fn generated(&mut self, class: &str) -> Value {
        let id = self.next_inner();
        json!({"class":class, "id":id, "sourceEmpty":true})
    }

    pub(super) fn node(&mut self, class: &str, position: &Value) -> Value {
        let id = self.next_inner();
        json!({"class":class, "id":id, "sourceEmpty":false, "position":position})
    }
}

/// The S1 consumer allocates scans in their recorded order, then all other
/// relations in dependency order. An INSERT's VALUES consumes an ID even
/// when it produces no operator in a pipeline.
fn relation_ids(program: &feldera_sql_rel_model::program::Program) -> Vec<usize> {
    let producers = &program.producers.relations;
    let mut ids = vec![usize::MAX; producers.len()];
    for (physical, &scan) in program.table_scan_order.iter().enumerate() {
        ids[scan as usize] = physical;
    }
    let mut next = program.table_scan_order.len();
    let mut stack = Vec::new();
    for root in 0..ids.len() {
        stack.push((root, false));
        while let Some((id, children_done)) = stack.pop() {
            if ids[id] != usize::MAX {
                continue;
            }
            if children_done {
                ids[id] = next;
                next += 1;
            } else {
                stack.push((id, true));
                stack.extend(
                    producers[id]
                        .inputs
                        .iter()
                        .rev()
                        .map(|&i| (i as usize, false)),
                );
            }
        }
    }
    ids
}

pub(super) fn range(line: u32, column: u32, end_line: u32, end_column: u32) -> Value {
    json!({"start_line_number":line, "start_column":column, "end_line_number":end_line, "end_column":end_column})
}

pub(super) fn location(origin: &Origin) -> Value {
    let [line, column, end_line, end_column] = origin.coordinates();
    range(line, column, end_line, end_column)
}

pub(super) fn name(identifier: &Name) -> Value {
    json!({"name":identifier.name, "isQuoted":identifier.quoted})
}

struct ColumnLayout {
    class: CircuitType,
    code: String,
    short: String,
    properties: Value,
    arguments: Vec<Self>,
    nullable: bool,
}

impl ColumnLayout {
    fn lower(ty: &ColumnType<'_>) -> Result<Self, Error> {
        let (class, code, name, arguments, nullable) = match ty {
            ColumnType::Scalar(ty) => return Self::scalar(ty),
            ColumnType::Array { nullable, element } => (
                CircuitType::Array,
                "ARRAY",
                "Array",
                vec![Self::lower(element)?],
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
                vec![Self::lower(key)?, Self::lower(value)?],
                *nullable,
            ),
        };
        let parameters = arguments
            .iter()
            .map(|arg| arg.short.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if nullable { "?" } else { "" };
        Ok(Self {
            class,
            code: code.into(),
            short: format!("{name}<{parameters}>{suffix}"),
            properties: json!({"name":name}),
            arguments,
            nullable,
        })
    }

    fn scalar(ty: &SqlType) -> Result<Self, Error> {
        let (class, code, mut short, properties) = match ty.kind {
            TypeKind::Integer { width, signed } => {
                let bits = match width {
                    IntegerWidth::I8 => 8,
                    IntegerWidth::I16 => 16,
                    IntegerWidth::I32 => 32,
                    IntegerWidth::I64 => 64,
                };
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
            arguments: Vec::new(),
            nullable: ty.nullable,
        })
    }

    // Collection constructors inherit the source of the element or map key.
    // Floating-point and NULL singletons have no source.
    fn source_empty(&self) -> bool {
        self.arguments.first().map_or_else(
            || {
                matches!(
                    self.class,
                    CircuitType::Double | CircuitType::Real | CircuitType::Null
                )
            },
            Self::source_empty,
        )
    }

    fn node(&self, ids: &mut Ids) -> Value {
        let arguments: Vec<_> = self.arguments.iter().map(|arg| arg.node(ids)).collect();
        let slot = match self.class {
            CircuitType::Double => Some(&mut ids.doubles[usize::from(self.nullable)]),
            CircuitType::Real => Some(&mut ids.reals[usize::from(self.nullable)]),
            CircuitType::Null => Some(&mut ids.null),
            _ => None,
        };
        let mut node = if let Some(slot) = slot {
            if let Some(id) = slot {
                return json!({"node":id});
            }
            let id = ids.inner;
            ids.inner += 1;
            *slot = Some(id);
            json!({"class":self.class.wire_name(),"id":id,"sourceEmpty":true,"singleton":true})
        } else if self.source_empty() {
            ids.generated(self.class.wire_name())
        } else {
            ids.node(self.class.wire_name(), &range(0, 0, 0, 0))
        };
        node.as_object_mut()
            .unwrap()
            .extend(self.properties.as_object().unwrap().clone());
        if !arguments.is_empty() {
            node["typeArgs"] = arguments.into();
        }
        node["code"] = self.code.clone().into();
        node["mayBeNull"] = self.nullable.into();
        node
    }
}

/// DBSP retains the SQL field names as well as the positional tuple layout.
pub(super) struct RowLayout<'a> {
    schema: &'a CatalogSchema,
    columns: Vec<(ColumnLayout, Value)>,
    signature: String,
}

impl<'a> RowLayout<'a> {
    pub(super) fn new(schema: &'a CatalogSchema, types: &TypeTable) -> Result<Self, Error> {
        let mut signature = String::from("[");
        let mut columns = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            let ty = types
                .column_type(col.ty)
                .ok_or_else(|| Error("unsupported column type".into()))?;
            let descriptor = ty
                .descriptor()
                .ok_or_else(|| Error("unsupported column schema".into()))?;
            let layout = ColumnLayout::lower(&ty)?;
            write!(signature, "{}:{},", col.name.name, layout.short).expect("string write");
            columns.push((layout, descriptor));
        }
        signature.push(']');
        Ok(Self {
            schema,
            columns,
            signature,
        })
    }

    pub(super) fn structure(&self, ids: &mut Ids, position: &Value) -> Value {
        self.struct_type(ids, position, false)
    }

    fn struct_type(&self, ids: &mut Ids, position: &Value, anonymous: bool) -> Value {
        let row_name = if anonymous {
            Name {
                name: String::new(),
                quoted: false,
            }
        } else {
            self.schema.name.clone()
        };
        let digest = Sha256::digest(format!("{}{}", row_name.name, self.signature));
        let fields: Vec<_> = self
            .schema
            .columns
            .iter()
            .zip(&self.columns)
            .map(|(col, (layout, _))| {
                let field_position = if anonymous {
                    position.clone()
                } else {
                    location(&col.diagnostic.origin)
                };
                let mut field = ids.node("Field", &field_position);
                field["type"] = layout.node(ids);
                field["index"] = col.index.into();
                field["name"] = name(&col.name);
                field
            })
            .collect();
        let mut node = ids.node("DBSPTypeStruct", position);
        node["fields"] = fields.into();
        node["hashName"] = format!("struct_{}", hex::encode(&digest[..8])).into();
        node["name"] = name(&row_name);
        node["code"] = "STRUCT".into();
        node["mayBeNull"] = false.into();
        node
    }

    pub(super) fn tuple(&self, ids: &mut Ids, position: &Value, anonymous: bool) -> Value {
        // The Java decoder reads tuple fields before originalStruct and resolves
        // singleton references immediately. Define shared scalar types there first.
        let fields = self.column_types(ids);
        let structure = self.struct_type(ids, position, anonymous);
        let mut tuple = ids.node("DBSPTypeTuple", position);
        tuple["tupFields"] = fields.into();
        tuple["originalStruct"] = structure;
        tuple["code"] = "TUPLE".into();
        tuple["mayBeNull"] = false.into();
        tuple
    }

    pub(super) fn column_types(&self, ids: &mut Ids) -> Vec<Value> {
        self.columns
            .iter()
            .map(|(layout, _)| layout.node(ids))
            .collect()
    }

    pub(super) fn descriptors(&self) -> impl Iterator<Item = &Value> {
        self.columns.iter().map(|(_, descriptor)| descriptor)
    }
}

pub(super) fn stream_type(ids: &mut Ids, element: Value, position: &Value) -> Result<Value, Error> {
    let element = element
        .try_into()
        .map_err(|e: feldera_sql_ir::Error| Error(e.to_string()))?;
    Ok(CollectionType::ZSet { element }.into_wire(ids.next_inner(), Some(position)))
}

pub(super) fn statement_position(source: &StatementSource) -> Value {
    let position = &source.position;
    range(
        position.line,
        position.column,
        position.end_line,
        position.end_column,
    )
}

/// The public schema retains the requested precision. DBSP represents all
/// TIME and TIMESTAMP values at the runtime's fixed precision.
pub(super) fn precision_warnings(
    column_types: impl IntoIterator<Item = feldera_sql_rel_model::TypeId>,
    types: &TypeTable,
    position: &Value,
    context: &Value,
    seen: &mut std::collections::HashSet<(&'static str, u32)>,
    messages: &mut Vec<Value>,
) {
    let mut pending: Vec<_> = column_types.into_iter().collect();
    pending.reverse();
    let mut visited = std::collections::HashSet::new();
    while let Some(id) = pending.pop() {
        if !visited.insert(id) {
            continue;
        }
        let (name, requested, runtime) = match types.types[id.0 as usize].kind {
            TypeKind::Row { ref fields, .. } => {
                pending.extend(fields.iter().rev().map(|field| field.ty));
                continue;
            }
            TypeKind::Array { element, .. } => {
                pending.push(element);
                continue;
            }
            TypeKind::Map { key, value } => {
                pending.extend([value, key]);
                continue;
            }
            TypeKind::Time {
                precision: Precision::Specified(n),
                zone: TimeZone::None,
            } => ("TIME", n, 9),
            TypeKind::Timestamp {
                precision: Precision::Specified(n),
                zone: TimeZone::None,
            } => ("TIMESTAMP", n, 6),
            _ => continue,
        };
        if requested != runtime && seen.insert((name, requested)) {
            messages.push(json!({
                "context":context, "range":position, "warning":true, "continuation":false,
                "type":format!("{name} precision ignored"),
                "message":format!("{name} precision is always {name}({runtime}); specified precision {requested} is ignored")
            }));
        }
    }
}

pub(super) fn lower(
    schema: &CatalogSchema,
    types: &TypeTable,
    source: &StatementSource,
    identifier: &DiagnosticSource,
    ids: &mut Ids,
) -> Result<(Value, Value), Error> {
    let position = statement_position(source);
    let table_name = name(&schema.name);
    let layout = RowLayout::new(schema, types)?;
    let original = layout.structure(ids, &position);
    let tuple = layout.tuple(ids, &position, false);
    let output = stream_type(ids, tuple, &position)?;
    let mut fields = Vec::new();
    let mut columns = Vec::new();
    for (col, (ty, descriptor)) in schema.columns.iter().zip(&layout.columns) {
        fields.push(json!({
            "name": col.name.name,
            "case_sensitive": col.name.quoted,
            "columntype": descriptor,
            "unused": false,
        }));
        columns.push(json!({
            "name": name(&col.name),
            "position": location(&col.diagnostic.origin),
            "type": ty.node(ids),
            "isPrimaryKey": col.primary_key,
            "interned": false,
        }));
    }
    let mut metadata = json!({
        "name": schema.name.name, "case_sensitive": schema.name.quoted,
        "fields": fields, "materialized": false, "foreign_keys": [],
    });
    let key = schema.primary_key();
    if !key.is_empty() {
        metadata["primary_key"] = json!(key);
    }
    let operator = json!({
        "class": "DBSPSourceMultisetOperator", "id": ids.operator,
        "provenance": {
            "id": ids.origin, "kind": "RelAnd", "position": range(0, 0, 0, 0),
            "positions": [], "children": [],
        },
        "annotations": [], "inputs": [],
        "originalRowType": original, "outputType": output,
        "tableName": table_name, "sourceNamePosition": location(&identifier.origin),
        "sourceNameEmpty": identifier.empty,
        "metadata": {
            "tableName": table_name, "materialized": false, "changes": "Unrestricted",
            "foreignKeys": [], "columnMetadata": columns,
        },
    });
    ids.operator += 1;
    ids.origin += 1;
    Ok((operator, metadata))
}
