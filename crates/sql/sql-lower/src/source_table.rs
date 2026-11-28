//! Lower SQL table rows to the DBSP named struct and its positional tuple.
//! The compiler keeps separate type objects for declarations, streams and
//! column metadata. Those identities remain distinct at the replay boundary.

mod column_layout;
mod type_declaration;

use super::Error;
pub(super) use column_layout::ColumnLayout;
use column_layout::{field, field_hash, structure, Repr};
use feldera_sql_ir::{build::CollectionType, TypeKind as CircuitType};
use feldera_sql_rel_model::{
    catalog::{CatalogSchema, Name},
    operators::DiagnosticSource,
    source::Origin,
    statements::StatementSource,
    types::{Precision, TimeZone, TypeKind, TypeTable},
};
use serde_json::{json, Value};
pub(super) use type_declaration::declare_record;

pub(super) struct Ids {
    inner: u64,
    operator: u64,
    relations: Vec<usize>,
    origin: u64,
    plan: std::collections::HashMap<u32, usize>,
    singletons: std::collections::HashMap<(CircuitType, bool), u64>,
}

impl Ids {
    pub(super) fn after(
        graph: &Value,
        program: &feldera_sql_rel_model::program::Program,
        plan: std::collections::HashMap<u32, usize>,
    ) -> Self {
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
        // Seed type identities used by the system closures, including ANY
        // used by function paths in user projections.
        let singletons = ops
            .iter()
            .filter_map(|op| {
                let ty = &op["function"]["type"]["resultType"];
                if ty["singleton"] != true {
                    return None;
                }
                let kind = CircuitType::from_wire(ty["class"].as_str()?)?;
                Some(((kind, ty["mayBeNull"].as_bool()?), ty["id"].as_u64()?))
            })
            .collect();
        Self {
            inner: next(graph),
            plan,
            relations: relation_ids(program),
            singletons,
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

    pub(super) fn plan(&self, relation: u32) -> Result<u64, Error> {
        self.plan
            .get(&relation)
            .map(|&id| id as u64)
            .ok_or_else(|| Error(format!("missing profiler plan for relation {relation}")))
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

    pub(super) fn singleton(&mut self, kind: CircuitType, code: &str, nullable: bool) -> Value {
        if let Some(id) = self.singletons.get(&(kind, nullable)) {
            return json!({"node":id});
        }
        self.singletons.insert((kind, nullable), self.inner);
        let mut ty = self.generated(kind.wire_name());
        if kind == CircuitType::String {
            ty["fixed"] = false.into();
            ty["precision"] = (-1).into();
        }
        ty["singleton"] = true.into();
        ty["code"] = code.into();
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

pub(super) fn expression_range(
    position: &feldera_sql_rel_model::expression_objects::ObjectPosition,
) -> Value {
    use feldera_sql_rel_model::expression_objects::ObjectPosition;
    match position {
        ObjectPosition::Present { position, .. } => range(
            position.line,
            position.column,
            position.end_line,
            position.end_column,
        ),
        ObjectPosition::Absent {} => range(0, 0, 0, 0),
    }
}

pub(super) fn location(origin: &Origin) -> Value {
    let [line, column, end_line, end_column] = origin.coordinates();
    range(line, column, end_line, end_column)
}

pub(super) fn name(identifier: &Name) -> Value {
    json!({"name":identifier.name, "isQuoted":identifier.quoted})
}

/// DBSP retains the SQL field names as well as the positional tuple layout.
pub(super) struct RowLayout<'a> {
    schema: &'a CatalogSchema,
    columns: Vec<(ColumnLayout, Value)>,
}

impl<'a> RowLayout<'a> {
    pub(super) fn new(schema: &'a CatalogSchema, types: &TypeTable) -> Result<Self, Error> {
        let mut columns = Vec::with_capacity(schema.columns.len());
        for col in &schema.columns {
            let ty = types
                .column_type(col.ty)
                .ok_or_else(|| Error("unsupported column type".into()))?;
            let descriptor = ty
                .descriptor()
                .ok_or_else(|| Error("unsupported column schema".into()))?;
            let layout = ColumnLayout::lower(&ty)?;
            columns.push((layout, descriptor));
        }
        Ok(Self { schema, columns })
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
        let repr = if anonymous { Repr::Tuple } else { Repr::Struct };
        let hash = field_hash(
            &row_name.name,
            self.schema
                .columns
                .iter()
                .zip(&self.columns)
                .map(|(col, (layout, _))| (col.name.name.as_str(), layout)),
            repr,
        );
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
                let ty = layout.emit(ids, repr);
                field(ids, &field_position, &col.name, col.index, ty)
            })
            .collect();
        structure(ids, position, &row_name, fields, hash, false)
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

    pub(super) fn column_type(&self, ids: &mut Ids, index: usize) -> Value {
        self.columns[index].0.node(ids)
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
