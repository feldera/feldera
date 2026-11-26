//! Lower primary-key tables to indexed sources. Classic keeps the full row in the
//! value. Gen-2 stores each column once and reconstructs the SQL row for its readers.
use crate::{
    rebuild::{deindex_function, ProjectionNames},
    stream_rewrites::body_members,
};
use feldera_sql_expr::{parameter_paths, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, Node, NodeId, OperatorKind as Op, OutputPort, TypeId,
};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Copy, Deserialize)]
pub struct IndexOptions {
    pub dedup_keys: bool,
    pub lazy_upsert: bool,
}

struct Layout {
    key: TypeId,
    value: TypeId,
    /// For each SQL column, the half of the indexed pair and its field number.
    columns: Vec<(usize, usize)>,
    dedup_keys: bool,
}

impl Layout {
    fn argument(&self, edit: &mut CircuitEditor, next: &mut u64) -> Result<ExpressionId, Error> {
        let key = edit.reference_type(self.key)?;
        let value = edit.reference_type(self.value)?;
        let pair = edit.tuple_type(key.into(), true, [key.into(), value.into()])?;
        edit.variable(pair, next)
    }

    fn column(
        &self,
        edit: &mut CircuitEditor,
        pair: ExpressionId,
        index: usize,
    ) -> Result<ExpressionId, Error> {
        let &(part, slot) = self
            .columns
            .get(index)
            .ok_or_else(|| Error::new("indexed input column out of range"))?;
        let reference = edit.tuple_field(pair, part)?;
        let value = edit.dereference(reference)?;
        edit.tuple_field(value, slot)
    }

    fn row(&self, edit: &mut CircuitEditor, pair: ExpressionId) -> Result<ExpressionId, Error> {
        let fields = (0..self.columns.len())
            .map(|i| {
                let field = self.column(edit, pair, i)?;
                edit.clone_if_needed(field)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        edit.tuple_expr(pair.into(), false, &fields)
    }
}

struct IndexProjection<'a> {
    pair: ExpressionId,
    layout: &'a Layout,
    bindings: HashSet<NodeId>,
}

impl Transformation for IndexProjection<'_> {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let n = edit.node(id)?;
        if matches!(n.inner_kind(), Some(InnerKind::Type(_))) {
            return Ok(Visit::Keep);
        }
        if self.bindings.contains(&id) {
            let row = if self.layout.dedup_keys {
                let row = self.layout.row(edit, self.pair)?;
                edit.borrow_value(row)?
            } else {
                edit.tuple_field(self.pair, 1)?
            };
            return Ok(Visit::Replace(row.into()));
        }
        if matches!(n.inner_kind(), Some(InnerKind::Expression(Expr::Field))) {
            let source = edit.node(n.edge("expression")?)?;
            if matches!(
                source.inner_kind(),
                Some(InnerKind::Expression(Expr::Deref))
            ) && self.bindings.contains(&source.edge("expression")?)
            {
                let index = n
                    .data("fieldNo")
                    .and_then(|v| v.as_u64())
                    .and_then(|v| usize::try_from(v).ok())
                    .ok_or_else(|| Error::new("expected an indexed input field number"))?;
                return Ok(Visit::Replace(
                    self.layout.column(edit, self.pair, index)?.into(),
                ));
            }
        }
        Ok(Visit::Children)
    }
}

fn columns(source: &Node) -> Result<&[Field], Error> {
    let Some(Field::Object(meta)) = source.field("metadata") else {
        return Err(Error::new("input table requires metadata"));
    };
    let Some(Field::Array(columns)) = meta.get("columnMetadata") else {
        return Err(Error::new("input table requires column metadata"));
    };
    Ok(columns)
}

fn index_source(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    options: IndexOptions,
    next: &mut u64,
) -> Result<Option<(NodeId, NodeId, Layout)>, Error> {
    let source = edit.node(site)?;
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut positions = Vec::new();
    let mut key_columns = Vec::new();
    let mut has_lateness = false;
    for (i, column) in columns(source)?.iter().enumerate() {
        let Field::Object(c) = column else {
            return Err(Error::new("invalid input column metadata"));
        };
        let ty = c
            .get("type")
            .and_then(Field::node_id)
            .ok_or_else(|| Error::new("input column requires a type"))?;
        has_lateness |= c.get("lateness").and_then(Field::node_id).is_some();
        match c.get("isPrimaryKey") {
            Some(Field::Data(v)) if v.as_bool() == Some(true) => {
                positions.push((0, keys.len()));
                keys.push(ty);
                key_columns.push(Field::Data(i.into()));
            }
            Some(Field::Data(v)) if v.as_bool() == Some(false) => {
                positions.push((1, if options.dedup_keys { values.len() } else { i }));
                values.push(ty);
            }
            _ => return Err(Error::new("input column requires its primary-key flag")),
        }
    }
    if keys.is_empty() {
        return Ok(None);
    }
    let input = source.edge("outputType")?;
    let key = edit.tuple_type(input, false, keys)?;
    let value = if options.dedup_keys {
        edit.tuple_type(input, false, values)?
    } else {
        edit.type_id(edit.node(input)?.edge("elementType")?)?
    };
    let ty = edit.indexed_zset_type(site, key, value)?;
    let layout = Layout {
        key,
        value,
        columns: positions,
        dedup_keys: options.dedup_keys,
    };
    let source = edit.node(site)?;
    let mut fields = Vec::new();
    for name in [
        "originalRowType",
        "outputType",
        "tableName",
        "sourceNamePosition",
        "sourceNameEmpty",
        "metadata",
    ] {
        if name == "outputType" {
            fields.push((name, Field::Reference(ty.into())));
        } else if let Some(field) = source.field(name) {
            fields.push((name, field.clone()));
        }
    }
    fields.push(("keyFields", Field::Array(key_columns)));
    let kind = if options.lazy_upsert && !has_lateness {
        Op::SourceLazyMap
    } else {
        Op::SourceMap
    };
    let indexed = edit.append_operator(body, site, kind, &[], fields)?;
    let (kind, function, row_type) = if options.dedup_keys {
        let pair = layout.argument(edit, next)?;
        let row = layout.row(edit, pair)?;
        (
            Op::Map,
            edit.closure_from_variables(row, &[pair])?,
            edit.expression_type(row)?,
        )
    } else {
        // The original source has no function, so the deindex closure has an empty location.
        (
            Op::Deindex,
            deindex_function(edit, key.into(), key, value, next)?,
            value,
        )
    };
    let output = edit.zset_type(row_type)?;
    let row = edit.append_operator(
        body,
        site,
        kind,
        &[port(indexed)],
        [
            ("outputType", Field::Reference(output.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    // Provenance references resolve in operator order while subsequent tables
    // are lowered. Move its definition to the first reader of this origin.
    let origin = edit.node(site)?.data("provenance").cloned();
    let reference = edit.node(indexed)?.data("provenance").cloned();
    edit.set_intermediate_origin(indexed, origin)?;
    edit.set_intermediate_origin(site, reference)?;
    edit.place_before(body, indexed, site)?;
    Ok(Some((indexed, row, layout)))
}

fn port(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

/// Rebind indexes built directly on a table to its indexed pair. Other readers
/// keep their original row type through a map or deindex at the source site.
pub fn expand_indexed_inputs(
    circuit: Circuit,
    options: IndexOptions,
    next: &mut u64,
) -> Result<Circuit, Error> {
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(b, id)| (id, b))
        .collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut layouts = HashMap::new();
    let mut names = ProjectionNames::default();
    let mut counter = *next;
    for id in order {
        let n = edit.node(id)?;
        names.visit(id, n, &mut counter)?;
        if n.operator_kind() == Some(Op::SourceMultiset) {
            if let Some((indexed, row, layout)) =
                index_source(&mut edit, bodies[&id], id, options, &mut counter)?
            {
                layouts.insert(id, (indexed, layout));
                replacements.insert(id, port(row));
                names.replaced(id);
            }
        } else if n.operator_kind() == Some(Op::MapIndex) {
            let source = n
                .inputs()
                .next()
                .ok_or_else(|| Error::new("map index requires an input"))?;
            let Some((indexed, layout)) = layouts.get(&source.operator) else {
                continue;
            };
            let closure = edit.node(n.edge("function")?)?;
            let [parameter] = closure
                .edges("parameters")?
                .try_into()
                .map_err(|_| Error::new("expected a unary index closure"))?;
            let body = closure.edge("body")?;
            let bindings = parameter_paths(&edit, body, parameter)?;
            let pair = layout.argument(&mut edit, &mut counter)?;
            let rewritten = Rewrite::translating_expressions(IndexProjection {
                pair,
                layout,
                bindings,
            })
            .apply(&mut edit, body)?;
            let row = edit.expression(rewritten)?;
            let function = edit.closure_from_variables(row, &[pair])?;
            let output = edit.indexed_zset_from_pair(edit.expression_type(row)?)?;
            edit.rewire_inputs(id, &[port(*indexed)])?;
            edit.set_operator_field(id, "outputType", Field::Reference(output.into()))?;
            edit.set_operator_field(id, "function", Field::Reference(function.into()))?;
            edit.clear_annotations(id)?;
            names.replaced(id);
        }
    }
    let circuit = names.finish(edit.finish_forwarding(&replacements)?)?;
    *next = counter;
    Ok(circuit)
}
