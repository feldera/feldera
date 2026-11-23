//! Replace narrow indexes with a common index at the first consumer that needs it.
use crate::index_key_equivalence::{ColumnRewriteError, KeyComparisonError};
use crate::index_reader::{IndexParameter, rewrite_index_reader};
use crate::rebuild::ProjectionNames;
use crate::shared_index_candidates::IndexReader;
use crate::shared_indexes::is_indexed_join;
use crate::stream_rewrites::{append_map, body_members};
use crate::wide_index::{CompareColumn, IndexComparison, project_index, split_with};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, Node, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::HashMap;

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

fn fixed_index(node: &Node, slot: usize) -> Result<bool, Error> {
    Ok(match node.operator_kind() {
        Some(Op::AntiJoin | Op::Window) => slot == 0,
        Some(Op::Sink) => {
            crate::recursive_views::metadata_field(node, "viewKind")? == "MATERIALIZED"
        }
        Some(Op::Aggregate | Op::StreamAggregate) => {
            node.field("aggregate").and_then(Field::node_id).is_none()
        }
        Some(Op::Distinct | Op::IndexedTopK | Op::Lag) => true,
        _ => false,
    })
}

struct WideIndex {
    site: NodeId,
    fields: Vec<ExpressionId>,
    nullable: bool,
    function: ExpressionId,
    output: Option<OutputPort>,
}
impl WideIndex {
    fn row_type(
        edit: &mut CircuitEditor,
        site: NodeId,
        fields: &[ExpressionId],
        nullable: bool,
    ) -> Result<TypeId, Error> {
        let types = fields
            .iter()
            .map(|&field| edit.expression_type(field).map(Into::into))
            .collect::<Result<Vec<_>, _>>()?;
        let ty = edit.tuple_type(site, false, types)?;
        edit.with_nullability(ty, nullable)
    }
    fn reader_variable(&self, edit: &mut CircuitEditor, next: &mut u64) -> Result<NodeId, Error> {
        // Each Java valueType() call constructs its own tuple type. Its column
        // types are shared with the projection and with every other consumer.
        let ty = Self::row_type(edit, self.site, &self.fields, self.nullable)?;
        let reference = edit.reference_type(ty)?;
        Ok(edit.variable(reference, next)?.into())
    }
    fn connect(
        &mut self,
        edit: &mut CircuitEditor,
        body: NodeId,
        consumer: NodeId,
        narrow: OutputPort,
    ) -> Result<OutputPort, Error> {
        if let Some(port) = self.output {
            return Ok(port);
        }
        let parent = edit
            .node(narrow.operator)?
            .inputs()
            .next()
            .ok_or_else(|| Error::new("shared index needs a parent"))?;
        let (index, _) = append_map(edit, body, self.site, Op::MapIndex, parent, self.function)?;
        edit.place_before(body, index, consumer)?;
        let port = OutputPort {
            operator: index,
            number: 0,
        };
        self.output = Some(port);
        Ok(port)
    }
}

struct SharedInput {
    index: usize,
    columns: Vec<usize>,
}

fn aggregate_list(
    edit: &mut CircuitEditor,
    list: NodeId,
    variable: NodeId,
    columns: &[usize],
) -> Result<NodeId, Error> {
    let mut parts = Vec::new();
    for part in edit.node(list)?.edges("aggregates")? {
        let node = edit.node(part)?;
        let bindings = crate::aggregate_groups::row_parameters(node)?;
        let parameters = bindings
            .iter()
            .map(|&(name, slot)| {
                let function = edit.node(node.edge(name)?)?;
                let declaration = *function
                    .edges("parameters")?
                    .get(slot)
                    .ok_or_else(|| Error::new("missing aggregate row parameter"))?;
                Ok(IndexParameter {
                    declaration,
                    variable,
                    columns: columns.to_vec(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        parts.push(rewrite_index_reader(edit, part, &parameters)?);
    }
    let mut fields = vec![("rowVar", link(variable))];
    if edit.node(list)?.field("cachedEmptySetResult").is_some() {
        let values = parts
            .iter()
            .map(|&part| edit.expression(edit.node(part)?.edge("emptySetResult")?))
            .collect::<Result<Vec<_>, Error>>()?;
        let ty = WideIndex::row_type(edit, list, &values, false)?;
        fields.push((
            "cachedEmptySetResult",
            link(edit.tuple_with_type_at(list, ty, &values)?),
        ));
    }
    fields.push((
        "aggregates",
        Field::Array(parts.into_iter().map(link).collect()),
    ));
    edit.derive_inner(list, fields)
}

struct Sharing {
    indexes: Vec<WideIndex>,
    inputs: HashMap<NodeId, HashMap<usize, SharedInput>>,
}

impl Sharing {
    fn prepare(
        edit: &mut CircuitEditor,
        candidates: &[Vec<IndexReader>],
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
        compare: &mut CompareColumn,
    ) -> Result<Option<Self>, KeyComparisonError> {
        let mut result = Self {
            indexes: Vec::new(),
            inputs: HashMap::new(),
        };
        for group in candidates {
            let mut functions = Vec::with_capacity(group.len());
            let mut fixed = Vec::with_capacity(group.len());
            for member in group {
                let index = NodeId::Outer(member.index);
                let consumer = edit.node(NodeId::Outer(member.consumer))?;
                if consumer.inputs().nth(member.slot).map(|p| p.operator) != Some(index) {
                    return Err(
                        Error::new("shared-index candidate differs from consumer input").into(),
                    );
                }
                functions.push(edit.node(index)?.edge("function")?);
                fixed.push(fixed_index(consumer, member.slot)?);
            }
            let Some(groups) =
                split_with(edit.circuit(), &functions, &fixed, next, warnings, compare)?
            else {
                return Ok(None);
            };
            for accepted in groups {
                let closures = accepted.iter().map(|&i| functions[i]).collect::<Vec<_>>();
                let Some(fields) = project_index(edit, &closures, next, warnings, compare)? else {
                    return Ok(None);
                };
                let layout = fields.layout;
                let site = NodeId::Outer(group[accepted[0]].index);
                let ty = WideIndex::row_type(edit, site, &layout.columns, fields.nullable)?;
                let value = edit.tuple_with_type(ty, &layout.columns)?;
                let pair = edit.tuple_expr(site, true, &[fields.key, value])?;
                let function = edit.closure_from_variables(pair, &[fields.variable])?;
                let index = result.indexes.len();
                result.indexes.push(WideIndex {
                    site,
                    fields: layout.columns,
                    nullable: fields.nullable,
                    function,
                    output: None,
                });
                for (i, columns) in accepted.into_iter().zip(layout.positions) {
                    let member = group[i];
                    let slots = result
                        .inputs
                        .entry(NodeId::Outer(member.consumer))
                        .or_default();
                    if slots
                        .insert(member.slot, SharedInput { index, columns })
                        .is_some()
                    {
                        return Err(Error::new(
                            "consumer input belongs to multiple shared indexes",
                        )
                        .into());
                    }
                }
            }
        }
        Ok(Some(result))
    }

    fn rewrite(
        &mut self,
        edit: &mut CircuitEditor,
        body: NodeId,
        id: NodeId,
        slots: &HashMap<usize, SharedInput>,
        next: &mut u64,
    ) -> Result<(), Error> {
        let mut ports: Vec<_> = edit.node(id)?.inputs().collect();
        for (i, port) in ports.iter_mut().enumerate() {
            if let Some(slot) = slots.get(&i) {
                *port = self.indexes[slot.index].connect(edit, body, id, *port)?;
            }
        }
        let operator = edit.node(id)?;
        let kind = operator.operator_kind();
        if is_indexed_join(kind) || kind == Some(Op::PartitionedRollingAggregate) {
            let rolling = kind == Some(Op::PartitionedRollingAggregate);
            let name = if rolling {
                "partitioningFunction"
            } else {
                "function"
            };
            let function = operator.edge(name)?;
            let mut parameters = Vec::new();
            for (i, declaration) in edit
                .node(function)?
                .edges("parameters")?
                .into_iter()
                .enumerate()
            {
                let slot = if rolling { Some(i) } else { i.checked_sub(1) };
                if let Some(source) = slot.and_then(|i| slots.get(&i)) {
                    let variable = self.indexes[source.index].reader_variable(edit, next)?;
                    parameters.push(IndexParameter {
                        declaration,
                        variable,
                        columns: source.columns.clone(),
                    });
                }
            }
            let result = rewrite_index_reader(edit, function, &parameters)?;
            edit.set_operator_field(id, name, link(result))?;
        } else if matches!(kind, Some(Op::Aggregate | Op::StreamAggregate)) {
            if let Some(list) = operator.field("aggregate").and_then(Field::node_id) {
                let source = slots
                    .get(&0)
                    .ok_or_else(|| Error::new("missing shared aggregate input"))?;
                let variable = self.indexes[source.index].reader_variable(edit, next)?;
                let result = aggregate_list(edit, list, variable, &source.columns)?;
                edit.set_operator_field(id, "aggregate", link(result))?;
            }
        }
        edit.rewire_inputs(id, &ports)
    }
}

/// Consume the ordered groups published by FindSharedIndexes. An undecided
/// expression comparison leaves the caller's allocator and diagnostics intact.
pub fn replace_shared_indexes(
    circuit: Circuit,
    candidates: &[Vec<IndexReader>],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Circuit>, Error> {
    replace_shared_indexes_with(circuit, candidates, next, warnings, |_| Ok(None))
        .or_else(ColumnRewriteError::decline)
}

/// Keep candidate selection, column layout and reader rewrites in Rust when
/// the live compiler supplies equality for an individual projected field.
pub fn replace_shared_indexes_with(
    circuit: Circuit,
    candidates: &[Vec<IndexReader>],
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
    mut compare: impl FnMut(IndexComparison<'_>) -> Result<Option<bool>, Error>,
) -> Result<Option<Circuit>, ColumnRewriteError> {
    if candidates.is_empty() {
        return Ok(Some(circuit));
    }
    let mut edit = CircuitEditor::new(circuit);
    let mut counter = *next;
    let mut emitted = Vec::new();
    let Some(mut sharing) = Sharing::prepare(
        &mut edit,
        candidates,
        &mut counter,
        &mut emitted,
        &mut compare,
    )
    .map_err(|error| ColumnRewriteError::at_comparison(error, counter, &mut emitted))?
    else {
        return Ok(None);
    };
    let parents: HashMap<_, _> = body_members(edit.circuit())?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let mut projections = ProjectionNames::default();
    for id in edit.circuit().operator_order(BodyOrder::Postorder)? {
        projections.visit(id, edit.node(id)?, &mut counter)?;
        if let Some(slots) = sharing.inputs.remove(&id) {
            sharing.rewrite(&mut edit, parents[&id], id, &slots, &mut counter)?;
            projections.replaced(id);
        }
    }
    let result = projections.finish(edit.finish_with_rewrites()?)?;
    *next = counter;
    warnings.extend(emitted);
    Ok(Some(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn annotated_join_keeps_its_annotation_and_reuses_the_index_on_both_inputs() -> Result<(), Error>
    {
        let original = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPConstantOperator","id":1,"inputs":[],"annotations":[{"class":"NoChain"}]}
            ]
        }))?;
        let (original, types) = original.with_inner_roots([json!({
            "class":"DBSPTypeInteger","id":1,"code":"INT32","width":32,"signed":true,"mayBeNull":false
        })])?;
        let root = original.root_id();
        let mut edit = CircuitEditor::new(original);
        let port = |operator| OutputPort {
            operator,
            number: 0,
        };
        let source = NodeId::Outer(1);
        let tuple = edit.tuple_type(types[0], false, [types[0]])?;
        let mut next = 0;
        let row = edit.ref_parameter(tuple, &mut next)?;
        let read = edit.deref_parameter(&row)?;
        let value = edit.tuple_field(read, 0)?;
        let value = edit.tuple_expr(source, false, &[value])?;
        let pair = edit.tuple_expr(source, true, &[value, value])?;
        let function = edit.closure(pair, &[row])?;
        let (l, _) = append_map(
            &mut edit,
            root,
            source,
            Op::MapIndex,
            port(source),
            function,
        )?;
        let (r, _) = append_map(
            &mut edit,
            root,
            source,
            Op::MapIndex,
            port(source),
            function,
        )?;
        let key = edit.ref_parameter(tuple, &mut next)?;
        let left = edit.ref_parameter(tuple, &mut next)?;
        let right = edit.ref_parameter(tuple, &mut next)?;
        let read = edit.deref_parameter(&left)?;
        let result = edit.tuple_field(read, 0)?;
        let join_fn = edit.closure(result, &[key, left, right])?;
        let out = edit.zset_type(edit.expression_type(result)?)?;
        let join = edit.append_operator(
            root,
            source,
            Op::Join,
            &[port(l), port(r)],
            [
                ("function", link(join_fn)),
                ("outputType", link(out)),
                ("balanced", Field::Data(false.into())),
            ],
        )?;
        edit.copy_annotations(source, join)?;
        let NodeId::Outer(consumer) = join else {
            unreachable!()
        };
        let mut members = Vec::new();
        for (slot, id) in [l, r].into_iter().enumerate() {
            let NodeId::Outer(index) = id else {
                unreachable!()
            };
            members.push(IndexReader {
                index,
                consumer,
                slot,
            });
        }
        let graph = replace_shared_indexes(edit.finish()?, &[members], &mut next, &mut Vec::new())?
            .unwrap();
        let join = graph.node(join).unwrap();
        assert_eq!(
            join.data("annotations"),
            Some(&json!([{"class":"NoChain"}]))
        );
        let ports: Vec<_> = join.inputs().collect();
        assert_eq!(ports[0], ports[1]);
        assert_ne!(ports[0], port(l));
        Ok(())
    }
}
