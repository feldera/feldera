//! Replace aggregate lists, UNNEST functions, fused join predicates and identity streams.
mod trace;

use crate::{lower_aggregate_fold, lower_flatmap, lower_join_filter_map, rebuild::ProjectionNames};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, NodeId, OperatorKind as Op,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

/// These type objects belong to the compiler prefix. A MAP shares its cached key/value
/// tuple across all UNNEST expressions over that type. Empty folds share a tuple as well.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LoweringTypes {
    keep_aggregate_lists: bool,
    instrument_apply: bool,
    empty_tuple: Value,
    elements: Vec<(u64, Value)>,
}

pub struct LoweringContext {
    keep_aggregate_lists: bool,
    instrument_apply: bool,
    empty_tuple: NodeId,
    elements: HashMap<NodeId, NodeId>,
}

impl LoweringTypes {
    /// Register the supplied objects in the same identity space as the circuit.
    /// A live reply can then refer back to them just like any existing input type.
    pub fn bind(self, circuit: Circuit) -> Result<(Circuit, LoweringContext), Error> {
        let (sites, elements): (Vec<_>, Vec<_>) = self.elements.into_iter().unzip();
        let (circuit, roots) =
            circuit.with_inner_roots(std::iter::once(self.empty_tuple).chain(elements))?;
        let (&empty_tuple, elements) = roots.split_first().expect("supplied the empty tuple");
        let elements = sites
            .into_iter()
            .map(NodeId::Inner)
            .zip(elements.iter().copied())
            .collect();
        Ok((
            circuit,
            LoweringContext {
                keep_aggregate_lists: self.keep_aggregate_lists,
                instrument_apply: self.instrument_apply,
                empty_tuple,
                elements,
            },
        ))
    }
}

/// Visit nested circuits in constructor order so that folds, flatmaps and rebuilt
/// deindex projections reserve their generated bindings at the same point.
pub fn lower_circuit(
    circuit: Circuit,
    context: &LoweringContext,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut names = ProjectionNames::default();
    let mut counter = *next;
    let mut warnings = Vec::new();
    for id in order {
        let node = edit.node(id)?;
        let kind = node.operator_kind();
        if kind == Some(Op::Noop) {
            // The replacement map retains the original closure even when its input changed.
            edit.lower_noop(id)?;
            names.replaced(id);
            continue;
        }
        names.visit(id, node, &mut counter)?;
        let function = match kind {
            Some(Op::Apply | Op::Apply2 | Op::ApplyN) if context.instrument_apply => {
                let function = trace::instrument_apply(&mut edit, id)?;
                edit.set_operator_field(id, "function", Field::Reference(function.into()))?;
                names.replaced(id);
                continue;
            }
            Some(Op::FlatMap) => {
                let function = node.edge("function")?;
                if edit.expr_kind(edit.expression(function)?)? != ExpressionKind::Flatmap {
                    continue;
                }
                let element = context
                    .elements
                    .get(&function)
                    .ok_or_else(|| Error::new("missing UNNEST element type"))?;
                let element = edit.type_id(*element)?;
                lower_flatmap(&mut edit, function, element, &mut counter, &mut warnings)?
            }
            Some(Op::JoinFilterMap | Op::LeftJoinFilterMap | Op::StarJoinFilterMap) => {
                let Some(filter) = node.field("filter").and_then(Field::node_id) else {
                    continue;
                };
                let join = node.edge("function")?;
                let map = node.field("map").and_then(Field::node_id);
                lower_join_filter_map(&mut edit, join, filter, map, &mut warnings)?
            }
            Some(
                Op::Aggregate
                | Op::StreamAggregate
                | Op::PartitionedRollingAggregate
                | Op::PartitionedRollingAggregateWithWaterline,
            ) if !context.keep_aggregate_lists => {
                let field = if kind == Some(Op::PartitionedRollingAggregateWithWaterline) {
                    "aggregateList"
                } else {
                    "aggregate"
                };
                let Some(list) = node.field(field).and_then(Field::node_id) else {
                    continue;
                };
                let empty = edit.type_id(context.empty_tuple)?;
                lower_aggregate_fold(&mut edit, list, Some(empty), &mut counter, &mut warnings)?
            }
            _ => continue,
        };
        edit.install_lowered_function(id, function)?;
        names.replaced(id);
    }
    let output = names.finish(edit.finish_with_rewrites()?)?;
    *next = counter;
    Ok((output, warnings))
}
