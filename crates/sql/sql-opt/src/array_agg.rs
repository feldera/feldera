//! Push a map column that consumes an ARRAY_AGG into the aggregate's postprocess
//! closure. The same expression must also run on the empty-set result.

mod build;
mod reader;
mod window;
use window::WindowIndex;

use build::{closure, compose, link, links, rebind, typed, Input, Parameters};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op,
};
use reader::{kind, Access, Binding, Reads};
use std::collections::{BTreeMap, HashMap};

/// The allocator is part of compiler state. Unused SQL variables can advance it
/// before the observed circuit, so their names cannot be recovered from the IR.
pub fn fuse_into_array_agg(mut circuit: Circuit, next_name: &mut u64) -> Result<Circuit, Error> {
    let mut order = circuit.operator_order(BodyOrder::Preorder)?;
    let mut next = *next_name;
    loop {
        let previous_count = order.len();
        let (fused, changed) = fuse_once(circuit, &order, &mut next)?;
        circuit = crate::remove_dead_operators(fused, &mut next)?;
        order = circuit.operator_order(BodyOrder::Preorder)?;
        if !changed && order.len() == previous_count {
            *next_name = next;
            return Ok(circuit);
        }
    }
}

// Removing a dead reader can leave its producer with a single live use.
// Recompute fanout before selecting the next set of candidates.
fn fuse_once(circuit: Circuit, order: &[NodeId], next: &mut u64) -> Result<(Circuit, bool), Error> {
    let mut fanout = HashMap::<NodeId, usize>::new();
    for id in order {
        for input in circuit.node(*id).expect("operator in circuit").inputs() {
            *fanout.entry(input.operator).or_default() += 1;
        }
    }
    let mut edit = CircuitEditor::new(circuit);
    let mut changed = false;
    for &reader in order {
        let node = edit.node(reader)?;
        if !matches!(
            node.operator_kind(),
            Some(Op::Map | Op::MapIndex | Op::Join | Op::StreamJoin | Op::StarJoin)
        ) {
            continue;
        }
        let inputs: Vec<_> = node.inputs().collect();
        for (index, input) in inputs.iter().enumerate() {
            if fanout.get(&input.operator) != Some(&1) {
                continue;
            }
            let source = edit.node(input.operator)?;
            let (producer, window) = match source.operator_kind() {
                Some(Op::Aggregate | Op::StreamAggregate) => (input.operator, None),
                Some(Op::MapIndex) => {
                    let Some(port) = source.inputs().next() else {
                        continue;
                    };
                    if fanout.get(&port.operator) != Some(&1)
                        || edit.node(port.operator)?.operator_kind()
                            != Some(Op::PartitionedRollingAggregate)
                    {
                        continue;
                    }
                    let Some(window) =
                        WindowIndex::recognize(&edit, input.operator, port.operator)?
                    else {
                        continue;
                    };
                    (port.operator, Some(window))
                }
                _ => continue,
            };
            if let Some(matched) = Match::find(&edit, producer, reader, index, window)? {
                matched.rewrite(&mut edit, next)?;
                changed = true;
                break;
            }
        }
    }
    Ok((edit.finish()?, changed))
}

struct Match {
    producer: NodeId,
    reader: NodeId,
    list: NodeId,
    function: NodeId,
    binding: Binding,
    window: Option<WindowIndex>,
    columns: Vec<NodeId>,
    /// Aggregate index to reader column and its field access.
    fused: BTreeMap<usize, (usize, Access)>,
}

impl Match {
    fn find(
        edit: &CircuitEditor,
        producer: NodeId,
        reader: NodeId,
        input: usize,
        window: Option<WindowIndex>,
    ) -> Result<Option<Self>, Error> {
        let producer_node = edit.node(producer)?;
        let Some(list) = producer_node.field("aggregate").and_then(Field::node_id) else {
            return Ok(None);
        };
        let collection = edit.node(producer_node.edge("outputType")?)?;
        if window.is_none()
            && edit
                .node(collection.edge("keyType")?)?
                .edges("tupFields")?
                .is_empty()
        {
            return Ok(None);
        }
        let aggregates = edit.node(list)?.edges("aggregates")?;
        let function = edit.node(reader)?.edge("function")?;
        if kind(edit, function) != Some(Expr::Closure) {
            return Ok(None);
        }
        let body = edit.node(function)?.edge("body")?;
        let mut columns = Vec::new();
        if !build::columns(edit, body, &mut columns)? {
            return Ok(None);
        }
        let pair = matches!(
            edit.node(reader)?.operator_kind(),
            Some(Op::Map | Op::MapIndex)
        );
        let binding = Binding::new(edit, function, pair, input)?;
        let mut uses = Vec::new();
        for column in &columns {
            let mut reads = Reads::default();
            reads.collect(edit, *column, &binding)?;
            uses.push(reads);
        }
        if uses.iter().any(|u| u.unsupported) {
            return Ok(None);
        }
        let mut fused = BTreeMap::new();
        for (index, aggregate) in aggregates.into_iter().enumerate() {
            let aggregate = edit.node(aggregate)?;
            if aggregate.class() != "NonLinearAggregate"
                || aggregate
                    .field("postProcess")
                    .and_then(Field::node_id)
                    .is_none()
                || edit.node(aggregate.edge("type")?)?.class() != "DBSPTypeArray"
                || edit
                    .node(aggregate.edge("semigroup")?)?
                    .data("name")
                    .and_then(|v| v.as_str())
                    != Some("ConcatSemigroup")
            {
                continue;
            }
            let mut readers = uses
                .iter()
                .enumerate()
                .filter(|(_, u)| u.values.contains(&index));
            let Some((c, uses)) = readers.next() else {
                continue;
            };
            if readers.next().is_some() || uses.key || uses.values.len() != 1 {
                continue;
            }
            let column = columns[c];
            let ty = edit.node(typed(edit, column)?)?;
            if !uses.expensive
                && !matches!(
                    ty.class(),
                    "DBSPTypeInteger"
                        | "DBSPTypeBool"
                        | "DBSPTypeDate"
                        | "DBSPTypeTime"
                        | "DBSPTypeTimestamp"
                        | "DBSPTypeTimestampTz"
                        | "DBSPTypeUuid"
                        | "DBSPTypeDecimal"
                        | "DBSPTypeDouble"
                        | "DBSPTypeReal"
                )
            {
                continue;
            }
            let read = *uses
                .accesses
                .first()
                .ok_or_else(|| Error::new("missing aggregate field in map column"))?;
            fused.insert(index, (c, read));
        }
        Ok((!fused.is_empty()).then_some(Self {
            producer,
            reader,
            list,
            function,
            binding,
            window,
            columns,
            fused,
        }))
    }

    fn rewrite(self, edit: &mut CircuitEditor, next_name: &mut u64) -> Result<(), Error> {
        edit.move_before(self.producer, self.reader)?;
        if let Some(window) = &self.window {
            edit.move_before(window.operator, self.reader)?;
        }
        let output = edit.node(self.producer)?.edge("outputType")?;
        let mut old_value = edit.node(output)?.edge("elementType")?;
        if self.window.is_some() {
            let fields = edit.node(old_value)?.edges("tupFields")?;
            let [_, results] = fields.as_slice() else {
                return Err(Error::new(
                    "rolling aggregate must return an order and a result",
                ));
            };
            old_value = *results;
        }
        let mut parts = edit.node(self.list)?.edges("aggregates")?;
        let mut types = edit.node(old_value)?.edges("tupFields")?;
        for (&index, &(c, _)) in &self.fused {
            let old = parts[index];
            let post = edit.node(old)?.edge("postProcess")?;
            let params = edit.node(post)?.edges("parameters")?;
            let column = self.columns[c];
            let compose_with = |edit: &mut CircuitEditor, replacement| {
                compose(
                    edit,
                    column,
                    Some(&Input {
                        binding: &self.binding,
                        column: index,
                        replacement,
                    }),
                )
            };
            let result = compose_with(edit, edit.node(post)?.edge("body")?)?;
            let new_post = closure(edit, post, result, &params)?;
            let empty = compose_with(edit, edit.node(old)?.edge("emptySetResult")?)?;
            types[index] = typed(edit, empty)?;
            parts[index] = edit.derive_inner(
                old,
                [
                    ("postProcess", link(new_post)),
                    ("emptySetResult", link(empty)),
                    ("type", link(types[index])),
                ],
            )?;
        }

        let value = edit.derive_inner(
            old_value,
            [
                ("tupFields", links(&types)),
                ("mayBeNull", Field::Data(false.into())),
            ],
        )?;
        let empty = parts
            .iter()
            .map(|p| edit.node(*p)?.edge("emptySetResult"))
            .collect::<Result<Vec<_>, _>>()?;
        let mut fields = vec![("aggregates", links(&parts))];
        if let Some(cached) = edit
            .node(self.list)?
            .field("cachedEmptySetResult")
            .and_then(Field::node_id)
        {
            let cached =
                edit.derive_inner(cached, [("type", link(value)), ("fields", links(&empty))])?;
            fields.push(("cachedEmptySetResult", link(cached)));
        }
        let list = edit.derive_inner(self.list, fields)?;
        let (producer_type, value) = if let Some(window) = self.window {
            window.rewrite(edit, output, value, &empty, next_name)?
        } else {
            (
                edit.derive_inner(output, [("elementType", link(value))])?,
                value,
            )
        };
        edit.set_operator_field(self.producer, "outputType", link(producer_type))?;
        edit.set_operator_field(self.producer, "aggregate", link(list))?;

        let mut params = Parameters::new(edit, &self.binding, value, next_name)?;
        let mut columns = Vec::new();
        let mut values = BTreeMap::new();
        for (index, &column) in self.columns.iter().enumerate() {
            let field = if let Some((_, read)) = self.fused.values().find(|(c, _)| *c == index) {
                let field = params.project(edit, *read, false)?;
                edit.clone_if_needed(edit.expression(field)?)?.into()
            } else {
                rebind(edit, column, &self.binding, &mut params, &mut values)?
            };
            columns.push(field);
        }
        let old_body = edit.node(self.function)?.edge("body")?;
        let body = build::with_columns(edit, old_body, &mut columns.into_iter())?;
        let tuple = typed(edit, body)?;
        let closure = closure(edit, self.function, body, &params.ids)?;
        let old = edit.node(self.reader)?.edge("outputType")?;
        let output = match edit.node(self.reader)?.operator_kind() {
            Some(Op::Map) => edit.zset_type(edit.type_id(tuple)?)?.into(),
            Some(Op::MapIndex) => {
                let fields = edit.node(tuple)?.edges("tupFields")?;
                let [key, element] = fields.as_slice() else {
                    return Err(Error::new(
                        "map-index function must return a key/value pair",
                    ));
                };
                edit.derive_inner(
                    old,
                    [("keyType", link(*key)), ("elementType", link(*element))],
                )?
            }
            _ => old,
        };
        edit.set_operator_field(self.reader, "function", link(closure))?;
        edit.set_operator_field(self.reader, "outputType", link(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::NodeId;
    use serde_json::json;

    #[test]
    fn iteration_removes_a_dead_branch_without_a_fusable_aggregate() {
        let source = json!({"operator":{"node":1},"outputNumber":0});
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPNegateOperator","id":2,"inputs":[source]},
                {"class":"DBSPSinkOperator","id":3,"inputs":[source]}
            ]
        }))
        .unwrap();
        let mut next = 17;
        let result = fuse_into_array_agg(circuit, &mut next).unwrap();
        let kept: Vec<_> = result.operators().map(|(id, _)| id).collect();
        assert_eq!(kept, [NodeId::Outer(1), NodeId::Outer(3)]);
        assert_eq!(next, 17);
    }
}
