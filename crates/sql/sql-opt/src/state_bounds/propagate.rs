//! One pass of boundedness propagation. Recursive view declarations read the
//! previous pass's facts for their feedback stream.
use super::{finite_input, pruned_streams};
use crate::stream_rewrites::body_members;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionKind, Field, Node, NodeId,
    OperatorKind as Op, OutputClocks, OutputPort, StreamKind,
};
use std::collections::{BTreeSet, HashMap};

struct Scan<'a> {
    edit: CircuitEditor,
    clocks: OutputClocks<'a>,
    known: BTreeSet<OutputPort>,
    collected: BTreeSet<OutputPort>,
    indexed_keys: &'a BTreeSet<OutputPort>,
    now_table: &'a str,
}

impl Scan<'_> {
    fn bounded(&mut self, port: OutputPort) -> Result<bool, Error> {
        Ok(self.known.contains(&port)
            || self.collected.contains(&port)
            || self.clocks.get(port)? == StreamKind::Waterline)
    }

    fn infer(&mut self, node: &Node) -> Result<bool, Error> {
        let inputs: Vec<_> = node.inputs().collect();
        let all = |this: &mut Self| {
            for &p in &inputs {
                if !this.bounded(p)? {
                    return Ok::<_, Error>(false);
                }
            }
            Ok(true)
        };
        let first = || {
            inputs
                .first()
                .copied()
                .ok_or_else(|| Error::new("operator needs a data input"))
        };
        let binary = || {
            <[OutputPort; 2]>::try_from(inputs.as_slice())
                .map_err(|_| Error::new("expected a binary join"))
        };
        Ok(match node.operator_kind().expect("ordered operator") {
            Op::Window => !node
                .data("lowerUnbounded")
                .and_then(|v| v.as_bool())
                .ok_or_else(|| Error::new("window missing lower bound flag"))?,
            Op::Waterline
            | Op::Constant
            | Op::IncrementalConstant
            | Op::PartitionedRollingAggregateWithWaterline => true,
            Op::SourceMap | Op::SourceLazyMap | Op::SourceMultiset => {
                let Some(Field::Object(metadata)) = node.field("metadata") else {
                    return Err(Error::new("missing table metadata"));
                };
                metadata.contains_key("expected_size")
                    || node.relation_name("tableName")? == self.now_table
            }
            Op::Aggregate
            | Op::StreamAggregate
            | Op::AggregateLinearPostprocess
            | Op::AggregateLinearPostprocessRetainKeys
            | Op::ChainAggregate
            | Op::PrimitiveAggregate => finite_input(&self.edit, node, true)? || all(self)?,
            Op::Distinct | Op::BinaryDistinct => {
                finite_input(&self.edit, node, false)? || all(self)?
            }
            Op::IndexedTopK => {
                let literal = match node.data("literalLimit") {
                    Some(value) => value
                        .as_bool()
                        .ok_or_else(|| Error::new("expected a literal limit flag"))?,
                    None => {
                        self.edit
                            .expr_kind(self.edit.expression(node.edge("limit")?)?)?
                            == ExpressionKind::USizeLiteral
                    }
                };
                (literal && finite_input(&self.edit, node, true)?) || all(self)?
            }
            Op::LeftJoin | Op::LeftJoinIndex | Op::LeftJoinFilterMap => {
                let [left, right] = binary()?;
                self.bounded(left)? && (self.bounded(right)? || self.indexed_keys.contains(&right))
            }
            Op::Join | Op::JoinIndex | Op::JoinFilterMap | Op::StreamJoin | Op::StreamJoinIndex => {
                let [left, right] = binary()?;
                let (l, r) = (self.bounded(left)?, self.bounded(right)?);
                (l && r)
                    || (l && self.indexed_keys.contains(&right))
                    || (r && self.indexed_keys.contains(&left))
            }
            Op::AntiJoin | Op::AsofJoin | Op::ConcreteAsofJoin => self.bounded(first()?)?,
            Op::StarJoin | Op::StarJoinIndex | Op::StarJoinFilterMap => {
                let mut unkeyed = inputs.iter().filter(|p| !self.indexed_keys.contains(p));
                match (unkeyed.next(), unkeyed.next()) {
                    (Some(p), None) => self.bounded(*p)?,
                    (Some(_), Some(_)) => false,
                    _ => {
                        for p in inputs {
                            if self.bounded(p)? {
                                return Ok(true);
                            }
                        }
                        false
                    }
                }
            }
            Op::AggregateZero
            | Op::Apply
            | Op::Apply2
            | Op::ApplyN
            | Op::AtomicSum
            | Op::Chain
            | Op::ControlledKeyFilter
            | Op::Deindex
            | Op::Delay
            | Op::Delta
            | Op::Differentiate
            | Op::Filter
            | Op::FlatMap
            | Op::FlatMapIndex
            | Op::Hop
            | Op::Map
            | Op::MapIndex
            | Op::Negate
            | Op::Noop
            | Op::Now
            | Op::Subtract
            | Op::Sum
            | Op::Weigh
            | Op::PartitionedRollingAggregate
            | Op::StreamDistinct
            | Op::Positive
            | Op::UpsertFeedback
            | Op::Rank
            | Op::RowNumber => all(self)?,
            _ => false,
        })
    }
}

fn feedback(circuit: &Circuit, body: NodeId, decl: &Node) -> Result<Option<OutputPort>, Error> {
    if body == circuit.root_id() {
        return Ok(None);
    }
    let parent = circuit.node(body).expect("enclosing body");
    let name = decl.relation_name("tableName")?.replace("-decl", "");
    for id in parent.edges("allOperators")? {
        let node = circuit.node(id).expect("body member");
        if node.operator_kind() == Some(Op::View) && node.relation_name("viewName")? == name {
            return Ok(Some(OutputPort {
                operator: id,
                number: 0,
            }));
        }
    }
    let (Some(Field::Array(names)), Some(Field::Array(outputs))) =
        (parent.field("outputViews"), parent.field("internalOutputs"))
    else {
        return Err(Error::new("recursive circuit requires named output ports"));
    };
    for (view, output) in names.iter().zip(outputs) {
        if matches!(view,Field::Object(fields) if matches!(fields.get("name"),Some(Field::Data(value)) if value.as_str()==Some(name.as_str())))
        {
            return Ok(OutputPort::from_field(output));
        }
    }
    Err(Error::new("no recursive output for view declaration"))
}

pub fn propagate_bounds(
    circuit: &Circuit,
    known: BTreeSet<OutputPort>,
    indexed_keys: &BTreeSet<OutputPort>,
    now_table: &str,
) -> Result<BTreeSet<OutputPort>, Error> {
    let mut scan = Scan {
        edit: CircuitEditor::new(circuit.clone()),
        clocks: OutputClocks::new(circuit)?,
        known,
        collected: pruned_streams(circuit)?,
        indexed_keys,
        now_table,
    };
    let parents: HashMap<_, _> = body_members(circuit)?
        .into_iter()
        .map(|(parent, id)| (id, parent))
        .collect();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("ordered operator");
        if node.operator_kind() == Some(Op::Nested) {
            let Some(Field::Array(outputs)) = node.field("internalOutputs") else {
                return Err(Error::new("missing recursive outputs"));
            };
            for (number, output) in outputs.iter().enumerate() {
                if let Some(internal) = OutputPort::from_field(output) {
                    if scan.bounded(internal)? {
                        scan.known.insert(OutputPort {
                            operator: id,
                            number,
                        });
                    }
                }
            }
        } else if node.operator_kind() == Some(Op::ViewDeclaration) {
            if let Some(port) = feedback(circuit, parents[&id], node)? {
                if scan.bounded(port)? {
                    scan.known.insert(OutputPort {
                        operator: id,
                        number: 0,
                    });
                }
            }
        } else if scan.infer(node)? {
            scan.known.insert(OutputPort {
                operator: id,
                number: 0,
            });
            if node.operator_kind() == Some(Op::ControlledKeyFilter) {
                scan.known.insert(OutputPort {
                    operator: id,
                    number: 1,
                });
            }
        }
    }
    Ok(scan.known)
}
