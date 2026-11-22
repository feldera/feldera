//! Validate operator inputs and the circuit's table and output contracts in visit order.
use super::{OutputClocks, StreamKind};
use crate::{BodyOrder, Error, Field, Node, OperatorKind as Op, OutputPort};
use serde::Serialize;
use StreamKind::{Collection, Delta, Waterline};

#[derive(Debug, PartialEq, Eq, Serialize)]
#[serde(tag = "reason", rename_all = "snake_case")]
pub enum StreamViolation {
    Input {
        operator: u64,
        input: usize,
        expected: &'static [StreamKind],
        actual: StreamKind,
    },
    NoInputs {
        operator: u64,
    },
    Table {
        operator: u64,
        expected: StreamKind,
        actual: StreamKind,
    },
    Output {
        operator: u64,
        expected: StreamKind,
        actual: StreamKind,
    },
}

const DATA: &[StreamKind] = &[Delta, Collection];

enum Inputs {
    Unrestricted,
    All(&'static [StreamKind]),
    Common,
    Ports(&'static [&'static [StreamKind]]),
}

fn inputs(op: Op) -> Inputs {
    use Inputs::*;
    match op {
        Op::InputMapWithWaterline
        | Op::SourceMultiset
        | Op::SourceLazyMap
        | Op::SourceMap
        | Op::ViewDeclaration
        | Op::Now
        | Op::Constant
        | Op::IncrementalConstant
        | Op::Delay => Unrestricted,
        Op::Integrate
        | Op::DelayedIntegral
        | Op::Aggregate
        | Op::AntiJoin
        | Op::AsofJoin
        | Op::ConcreteAsofJoin
        | Op::Distinct
        | Op::IndexedTopK
        | Op::Join
        | Op::JoinFilterMap
        | Op::JoinIndex
        | Op::Lag
        | Op::LeftJoin
        | Op::LeftJoinFilterMap
        | Op::LeftJoinIndex
        | Op::PartitionedRollingAggregate
        | Op::Positive
        | Op::Rank
        | Op::RowNumber
        | Op::StarJoin
        | Op::StarJoinFilterMap
        | Op::StarJoinIndex
        | Op::UpsertFeedback => All(&[Delta]),
        Op::Differentiate
        | Op::StreamAggregate
        | Op::StreamAntiJoin
        | Op::StreamDistinct
        | Op::StreamJoin
        | Op::StreamJoinIndex => All(&[Collection]),
        Op::Waterline | Op::Delta => All(DATA),
        Op::Apply | Op::Apply2 | Op::ApplyN => All(&[Waterline]),
        Op::PrimitiveAggregate => Ports(&[&[Delta], &[Collection]]),
        Op::BinaryDistinct => Ports(&[&[Collection], &[Delta]]),
        Op::Window | Op::PartitionedRollingAggregateWithWaterline => {
            Ports(&[&[Delta], &[Waterline]])
        }
        Op::AggregateLinearPostprocessRetainKeys
        | Op::IntegrateTraceRetainKeys
        | Op::IntegrateTraceRetainValues
        | Op::IntegrateTraceRetainNValues => Ports(&[DATA, &[Waterline]]),
        // A controlled filter also accepts a waterline on its data input.
        Op::ControlledKeyFilter => Ports(&[&[Delta, Collection, Waterline], &[Waterline]]),
        Op::View | Op::Sink => Ports(&[DATA]),
        Op::AggregateLinearPostprocess
        | Op::AggregateZero
        | Op::AtomicSum
        | Op::Chain
        | Op::ChainAggregate
        | Op::Deindex
        | Op::Filter
        | Op::FlatMap
        | Op::FlatMapIndex
        | Op::Hop
        | Op::Map
        | Op::MapIndex
        | Op::Negate
        | Op::Nested
        | Op::Noop
        | Op::Subtract
        | Op::Sum
        | Op::Weigh
        | Op::WeightValidator => Common,
    }
}

impl Inputs {
    fn check(self, operator: u64, kinds: &[StreamKind]) -> Result<Option<StreamViolation>, Error> {
        let allowed = match self {
            Self::Unrestricted => return Ok(None),
            Self::All(allowed) => allowed,
            Self::Ports(ports) => {
                for (input, &expected) in ports.iter().enumerate() {
                    let actual = *kinds.get(input).ok_or_else(|| {
                        Error::new(format!("operator {operator} is missing input {input}"))
                    })?;
                    if !expected.contains(&actual) {
                        return Ok(Some(StreamViolation::Input {
                            operator,
                            input,
                            expected,
                            actual,
                        }));
                    }
                }
                return Ok(None);
            }
            Self::Common => match kinds.first() {
                None => return Ok(Some(StreamViolation::NoInputs { operator })),
                Some(Delta) => &[Delta],
                Some(Collection) => &[Collection],
                Some(Waterline) => {
                    return Ok(Some(StreamViolation::Input {
                        operator,
                        input: 0,
                        expected: DATA,
                        actual: Waterline,
                    }));
                }
            },
        };
        Ok(kinds
            .iter()
            .position(|k| !allowed.contains(k))
            .map(|input| StreamViolation::Input {
                operator,
                input,
                expected: allowed,
                actual: kinds[input],
            }))
    }
}

fn system_sink(node: &Node) -> Result<bool, Error> {
    let Some(Field::Object(metadata)) = node.field("metadata") else {
        return Err(Error::new("missing sink metadata"));
    };
    match metadata.get("system") {
        Some(Field::Data(value)) => value.as_bool(),
        _ => None,
    }
    .ok_or_else(|| Error::new("sink metadata requires the system flag"))
}

impl OutputClocks<'_> {
    /// Report the first violation at the same boundary as the live optimizer.
    /// A nested region with no exported ports still validates its inner operators.
    pub fn validate(&mut self) -> Result<Option<StreamViolation>, Error> {
        let expected = if self
            .circuit
            .root()
            .data("incremental")
            .and_then(|v| v.as_bool())
            == Some(true)
        {
            Delta
        } else {
            Collection
        };
        let mut kinds = Vec::new();
        for id in self.circuit.operator_order(BodyOrder::Postorder)? {
            let node = self.circuit.node(id).expect("validated operator");
            let op = node.operator_kind().expect("validated operator");
            if op == Op::Nested {
                let Some(Field::Array(outputs)) = node.field("internalOutputs") else {
                    return Err(Error::new("nested circuit requires its output slots"));
                };
                if outputs
                    .iter()
                    .all(|slot| OutputPort::from_field(slot).is_none())
                    || node.inputs().next().is_none()
                {
                    continue;
                }
            }
            kinds.clear();
            for port in node.inputs() {
                kinds.push(self.get(port)?);
            }
            let operator = id.number();
            if let Some(violation) = inputs(op).check(operator, &kinds)? {
                return Ok(Some(violation));
            }
            match op {
                Op::SourceMultiset
                | Op::SourceMap
                | Op::SourceLazyMap
                | Op::InputMapWithWaterline => {
                    let actual = self.get(OutputPort {
                        operator: id,
                        number: 0,
                    })?;
                    if actual != expected {
                        return Ok(Some(StreamViolation::Table {
                            operator,
                            actual,
                            expected,
                        }));
                    }
                }
                Op::Sink if kinds[0] != expected && !system_sink(node)? => {
                    return Ok(Some(StreamViolation::Output {
                        operator,
                        expected,
                        actual: kinds[0],
                    }));
                }
                _ => {}
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Circuit;
    use serde_json::{json, Value};

    fn check(nodes: &[Value]) -> Result<Option<StreamViolation>, Error> {
        let graph = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "incremental": true,
            "declarations": [], "allOperators": nodes
        }))?;
        OutputClocks::new(&graph)?.validate()
    }

    fn port(id: u64, number: usize) -> Value {
        json!({"operator":{"node":id},"outputNumber":number})
    }

    #[test]
    fn declared_table_kind_and_user_sink_must_match_the_mode() -> Result<(), Error> {
        let source = json!({"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],"sourceKind":"COLLECTION"});
        assert_eq!(
            check(&[source])?,
            Some(StreamViolation::Table {
                operator: 1,
                expected: Delta,
                actual: Collection
            })
        );
        let source = json!({"class":"DBSPConstantOperator","id":1,"inputs":[]});
        let mut sink = json!({"class":"DBSPSinkOperator","id":2,"inputs":[port(1,0)],"metadata":{"system":false}});
        assert_eq!(
            check(&[source.clone(), sink.clone()])?,
            Some(StreamViolation::Output {
                operator: 2,
                expected: Delta,
                actual: Collection
            })
        );
        sink["metadata"]["system"] = json!(true);
        assert_eq!(check(&[source, sink])?, None);
        Ok(())
    }

    #[test]
    fn inputs_use_their_numbered_port_and_report_the_first_wrong_kind() -> Result<(), Error> {
        let source = json!({"class":"DBSPInputMapWithWaterlineOperator","id":1,"inputs":[],"sourceKind":"DELTA"});
        let mut window =
            json!({"class":"DBSPWindowOperator","id":2,"inputs":[port(1,0),port(1,2)]});
        assert_eq!(check(&[source.clone(), window.clone()])?, None);
        window["inputs"] = json!([port(1, 2), port(1, 1)]);
        assert_eq!(
            check(&[source.clone(), window.clone()])?,
            Some(StreamViolation::Input {
                operator: 2,
                input: 0,
                expected: &[Delta],
                actual: Waterline
            })
        );
        window["inputs"] = json!([port(1, 1), port(1, 0)]);
        assert_eq!(
            check(&[source, window])?,
            Some(StreamViolation::Input {
                operator: 2,
                input: 1,
                expected: &[Waterline],
                actual: Delta
            })
        );
        Ok(())
    }
}
