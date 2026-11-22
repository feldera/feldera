//! Stream clocks on a decoded circuit. Source clocks are implicit in the wire format.
use crate::{Circuit, Error, NodeId, OperatorKind as Op, OutputPort};
mod validate;
use std::collections::{BTreeMap, BTreeSet};
pub use validate::StreamViolation;

#[derive(Clone, Copy, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum StreamKind {
    Collection,
    Delta,
    Waterline,
}

/// Look up clocks for the outputs a transformation actually consumes. Linear
/// operators inherit their input clock. Cache the entire path after each lookup.
pub struct OutputClocks<'a> {
    circuit: &'a Circuit,
    snapshot_sources: BTreeSet<NodeId>,
    known: BTreeMap<OutputPort, StreamKind>,
}

impl<'a> OutputClocks<'a> {
    pub fn new(circuit: &'a Circuit) -> Result<Self, Error> {
        let incremental = circuit
            .root()
            .data("incremental")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new("circuit requires its incremental flag"))?;
        // Every source inside a recursive component produces deltas in its inner clock.
        let snapshot_sources = if incremental {
            BTreeSet::new()
        } else {
            circuit.root().edges("allOperators")?.into_iter().collect()
        };
        Ok(Self {
            circuit,
            snapshot_sources,
            known: BTreeMap::new(),
        })
    }

    pub fn get(&mut self, mut port: OutputPort) -> Result<StreamKind, Error> {
        use StreamKind::{Collection, Delta, Waterline};
        let mut path = BTreeSet::new();
        let clock = loop {
            if let Some(&value) = self.known.get(&port) {
                break value;
            }
            if !path.insert(port) {
                return Err(Error::new("cyclic clock dependency"));
            }
            let node = self
                .circuit
                .node(port.operator)
                .ok_or_else(|| Error::new("missing clock producer"))?;
            if !crate::wire::has_output(node, port.number)? {
                return Err(Error::new("clock requested for an unavailable output"));
            }
            match node
                .operator_kind()
                .ok_or_else(|| Error::new("clock producer must be an operator"))?
            {
                Op::ControlledKeyFilter | Op::InputMapWithWaterline if port.number == 1 => {
                    break Delta;
                }
                Op::InputMapWithWaterline if port.number == 2 => break Waterline,
                Op::SourceMultiset
                | Op::SourceMap
                | Op::SourceLazyMap
                | Op::ViewDeclaration
                | Op::InputMapWithWaterline => {
                    // The live validator carries the actual source kind. A saved S2
                    // or S4 omits it and reconstructs it from the enclosing clock.
                    if let Some(declared) = node.data("sourceKind") {
                        break serde::Deserialize::deserialize(declared)
                            .map_err(|_| Error::new("invalid source stream kind"))?;
                    }
                    break if self.snapshot_sources.contains(&port.operator) {
                        Collection
                    } else {
                        Delta
                    };
                }
                Op::Constant
                | Op::Now
                | Op::Integrate
                | Op::DelayedIntegral
                | Op::StreamAggregate
                | Op::StreamAntiJoin
                | Op::StreamDistinct
                | Op::StreamJoin
                | Op::StreamJoinIndex => break Collection,
                Op::Aggregate
                | Op::AntiJoin
                | Op::AsofJoin
                | Op::BinaryDistinct
                | Op::ConcreteAsofJoin
                | Op::Delta
                | Op::Differentiate
                | Op::Distinct
                | Op::IncrementalConstant
                | Op::IndexedTopK
                | Op::Join
                | Op::JoinFilterMap
                | Op::JoinIndex
                | Op::Lag
                | Op::LeftJoin
                | Op::LeftJoinFilterMap
                | Op::LeftJoinIndex
                | Op::PartitionedRollingAggregate
                | Op::PartitionedRollingAggregateWithWaterline
                | Op::Positive
                | Op::PrimitiveAggregate
                | Op::Rank
                | Op::RowNumber
                | Op::StarJoin
                | Op::StarJoinFilterMap
                | Op::StarJoinIndex
                | Op::UpsertFeedback
                | Op::Window => break Delta,
                Op::Apply | Op::Apply2 | Op::ApplyN | Op::Waterline => break Waterline,
                Op::Nested if node.inputs().next().is_none() => break Collection,
                Op::AggregateLinearPostprocess
                | Op::AggregateLinearPostprocessRetainKeys
                | Op::AggregateZero
                | Op::AtomicSum
                | Op::Chain
                | Op::ChainAggregate
                | Op::ControlledKeyFilter
                | Op::Deindex
                | Op::Delay
                | Op::Filter
                | Op::FlatMap
                | Op::FlatMapIndex
                | Op::Hop
                | Op::IntegrateTraceRetainKeys
                | Op::IntegrateTraceRetainNValues
                | Op::IntegrateTraceRetainValues
                | Op::Map
                | Op::MapIndex
                | Op::Negate
                | Op::Nested
                | Op::Noop
                | Op::Sink
                | Op::Subtract
                | Op::Sum
                | Op::View
                | Op::Weigh
                | Op::WeightValidator => {
                    port = node
                        .inputs()
                        .next()
                        .ok_or_else(|| Error::new("missing input for an inherited clock"))?;
                }
            }
        };
        self.known
            .extend(path.into_iter().map(|port| (port, clock)));
        Ok(clock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_sources_and_error_ports_have_their_own_clocks() {
        let port = |n, i| json!({"operator":{"node":n},"outputNumber":i});
        let graph = json!({"class":"DBSPCircuit","id":0,"incremental":false,"declarations":[],"allOperators":[
            {"class":"DBSPInputMapWithWaterlineOperator","id":1,"inputs":[]},
            {"class":"DBSPNestedOperator","id":2,"inputs":[port(1,0)],"internalOutputs":[port(3,0)],
             "allOperators":[{"class":"DBSPViewDeclarationOperator","id":3,"inputs":[]}]},
            {"class":"DBSPControlledKeyFilterOperator","id":4,"inputs":[port(2,0),port(1,2)]},
            {"class":"DBSPNoopOperator","id":5,"inputs":[port(4,1)]}
        ]});
        let circuit = Circuit::from_json(graph).unwrap();
        let mut clocks = OutputClocks::new(&circuit).unwrap();
        for (operator, number, expected) in [
            (1, 0, StreamKind::Collection),
            (1, 2, StreamKind::Waterline),
            (2, 0, StreamKind::Collection),
            (3, 0, StreamKind::Delta),
            (4, 0, StreamKind::Collection),
            (5, 0, StreamKind::Delta),
        ] {
            assert_eq!(
                clocks
                    .get(OutputPort {
                        operator: NodeId::Outer(operator),
                        number
                    })
                    .unwrap(),
                expected
            );
        }
    }
}
