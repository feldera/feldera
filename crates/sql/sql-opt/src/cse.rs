//! Eliminate equivalent operators within each circuit body. A trace can be
//! shared only when the collectors attached to each output agree as well.

use crate::operator_inputs::{OperatorInputs, output_port};
use crate::terms::Terms;
use feldera_sql_ir::{
    BodyOrder, Circuit, Error, Field, Node, NodeId, OperatorKind as Op, OutputPort,
};
use std::collections::{BTreeMap, BTreeSet, HashMap, hash_map::Entry};

#[derive(Hash, Eq, Ord, PartialEq, PartialOrd)]
struct Payload(Vec<(&'static str, usize)>);

fn payload(circuit: &Circuit, node: &Node, terms: &mut Terms) -> Payload {
    let kind = node.operator_kind().expect("CSE compares operators");
    let extra: &[&str] = match kind {
        Op::Aggregate | Op::StreamAggregate | Op::PrimitiveAggregate => &["aggregate"],
        Op::AggregateLinearPostprocess => &["postProcess"],
        Op::AggregateLinearPostprocessRetainKeys => &["postProcess", "retainKeysFunction"],
        Op::AsofJoin => &[
            "comparator",
            "isLeft",
            "leftTimestampIndex",
            "rightTimestampIndex",
        ],
        Op::ConcreteAsofJoin => &[
            "comparator",
            "isLeft",
            "leftTimestamp",
            "rightTimestamp",
            "balanced",
        ],
        Op::BinaryDistinct => &["positive"],
        Op::ChainAggregate => &["init"],
        Op::Chain => &["chain", "kinds"],
        Op::ControlledKeyFilter => &["outputType", "errorType", "error"],
        Op::Hop => &["interval", "size", "start", "timestampIndex"],
        Op::IndexedTopK => &["numbering", "equalityComparator", "outputProducer", "limit"],
        Op::IntegrateTraceRetainKeys => &["accumulate"],
        Op::IntegrateTraceRetainNValues => &["n", "which"],
        Op::Join
        | Op::JoinIndex
        | Op::LeftJoin
        | Op::LeftJoinIndex
        | Op::StreamJoin
        | Op::StreamJoinIndex => &["balanced"],
        Op::JoinFilterMap | Op::LeftJoinFilterMap => &["filter", "map", "balanced"],
        Op::StarJoinFilterMap => &["filter", "map"],
        Op::Lag => &["comparator", "projection", "offset"],
        Op::PartitionedRollingAggregate | Op::PartitionedRollingAggregateWithWaterline => {
            &["aggregate", "partitioningFunction", "lower", "upper"]
        }
        Op::Rank => &[
            "numbering",
            "rankCmpFunc",
            "outputProducer",
            "projectionFunc",
        ],
        Op::RowNumber => &["outputProducer"],
        Op::Waterline => &["init", "extractTs"],
        Op::Window => &["lowerInclusive", "upperInclusive", "lowerUnbounded"],
        Op::Constant | Op::IncrementalConstant => &["outputType"],
        _ => &[],
    };
    let fields = std::iter::once("function")
        .chain(extra.iter().copied())
        .filter_map(|name| Some((name, terms.field(circuit, node.field(name)?, &[]))))
        .collect();
    Payload(fields)
}

/// Operator equivalence after the caller has established corresponding input streams.
pub(super) fn same_operation(
    circuit: &Circuit,
    left: &Node,
    right: &Node,
    terms: &mut Terms,
) -> bool {
    let kind = left.operator_kind().expect("operator");
    !matches!(kind, Op::View | Op::Sink | Op::Nested)
        && right
            .operator_kind()
            .is_some_and(|other| other.cse_family() == kind.cse_family())
        && payload(circuit, left, terms) == payload(circuit, right, terms)
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
struct Collector {
    operation: &'static str,
    control: Vec<OutputPort>,
    payload: Payload,
}

/// Most operators have a unique combination of kind and inputs. Only intern
/// their expressions if another operator can actually share their output.
enum Candidates {
    First(NodeId),
    ByPayload(HashMap<Payload, Vec<NodeId>>),
}

impl Candidates {
    fn matching_payload(
        &mut self,
        circuit: &Circuit,
        node: &Node,
        terms: &mut Terms,
    ) -> &mut Vec<NodeId> {
        if let Self::First(first) = self {
            let first_node = circuit.node(*first).expect("previous candidate");
            *self = Self::ByPayload(HashMap::from([(
                payload(circuit, first_node, terms),
                vec![*first],
            )]));
        }
        let Self::ByPayload(groups) = self else {
            unreachable!("second candidate initialized payload groups")
        };
        groups.entry(payload(circuit, node, terms)).or_default()
    }
}

fn can_have_sibling(kind: Op, inputs: &[OutputPort]) -> bool {
    match kind {
        Op::Constant | Op::IncrementalConstant => true,
        Op::Nested
        | Op::View
        | Op::Sink
        | Op::ViewDeclaration
        | Op::InputMapWithWaterline
        | Op::SourceMultiset
        | Op::SourceLazyMap
        | Op::SourceMap => false,
        _ => !inputs.is_empty(),
    }
}

/// Select the operators whose expressions CSE needs from their ordered input ports.
#[derive(Default)]
struct CseCandidates {
    first: HashMap<(NodeId, Op, Vec<OutputPort>), NodeId>,
    needed: BTreeSet<NodeId>,
    retainers: Vec<(NodeId, NodeId)>,
}

impl CseCandidates {
    fn observe(&mut self, body: NodeId, id: NodeId, kind: Op, inputs: Vec<OutputPort>) {
        if is_retainer(kind) {
            if let Some(data) = inputs.first() {
                self.retainers.push((id, data.operator));
            }
        }
        if can_have_sibling(kind, &inputs) {
            match self.first.entry((body, kind.cse_family(), inputs)) {
                Entry::Vacant(slot) => {
                    slot.insert(id);
                }
                Entry::Occupied(slot) => {
                    self.needed.extend([*slot.get(), id]);
                }
            }
        }
    }

    fn payloads(mut self) -> BTreeSet<NodeId> {
        // A candidate's trace policy participates even when the retainer has no sibling.
        let policies = self
            .retainers
            .into_iter()
            .filter_map(|(id, data)| self.needed.contains(&data).then_some(id))
            .collect::<Vec<_>>();
        self.needed.extend(policies);
        self.needed
    }
}

pub fn projected_cse_payloads(operators: Vec<OperatorInputs>) -> BTreeSet<NodeId> {
    let mut candidates = CseCandidates::default();
    for op in operators {
        let inputs = op.inputs.into_iter().map(output_port).collect();
        candidates.observe(
            NodeId::Outer(op.body),
            NodeId::Outer(op.id),
            op.kind,
            inputs,
        );
    }
    candidates.payloads()
}

pub fn cse_payloads(circuit: &Circuit) -> Result<BTreeSet<NodeId>, Error> {
    let mut candidates = CseCandidates::default();
    for (body, id) in crate::stream_rewrites::body_members(circuit)? {
        let node = circuit.node(id).expect("validated body member");
        candidates.observe(
            body,
            id,
            node.operator_kind().expect("operator"),
            node.inputs().collect(),
        );
    }
    Ok(candidates.payloads())
}

fn is_retainer(kind: Op) -> bool {
    matches!(
        kind,
        Op::IntegrateTraceRetainNValues
            | Op::IntegrateTraceRetainKeys
            | Op::IntegrateTraceRetainValues
    )
}

fn aliases_in_body(
    circuit: &Circuit,
    parent: &Node,
    terms: &mut Terms,
    aliases: &mut BTreeMap<NodeId, NodeId>,
    inactive_constants: &BTreeSet<NodeId>,
) {
    let Some(Field::Array(body)) = parent.field("allOperators") else {
        return;
    };
    let operators = || {
        body.iter().map(|f| {
            let id = f.node_id().expect("validated body slot");
            (id, circuit.node(id).expect("validated body member"))
        })
    };
    let mut collectors = BTreeMap::<NodeId, Vec<(usize, Collector)>>::new();
    for (_, node) in operators() {
        if is_retainer(node.operator_kind().expect("validated operator")) {
            let mut inputs = node.inputs();
            if let Some(data) = inputs.next() {
                collectors.entry(data.operator).or_default().push((
                    data.number,
                    Collector {
                        operation: node.operator_kind().unwrap().wire_name(),
                        control: inputs.collect(),
                        payload: payload(circuit, node, terms),
                    },
                ));
            }
        }
    }
    for gc in collectors.values_mut() {
        gc.sort_unstable();
    }
    let mut candidates = HashMap::new();
    for (id, node) in operators() {
        let kind = node.operator_kind().expect("validated operator");
        let inputs: Vec<_> = node.inputs().collect();
        if !can_have_sibling(kind, &inputs) {
            continue;
        }
        let constant = matches!(kind, Op::Constant | Op::IncrementalConstant);
        // FindCSE compares a producer's successors when it visits that producer.
        // Its special Delta visitor only compares delta inputs. A constant seen
        // earlier in this CSE instance returns before visiting its successors.
        if !constant
            && kind != Op::Delta
            && !inputs.iter().any(|port| {
                match circuit.node(port.operator).and_then(Node::operator_kind) {
                    Some(Op::Delta) => false,
                    Some(Op::Constant | Op::IncrementalConstant) => {
                        !inactive_constants.contains(&port.operator)
                            && !aliases.contains_key(&port.operator)
                    }
                    _ => true,
                }
            })
        {
            continue;
        }
        let group = match candidates.entry((kind.cse_family(), inputs)) {
            Entry::Vacant(entry) => {
                entry.insert(Candidates::First(id));
                continue;
            }
            Entry::Occupied(entry) => entry.into_mut().matching_payload(circuit, node, terms),
        };
        if let Some(&base) = group.iter().find(|base| {
            constant || kind == Op::Delta || collectors.get(base) == collectors.get(&id)
        }) {
            aliases.insert(id, base);
        } else {
            group.push(id);
        }
    }
}

/// Repeat until merging an operator no longer exposes new sibling candidates.
pub fn common_subexpressions(circuit: Circuit) -> Result<Circuit, Error> {
    CseState::default().eliminate(circuit, None, &BTreeMap::new(), |_| {})
}

/// Run a fresh optimizer placement with the enclosing compilation's name counter.
pub fn common_subexpressions_with_names(
    circuit: Circuit,
    next: &mut u64,
) -> Result<Circuit, Error> {
    CseState::default().run(circuit, next, &BTreeMap::new())
}

/// One scheduled CSE instance can be invoked repeatedly by its enclosing optimizer.
/// Java's constant finder remembers the producers whose successors it has visited.
#[derive(Clone, Default)]
pub struct CseState {
    seen_constants: BTreeSet<NodeId>,
}

impl CseState {
    /// Resume one placement using the live constants its visitor has already seen.
    pub fn with_seen_constants(seen: impl IntoIterator<Item = NodeId>) -> Self {
        Self {
            seen_constants: seen.into_iter().collect(),
        }
    }

    /// Shuffle identities distinguish separate Flatmap shuffles with identical indexes.
    pub fn run(
        &mut self,
        circuit: Circuit,
        next: &mut u64,
        shuffles: &BTreeMap<NodeId, u64>,
    ) -> Result<Circuit, Error> {
        self.run_observing(circuit, next, shuffles, |_| {})
    }

    /// Report the substitutions after each successful round of reconstruction.
    pub fn run_observing(
        &mut self,
        circuit: Circuit,
        next: &mut u64,
        shuffles: &BTreeMap<NodeId, u64>,
        observe: impl FnMut(&BTreeMap<NodeId, NodeId>),
    ) -> Result<Circuit, Error> {
        let mut state = self.clone();
        let mut counter = *next;
        let result = state.eliminate(circuit, Some(&mut counter), shuffles, observe)?;
        *next = counter;
        *self = state;
        Ok(result)
    }

    /// Decide the substitutions before any input is rewired. The live JVM
    /// applies them with its own constructors, then presents the next graph.
    pub fn aliases(
        &self,
        circuit: &Circuit,
        shuffles: &BTreeMap<NodeId, u64>,
    ) -> Result<BTreeMap<NodeId, NodeId>, Error> {
        let mut terms = Terms::with_shuffles(shuffles);
        let mut aliases = BTreeMap::new();
        aliases_in_body(
            circuit,
            circuit.root(),
            &mut terms,
            &mut aliases,
            &self.seen_constants,
        );

        for id in circuit.operator_order(BodyOrder::Preorder)? {
            let node = circuit.node(id).expect("validated body member");
            if node.operator_kind() == Some(Op::Nested) {
                aliases_in_body(
                    circuit,
                    node,
                    &mut terms,
                    &mut aliases,
                    &self.seen_constants,
                );
            }
        }
        Ok(aliases)
    }

    fn eliminate(
        &mut self,
        mut circuit: Circuit,
        mut next: Option<&mut u64>,
        shuffles: &BTreeMap<NodeId, u64>,
        mut observe: impl FnMut(&BTreeMap<NodeId, NodeId>),
    ) -> Result<Circuit, Error> {
        loop {
            let aliases = self.aliases(&circuit, shuffles)?;
            let order = circuit.operator_order(BodyOrder::Preorder)?;
            self.seen_constants.extend(order.into_iter().filter(|id| {
                matches!(
                    circuit.node(*id).and_then(Node::operator_kind),
                    Some(Op::Constant | Op::IncrementalConstant)
                )
            }));
            if aliases.is_empty() {
                return Ok(circuit);
            }
            let rewrite = circuit.coalesce_with_rewrites(&aliases)?;
            observe(&aliases);
            circuit = match next.as_deref_mut() {
                Some(counter) => crate::rebuild::rebuild_input_closures(rewrite, counter)?,
                None => rewrite.circuit,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    fn operator(kind: Op, id: u64, inputs: &[u64]) -> Value {
        let inputs: Vec<_> = inputs
            .iter()
            .map(|n| json!({"operator":{"node":n}, "outputNumber":0}))
            .collect();
        json!({"class":kind.wire_name(),"id":id,"inputs":inputs,"annotations":[]})
    }

    #[test]
    fn repeated_instance_remembers_which_constant_successors_it_visited() {
        let graph = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            operator(Op::Constant, 1, &[]),
            operator(Op::Constant, 2, &[]),
            operator(Op::MapIndex, 3, &[1]),
            operator(Op::MapIndex, 4, &[2]),
        ]});
        let mut state = CseState::default();
        let mut next = 0;
        let shuffles = BTreeMap::new();
        let first = state
            .run(Circuit::from_json(graph).unwrap(), &mut next, &shuffles)
            .unwrap();
        assert_eq!(first.operators().count(), 3);
        let again = state.run(first, &mut next, &shuffles).unwrap();
        assert_eq!(again.operators().count(), 3);
        let fresh = CseState::default()
            .run(again, &mut next, &shuffles)
            .unwrap();
        assert_eq!(fresh.operators().count(), 2);
    }

    #[test]
    fn collectors_must_match_data_slot_and_control_before_merging() {
        let input = |bound| {
            let ops = vec![
                operator(Op::SourceMultiset, 1, &[]),
                operator(Op::SourceMultiset, 2, &[]),
                operator(Op::SourceMultiset, 3, &[]),
                operator(Op::Distinct, 4, &[1]),
                operator(Op::Distinct, 5, &[1]),
                operator(Op::IntegrateTraceRetainKeys, 6, &[4, 2]),
                operator(Op::IntegrateTraceRetainKeys, 7, &[5, bound]),
                operator(Op::Sum, 8, &[4, 5]),
            ];
            json!({"class":"DBSPCircuit","id":0,"allOperators":ops,"declarations":[]})
        };
        let projected = Circuit::from_json(input(3)).unwrap();
        assert_eq!(
            cse_payloads(&projected).unwrap(),
            [4, 5, 6, 7].map(NodeId::Outer).into()
        );
        let different = input(3);
        assert_eq!(
            common_subexpressions(Circuit::from_json(different.clone()).unwrap())
                .unwrap()
                .to_json(),
            different
        );
        let same = common_subexpressions(Circuit::from_json(input(2)).unwrap()).unwrap();
        assert_eq!(same.operators().count(), 6);
        let inputs: Vec<_> = same
            .node(NodeId::Outer(8))
            .unwrap()
            .inputs()
            .map(|p| p.operator)
            .collect();
        assert_eq!(inputs, [NodeId::Outer(4); 2]);
    }
}
