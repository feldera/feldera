//! Check that each retention policy has a trace to collect at the StrayGC placement.
use crate::trace_integrators::can_collect_input;
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Retained {
    Keys,
    Values,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "reason", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RetentionViolation {
    InputAccumulate {
        operator: u64,
    },
    TraceNonaccumulate {
        operator: u64,
    },
    Duplicate {
        operator: u64,
        previous: u64,
        dimension: Retained,
    },
    MissingSibling {
        operator: u64,
    },
}

pub(super) fn retention_slot(kind: Option<Op>) -> Option<Retained> {
    match kind {
        Some(Op::IntegrateTraceRetainKeys) => Some(Retained::Keys),
        Some(Op::IntegrateTraceRetainValues | Op::IntegrateTraceRetainNValues) => {
            Some(Retained::Values)
        }
        _ => None,
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct DuplicateRetention {
    pub first: u64,
    pub other: u64,
}

/// CheckRetain examines all successors of each policy's data port, including
/// successors that appear later in the body. Report the first conflicting
/// policy in traversal order, even when other pairs overlap it.
pub fn duplicate_retention(circuit: &Circuit) -> Result<Option<DuplicateRetention>, Error> {
    let mut first = HashMap::new();
    let mut earliest = None;
    for (position, id) in circuit
        .operator_order(BodyOrder::Postorder)?
        .into_iter()
        .enumerate()
    {
        let node = circuit.node(id).expect("validated operator");
        let Some(dimension) = retention_slot(node.operator_kind()) else {
            continue;
        };
        let data = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("retention requires a data port"))?;
        let NodeId::Outer(operator) = id else {
            unreachable!("body member")
        };
        let (first_position, first_operator) = *first
            .entry((data, dimension))
            .or_insert((position, operator));
        if first_position != position
            && earliest
                .as_ref()
                .is_none_or(|(best, _)| first_position < *best)
        {
            earliest = Some((
                first_position,
                DuplicateRetention {
                    first: first_operator,
                    other: operator,
                },
            ));
        }
    }
    Ok(earliest.map(|(_, finding)| finding))
}

pub fn retention_violation(circuit: &Circuit) -> Result<Option<RetentionViolation>, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let node = |id| circuit.node(id).expect("validated circuit member");
    let mut collected_inputs = HashSet::new();
    for &id in &order {
        let consumer = node(id);
        if consumer.operator_kind().is_some_and(can_collect_input) {
            collected_inputs.extend(consumer.inputs().map(|port| port.operator));
        }
    }

    let mut retainers = HashMap::new();
    for id in order {
        let retainer = node(id);
        let Some(dimension) = retention_slot(retainer.operator_kind()) else {
            continue;
        };
        let accumulate = match dimension {
            Retained::Keys => retainer
                .data("accumulate")
                .and_then(|v| v.as_bool())
                .ok_or_else(|| Error::new("key retention requires an accumulate flag"))?,
            Retained::Values => true,
        };
        let mut inputs = retainer.inputs();
        let data = inputs
            .next()
            .filter(|_| inputs.count() == 1)
            .ok_or_else(|| Error::new("retention requires a data port and a waterline"))?;
        let source = node(data.operator).operator_kind();
        let input_table = source == Some(Op::InputMapWithWaterline);
        let NodeId::Outer(operator) = id else {
            unreachable!("body member")
        };
        if input_table && accumulate {
            return Ok(Some(RetentionViolation::InputAccumulate { operator }));
        }
        if !input_table && !accumulate {
            return Ok(Some(RetentionViolation::TraceNonaccumulate { operator }));
        }
        if let Some(previous) = retainers.insert((data, dimension), operator) {
            return Ok(Some(RetentionViolation::Duplicate {
                operator,
                previous,
                dimension,
            }));
        }
        let owns_key_trace = dimension == Retained::Keys
            && matches!(
                source,
                Some(
                    Op::AggregateLinearPostprocessRetainKeys
                        | Op::ChainAggregate
                        | Op::Aggregate
                        | Op::Lag
                )
            );
        if !input_table && !owns_key_trace && !collected_inputs.contains(&data.operator) {
            return Ok(Some(RetentionViolation::MissingSibling { operator }));
        }
    }
    Ok(None)
}

/// The enclosing graph pass prunes dead operators after each visit. A dead
/// integrator can be the only sibling that justified a retainer on the first visit.
pub fn check_and_prune_retention(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Option<RetentionViolation>), Error> {
    if let Some(violation) = retention_violation(&circuit)? {
        return Ok((circuit, Some(violation)));
    }
    let circuit = crate::remove_dead_operators(circuit, next)?;
    let violation = retention_violation(&circuit)?;
    Ok((circuit, violation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn trace(source: &str, siblings: &[&str], retainers: &[(&str, bool, usize)]) -> Circuit {
        let port = |id, n| json!({"operator":{"node":id},"outputNumber":n});
        let mut ops = vec![
            json!({"class":source,"id":1,"inputs":[]}),
            json!({"class":"DBSPConstantOperator","id":2,"inputs":[]}),
        ];
        for (i, class) in siblings.iter().enumerate() {
            ops.push(json!({"class":class,"id":10+i,"inputs":[port(1,0)]}));
        }
        for (i, (class, accumulate, output)) in retainers.iter().enumerate() {
            ops.push(json!({"class":class,"id":20+i,"inputs":[port(1,*output),port(2,0)],"accumulate":accumulate}));
        }
        Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[],
                                  "allOperators":Value::Array(ops)}))
        .unwrap()
    }

    const KEYS: &str = "DBSPIntegrateTraceRetainKeysOperator";
    const VALUES: &str = "DBSPIntegrateTraceRetainValuesOperator";
    const N_VALUES: &str = "DBSPIntegrateTraceRetainNValuesOperator";

    #[test]
    fn key_and_value_limits_are_independent_but_value_limits_share_a_slot() {
        let graph = trace(
            "DBSPSourceMultisetOperator",
            &["DBSPDistinctOperator"],
            &[(KEYS, true, 0), (VALUES, true, 0), (N_VALUES, true, 0)],
        );
        assert_eq!(
            retention_violation(&graph).unwrap(),
            Some(RetentionViolation::Duplicate {
                operator: 22,
                previous: 21,
                dimension: Retained::Values
            })
        );
        let input = trace(
            "DBSPInputMapWithWaterlineOperator",
            &[],
            &[(KEYS, false, 0), (KEYS, false, 1)],
        );
        assert_eq!(retention_violation(&input).unwrap(), None);
    }

    #[test]
    fn duplicate_scan_reports_the_first_policys_other_reader() {
        let graph = trace(
            "DBSPSourceMultisetOperator",
            &["DBSPDistinctOperator"],
            &[
                (KEYS, true, 0),
                (VALUES, true, 0),
                (N_VALUES, true, 0),
                (KEYS, true, 0),
            ],
        );
        assert_eq!(
            duplicate_retention(&graph).unwrap(),
            Some(DuplicateRetention {
                first: 20,
                other: 23
            })
        );
        assert!(matches!(
            retention_violation(&graph).unwrap(),
            Some(RetentionViolation::Duplicate {
                operator: 22,
                previous: 21,
                ..
            })
        ));
    }

    #[test]
    fn accumulate_variant_depends_on_the_data_input() {
        let input = trace(
            "DBSPInputMapWithWaterlineOperator",
            &[],
            &[(VALUES, true, 0)],
        );
        assert_eq!(
            retention_violation(&input).unwrap(),
            Some(RetentionViolation::InputAccumulate { operator: 20 })
        );
        let trace = trace("DBSPLagOperator", &[], &[(KEYS, false, 0)]);
        assert_eq!(
            retention_violation(&trace).unwrap(),
            Some(RetentionViolation::TraceNonaccumulate { operator: 20 })
        );
    }

    #[test]
    fn dead_sibling_cannot_justify_garbage_collection_after_pruning() {
        let graph = trace(
            "DBSPSourceMultisetOperator",
            &["DBSPDistinctOperator"],
            &[(KEYS, true, 0)],
        );
        assert_eq!(retention_violation(&graph).unwrap(), None);
        let (_, finding) = check_and_prune_retention(graph, &mut 0).unwrap();
        assert_eq!(
            finding,
            Some(RetentionViolation::MissingSibling { operator: 20 })
        );
    }
}
