//! Decide whether a scheduled pass needs types, expressions or provenance.
//! If the graph cannot contain a rewrite, the JVM can keep its live circuit.
use feldera_sql_ir::{BodyOrder, Circuit, Error, OperatorKind as Op};

pub fn needs_full_snapshot(pass: &str, circuit: &Circuit) -> Result<bool, Error> {
    let order = circuit.operator_order(BodyOrder::Preorder)?;
    let node = |id| circuit.node(id).expect("validated circuit member");
    let has = |kinds: &[Op]| {
        order.iter().any(|&id| {
            node(id)
                .operator_kind()
                .is_some_and(|kind| kinds.contains(&kind))
        })
    };
    let reads = |reader, inputs: &[Op]| {
        order.iter().any(|&id| {
            let op = node(id);
            op.operator_kind() == Some(reader)
                && op.inputs().any(|p| {
                    node(p.operator)
                        .operator_kind()
                        .is_some_and(|kind| inputs.contains(&kind))
                })
        })
    };
    let has_dead_code = |keep_sources| -> Result<bool, Error> {
        let live = crate::dead_code::reachable(circuit, keep_sources)?;
        Ok(order
            .iter()
            .any(|&id| node(id).operator_kind() != Some(Op::Nested) && !live.contains(&id)))
    };

    Ok(match pass {
        "ExpandStreamAggregates" => has(&[Op::StreamAggregate]),
        "IndexedInputs" => order.iter().any(|&id| {
            let n = node(id);
            n.operator_kind() == Some(Op::SourceMultiset)
                && n.data("hasPrimaryKey").and_then(|v| v.as_bool()) != Some(false)
        }),
        "OptimizeProjectionVisitor" => [Op::Map, Op::MapIndex].into_iter().any(|reader| {
            reads(
                reader,
                &[
                    Op::Constant,
                    Op::FlatMap,
                    Op::Join,
                    Op::JoinIndex,
                    Op::LeftJoin,
                    Op::LeftJoinIndex,
                    Op::StarJoin,
                    Op::StarJoinIndex,
                    Op::StreamJoin,
                    Op::StreamJoinIndex,
                    Op::AsofJoin,
                ],
            )
        }),
        "PullFilterVisitor" => reads(
            Op::Filter,
            &[
                Op::Map,
                Op::MapIndex,
                Op::AggregateLinearPostprocess,
                Op::Aggregate,
                Op::StreamAggregate,
                Op::PartitionedRollingAggregate,
                Op::ChainAggregate,
                Op::Noop,
                Op::Integrate,
                Op::Differentiate,
                Op::Filter,
                Op::Constant,
                Op::IncrementalConstant,
            ],
        ),
        "PropagateConstants" => {
            has(&[Op::Distinct])
                || [Op::Map, Op::Filter, Op::Negate]
                    .into_iter()
                    .any(|consumer| reads(consumer, &[Op::Constant]))
        }
        "DeadCode" => has_dead_code(false)?,
        "DeadCodeKeepSources" | "StrayGC" => has_dead_code(true)?,
        // These composite placements also prune unused operators on their first iteration.
        "OptimizeIncrementalVisitor" => has(&[Op::Integrate]),
        "SeparateIntegrators" => {
            has(&[Op::StreamAggregate]) || crate::trace_integrators::needs_separation(circuit)?
        }
        "ChainVisitor" => crate::chains::has_chain_pattern(circuit)? || has_dead_code(true)?,
        "ImplementChains" => has(&[Op::Chain]),
        "CloneOperatorsWithFanout" => {
            crate::fanout::has_candidates(circuit)? || has_dead_code(true)?
        }
        "RemoveNoops" => has(&[Op::Noop]) || has_dead_code(true)?,
        "PushDifferentialsUp" => {
            reads(Op::Differentiate, &[Op::Map, Op::MapIndex]) || has_dead_code(true)?
        }
        "FuseIntoArrayAgg" => {
            has(&[
                Op::Aggregate,
                Op::StreamAggregate,
                Op::PartitionedRollingAggregate,
            ]) || has_dead_code(true)?
        }
        "PropagateEmptySources" => crate::empty_sources::topology_needs_rewrite(circuit)?,
        "RemoveConstantFilters" => order.iter().any(|&id| {
            let n = node(id);
            n.operator_kind() == Some(Op::Filter)
                && n.data("literalPredicate").and_then(|v| v.as_bool()) != Some(false)
        }),
        "CSE" => !crate::cse_payloads(circuit)?.is_empty(),
        "ComparatorDeclarations" => {
            for id in order {
                if crate::comparators::has_ordered_index(circuit, id)? {
                    return Ok(true);
                }
            }
            false
        }
        "BalancedJoins" => {
            !crate::balanced_joins::eligible_joins(circuit)?.is_empty() || has_dead_code(true)?
        }
        "RemoveDeindexOperators" => has(&[Op::Deindex]),
        "OptimizeDistinctVisitor" => has(&[Op::Distinct, Op::StreamDistinct, Op::Positive]),
        "RemoveIAfterD" => reads(Op::Integrate, &[Op::Differentiate, Op::IncrementalConstant]),
        "ExpandMetadataCasts" => order.iter().any(|&id| {
            let n = node(id);
            matches!(
                n.operator_kind(),
                Some(
                    Op::SourceMap
                        | Op::SourceMultiset
                        | Op::SourceLazyMap
                        | Op::InputMapWithWaterline
                )
            ) && n.data("hasInitializers").and_then(|value| value.as_bool()) != Some(false)
        }),
        "ExpandAggregateZero" => has(&[Op::AggregateZero]),
        "RemoveIdentityOperators" => has(&[Op::Map, Op::MapIndex]),
        "BuildNestedOperators" => has(&[Op::ViewDeclaration]),
        "SubstituteLeftJoins" => has(&[Op::Nested]) && has(&[Op::LeftJoin]),
        "TrimWindows" => reads(Op::MapIndex, &[Op::Window]),
        "ExpandHop" => has(&[Op::Hop]),
        "LinearPostprocessRetainKeys" => reads(
            Op::IntegrateTraceRetainKeys,
            &[Op::AggregateLinearPostprocess],
        ),
        "ImplementJoins" => has(&[Op::StreamAntiJoin, Op::StreamJoinIndex]),
        "FilterJoinVisitor" => reads(Op::Filter, &[Op::Join, Op::LeftJoin, Op::StarJoin]),
        "DecomposeExpensiveFilters" | "BreakFilters" | "DesugarNowEquality" => has(&[Op::Filter]),
        // CircuitRewriter also visits aggregate lists regardless of the
        // annotation predicate applied to ordinary function fields.
        "SimplifyWaterline" => {
            order
                .iter()
                .any(|&id| crate::waterline::has_waterline(node(id)))
                || has(&[
                    Op::Aggregate,
                    Op::StreamAggregate,
                    Op::PartitionedRollingAggregate,
                    Op::PartitionedRollingAggregateWithWaterline,
                ])
        }
        "DuplicateSharedIndexes" => crate::shared_indexes::has_shared_join_indexes(circuit)?,
        "ShareOutputIntegrators" => crate::output_integrators::has_join_index(circuit)?,
        "MergeSums" => has(&[Op::Sum]) || reads(Op::Negate, &[Op::Negate]),
        "RemoveViewOperators" | "RemoveAllViews" => has(&[Op::View]),
        _ => true,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn composites_check_dead_branches_even_when_their_own_pattern_is_absent() {
        let wire = json!({"class":"DBSPCircuit","id":0,"declarations":[], "allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
            {"class":"DBSPMapOperator","id":2,"inputs":[{"operator":{"node":1},"outputNumber":0}]}]});
        let circuit = Circuit::from_json(wire).unwrap();
        for pass in [
            "RemoveNoops",
            "PushDifferentialsUp",
            "FuseIntoArrayAgg",
            "DeadCodeKeepSources",
        ] {
            assert!(needs_full_snapshot(pass, &circuit).unwrap(), "{pass}");
        }
        assert!(!needs_full_snapshot("RemoveIAfterD", &circuit).unwrap());
        assert!(!needs_full_snapshot("CSE", &circuit).unwrap());
    }
}
