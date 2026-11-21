//! Derive and check stream kinds in each clock scope of an optimizer graph.

use super::{array, boolean, field, nesting, text, uint, wire_port, WirePort};
use crate::{Error, Result};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Kind {
    Collection,
    Delta,
    Waterline,
}

use Kind::{Collection, Delta, Waterline};

impl Kind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Collection => "COLLECTION",
            Delta => "DELTA",
            Waterline => "WATERLINE",
        }
    }
}

type Ports = Vec<Option<Kind>>;
type Scope = HashMap<u64, usize>;

pub(crate) fn infer(circuit: &Value) -> Result<Vec<Ports>> {
    let circuit_kind = if boolean(circuit, "incremental")? {
        Delta
    } else {
        Collection
    };
    let mut outputs = Vec::new();
    infer_scope(circuit, circuit_kind, None, 0, &mut outputs)?;
    Ok(outputs)
}

fn read_port(outputs: &[Ports], positions: &Scope, port: WirePort) -> Result<Kind> {
    positions
        .get(&port.operator)
        .and_then(|&source| outputs[source].get(usize::try_from(port.output).ok()?))
        .copied()
        .flatten()
        .ok_or_else(|| {
            Error(format!(
                "unavailable port {}:{}",
                port.operator, port.output
            ))
        })
}

fn infer_scope(
    region: &Value,
    clock: Kind,
    incoming: Option<&[(WirePort, Kind)]>,
    depth: usize,
    outputs: &mut Vec<Ports>,
) -> Result<Scope> {
    nesting(depth)?;
    let operators = array(region, "allOperators")?;
    let mut positions = Scope::with_capacity(operators.len());
    let mut remaining_inputs = incoming.unwrap_or_default().iter();
    for node in operators {
        let id = uint(node, "id")?;
        let class = text(node, "class")?;
        let facts = infer_operator(
            node,
            class,
            clock,
            &positions,
            &mut remaining_inputs,
            depth,
            outputs,
        )
        .map_err(|error| Error(format!("operator {id} {class}: {error}")))?;
        if positions.insert(id, outputs.len()).is_some() {
            return Err(Error(format!("duplicate operator {id}")));
        }
        outputs.push(facts);
    }
    if remaining_inputs.next().is_some() {
        return Err(Error("nested region has unused input declarations".into()));
    }
    Ok(positions)
}

fn infer_operator<'a>(
    node: &Value,
    class: &str,
    clock: Kind,
    positions: &Scope,
    remaining_inputs: &mut impl Iterator<Item = &'a (WirePort, Kind)>,
    depth: usize,
    outputs: &mut Vec<Ports>,
) -> Result<Ports> {
    let ports = array(node, "inputs")?;
    if class == "DBSPDeltaOperator" {
        let [port] = ports else {
            return Err(Error("expected one imported input".into()));
        };
        let (expected, kind) = remaining_inputs
            .next()
            .ok_or_else(|| Error("no corresponding nested input".into()))?;
        if wire_port(port)? != *expected {
            return Err(Error("delta input does not match the nested input".into()));
        }
        among(*kind, &[Collection, Delta])?;
        return Ok(vec![Some(Delta)]);
    }
    let inputs = ports
        .iter()
        .map(|input| {
            let port = wire_port(input)?;
            Ok((port, read_port(outputs, positions, port)?))
        })
        .collect::<Result<Vec<_>>>()?;
    if class == "DBSPNestedOperator" {
        let kind = inputs.first().map_or(Collection, |(_, kind)| *kind);
        for &(_, input) in &inputs {
            among(input, &[kind])?;
        }
        let inner = infer_scope(node, Delta, Some(&inputs), depth + 1, outputs)?;
        let ports = array(node, "internalOutputs")?;
        if ports.len() != array(node, "outputViews")?.len() {
            return Err(Error(
                "nested outputs and view names have different lengths".into(),
            ));
        }
        return ports
            .iter()
            .map(|port| {
                if port.is_null() {
                    Ok(None)
                } else {
                    read_port(outputs, &inner, wire_port(port)?)?;
                    Ok(Some(kind))
                }
            })
            .collect();
    }
    let kinds: Vec<_> = inputs.into_iter().map(|(_, kind)| kind).collect();
    Ok(operator_kinds(node, class, &kinds, clock)?
        .into_iter()
        .map(Some)
        .collect())
}

fn among(kind: Kind, allowed: &[Kind]) -> Result<()> {
    if allowed.contains(&kind) {
        Ok(())
    } else {
        Err(Error(format!("expected {allowed:?}, found {kind:?}")))
    }
}

fn all(inputs: &[Kind], expected: &[Kind], output: Kind) -> Result<Vec<Kind>> {
    for (index, input) in inputs.iter().enumerate() {
        among(*input, expected).map_err(|error| Error(format!("input {index}: {error}")))?;
    }
    Ok(vec![output])
}

fn pair(inputs: &[Kind], left: &[Kind], right: &[Kind]) -> Result<Kind> {
    let [first, second] = inputs else {
        return Err(Error("expected two inputs".into()));
    };
    among(*first, left).map_err(|error| Error(format!("input 0: {error}")))?;
    among(*second, right).map_err(|error| Error(format!("input 1: {error}")))?;
    Ok(*first)
}

fn operator_kinds(node: &Value, class: &str, inputs: &[Kind], circuit: Kind) -> Result<Vec<Kind>> {
    const DATA: &[Kind] = &[Collection, Delta];
    match class {
        "DBSPSourceMultisetOperator"
        | "DBSPSourceMapOperator"
        | "DBSPSourceLazyMapOperator"
        | "DBSPViewDeclarationOperator" => all(inputs, &[], circuit),
        "DBSPInputMapWithWaterlineOperator" => {
            all(inputs, &[], circuit)?;
            Ok(vec![circuit, Delta, Waterline])
        }
        "DBSPConstantOperator" | "DBSPNowOperator" => all(inputs, &[], Collection),
        "DBSPIncrementalConstantOperator" => all(inputs, &[], Delta),
        "DBSPDifferentiateOperator" => all(inputs, &[Collection], Delta),
        "DBSPIntegrateOperator" | "DBSPDelayedIntegralOperator" => {
            all(inputs, &[Delta], Collection)
        }
        "DBSPWaterlineOperator" => all(inputs, DATA, Waterline),
        "DBSPApplyOperator" | "DBSPApply2Operator" | "DBSPApplyNOperator" => {
            all(inputs, &[Waterline], Waterline)
        }
        "DBSPBinaryDistinctOperator" => {
            pair(inputs, &[Collection], &[Delta])?;
            Ok(vec![Delta])
        }
        "DBSPPrimitiveAggregateOperator" => {
            pair(inputs, &[Delta], &[Collection])?;
            Ok(vec![Delta])
        }
        "DBSPPartitionedRollingAggregateWithWaterlineOperator" | "DBSPWindowOperator" => {
            pair(inputs, &[Delta], &[Waterline])?;
            Ok(vec![Delta])
        }
        "DBSPControlledKeyFilterOperator" => {
            let kind = pair(inputs, &[Collection, Delta, Waterline], &[Waterline])?;
            Ok(vec![kind, Delta])
        }
        "DBSPAggregateLinearPostprocessRetainKeysOperator"
        | "DBSPIntegrateTraceRetainKeysOperator"
        | "DBSPIntegrateTraceRetainValuesOperator"
        | "DBSPIntegrateTraceRetainNValuesOperator" => Ok(vec![pair(inputs, DATA, &[Waterline])?]),
        "DBSPDelayOperator" | "DBSPViewOperator" | "DBSPSinkOperator" => {
            let [kind] = inputs else {
                return Err(Error("expected one input".into()));
            };
            if class != "DBSPDelayOperator" {
                among(*kind, DATA)?;
            }
            if class == "DBSPSinkOperator" && !boolean(field(node, "metadata")?, "system")? {
                among(*kind, &[circuit])?;
            }
            Ok(vec![*kind])
        }
        "DBSPStreamAggregateOperator"
        | "DBSPStreamAntiJoinOperator"
        | "DBSPStreamDistinctOperator"
        | "DBSPStreamJoinIndexOperator"
        | "DBSPStreamJoinOperator" => all(inputs, &[Collection], Collection),
        "DBSPAggregateOperator"
        | "DBSPAntiJoinOperator"
        | "DBSPAsofJoinOperator"
        | "DBSPConcreteAsofJoinOperator"
        | "DBSPDistinctOperator"
        | "DBSPIndexedTopKOperator"
        | "DBSPJoinFilterMapOperator"
        | "DBSPJoinIndexOperator"
        | "DBSPJoinOperator"
        | "DBSPLagOperator"
        | "DBSPLeftJoinFilterMapOperator"
        | "DBSPLeftJoinIndexOperator"
        | "DBSPLeftJoinOperator"
        | "DBSPPartitionedRollingAggregateOperator"
        | "DBSPPositiveOperator"
        | "DBSPRankOperator"
        | "DBSPRowNumberOperator"
        | "DBSPStarJoinFilterMapOperator"
        | "DBSPStarJoinIndexOperator"
        | "DBSPStarJoinOperator"
        | "DBSPUpsertFeedbackOperator" => all(inputs, &[Delta], Delta),
        "DBSPAggregateLinearPostprocessOperator"
        | "DBSPAggregateZeroOperator"
        | "DBSPAtomicSumOperator"
        | "DBSPChainAggregateOperator"
        | "DBSPChainOperator"
        | "DBSPDeindexOperator"
        | "DBSPFilterOperator"
        | "DBSPFlatMapIndexOperator"
        | "DBSPFlatMapOperator"
        | "DBSPHopOperator"
        | "DBSPMapIndexOperator"
        | "DBSPMapOperator"
        | "DBSPNegateOperator"
        | "DBSPNoopOperator"
        | "DBSPSubtractOperator"
        | "DBSPSumOperator"
        | "DBSPWeighOperator"
        | "DBSPWeightValidatorOperator" => {
            let kind = *inputs
                .first()
                .ok_or_else(|| Error("expected data inputs".into()))?;
            among(kind, DATA)?;
            all(inputs, &[kind], kind)
        }
        _ => Err(Error("unknown stream-kind rule".into())),
    }
}
