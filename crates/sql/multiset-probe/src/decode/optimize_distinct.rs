// Copyright 2022 VMware, Inc.
// SPDX-License-Identifier: MIT

//! Remove redundant distincts and move them through filters and snapshot joins.

use super::{
    array, closure, constant, encode_rewrite, field, text, uint,
    wire_edits::{copy_origin, operator, port, WireEdits},
    wire_port, WirePort,
};
use crate::{analysis, model::CollectionKind, Error, Result};
use serde_json::{json, Value};
use std::collections::HashMap;

const STREAM_DISTINCT: &str = "DBSPStreamDistinctOperator";
const DELTA_DISTINCT: &str = "DBSPDistinctOperator";
const POSITIVE: &str = "DBSPPositiveOperator";

struct Circuit<'a> {
    edits: WireEdits<'a>,
    operators: Vec<Value>,
    positions: HashMap<u64, usize>,
    aliases: HashMap<u64, Value>,
    multisets: HashMap<WirePort, Result<bool>>,
}

impl<'a> Circuit<'a> {
    fn new(wire: &'a Value) -> Result<Self> {
        Ok(Self {
            edits: WireEdits::new(wire)?,
            operators: Vec::new(),
            positions: HashMap::new(),
            aliases: HashMap::new(),
            multisets: HashMap::new(),
        })
    }

    fn mapped(&self, input: &Value) -> Result<Value> {
        let port = wire_port(input)?;
        let mapped = if port.output == 0 {
            self.aliases.get(&port.operator).unwrap_or(input)
        } else {
            input
        };
        self.producer(mapped)?;
        Ok(mapped.clone())
    }

    fn producer(&self, input: &Value) -> Result<&Value> {
        let id = wire_port(input)?.operator;
        self.positions
            .get(&id)
            .map(|index| &self.operators[*index])
            .ok_or_else(|| Error(format!("missing distinct-pass producer {id}")))
    }

    fn is_multiset(&self, input: &Value) -> Result<bool> {
        self.multisets
            .get(&wire_port(input)?)
            .ok_or_else(|| Error("distinct pass references an unavailable fact".into()))?
            .as_ref()
            .copied()
            .map_err(|error| Error(error.0.clone()))
    }

    fn infer(&self, node: &Value) -> Result<bool> {
        let inputs = array(node, "inputs")?;
        let inherits = || {
            self.is_multiset(
                inputs
                    .first()
                    .ok_or_else(|| Error("multiset rule requires an input".into()))?,
            )
        };
        match text(node, "class")? {
            "DBSPSourceMapOperator"
            | "DBSPSourceLazyMapOperator"
            | "DBSPNowOperator"
            | STREAM_DISTINCT
            | DELTA_DISTINCT
            | "DBSPBinaryDistinctOperator"
            | "DBSPAggregateOperator"
            | "DBSPStreamAggregateOperator"
            | "DBSPAggregateZeroOperator"
            | "DBSPPrimitiveAggregateOperator"
            | "DBSPChainAggregateOperator"
            | "DBSPAggregateLinearPostprocessOperator"
            | "DBSPAggregateLinearPostprocessRetainKeysOperator"
            | "DBSPUpsertFeedbackOperator"
            | "DBSPApplyOperator"
            | "DBSPApply2Operator"
            | "DBSPApplyNOperator"
            | "DBSPWaterlineOperator"
            | "DBSPInputMapWithWaterlineOperator" => Ok(false),
            "DBSPSourceMultisetOperator"
            | "DBSPSumOperator"
            | "DBSPAtomicSumOperator"
            | "DBSPSubtractOperator"
            | "DBSPNegateOperator"
            | "DBSPFlatMapOperator"
            | "DBSPFlatMapIndexOperator"
            | "DBSPPartitionedRollingAggregateOperator"
            | "DBSPPartitionedRollingAggregateWithWaterlineOperator"
            | "DBSPWeighOperator" => Ok(true),
            POSITIVE
            | "DBSPFilterOperator"
            | "DBSPControlledKeyFilterOperator"
            | "DBSPIntegrateOperator"
            | "DBSPDifferentiateOperator"
            | "DBSPDelayOperator"
            | "DBSPDelayedIntegralOperator"
            | "DBSPWeightValidatorOperator"
            | "DBSPNoopOperator"
            | "DBSPViewOperator"
            | "DBSPSinkOperator"
            | "DBSPHopOperator"
            | "DBSPWindowOperator"
            | "DBSPIntegrateTraceRetainKeysOperator"
            | "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator"
            | "DBSPAntiJoinOperator"
            | "DBSPStreamAntiJoinOperator" => inherits(),
            "DBSPConstantOperator" | "DBSPIncrementalConstantOperator" => {
                let kind = constant(
                    self.edits.definitions(),
                    field(node, "function")?,
                    CollectionKind::Snapshot,
                )?;
                Ok(analysis::operator_facts(&kind, |_| unreachable!("constant has no inputs"))[0])
            }
            "DBSPMapOperator"
            | "DBSPMapIndexOperator"
            | "DBSPDeindexOperator"
            | "DBSPStreamJoinOperator"
            | "DBSPStreamJoinIndexOperator"
            | "DBSPJoinOperator"
            | "DBSPJoinIndexOperator"
            | "DBSPLeftJoinOperator"
            | "DBSPLeftJoinIndexOperator"
            | "DBSPAsofJoinOperator"
            | "DBSPConcreteAsofJoinOperator"
            | "DBSPStarJoinOperator"
            | "DBSPStarJoinIndexOperator" => {
                for input in inputs {
                    if self.is_multiset(input)? {
                        return Ok(true);
                    }
                }
                Ok(!analysis::injective(&closure(
                    self.edits.definitions(),
                    field(node, "function")?,
                )?))
            }
            class => Err(Error(format!(
                "no Rust distinct-pass multiset rule for {class}"
            ))),
        }
    }

    fn add(&mut self, node: Value) -> Result<()> {
        let id = uint(&node, "id")?;
        self.multisets.insert(
            WirePort {
                operator: id,
                output: 0,
            },
            self.infer(&node),
        );
        if matches!(
            node["class"].as_str(),
            Some("DBSPControlledKeyFilterOperator" | "DBSPInputMapWithWaterlineOperator")
        ) {
            self.multisets.insert(
                WirePort {
                    operator: id,
                    output: 1,
                },
                Ok(true),
            );
        }
        if node["class"] == "DBSPInputMapWithWaterlineOperator" {
            self.multisets.insert(
                WirePort {
                    operator: id,
                    output: 2,
                },
                Ok(false),
            );
        }
        self.positions.insert(id, self.operators.len());
        self.operators.push(node);
        Ok(())
    }

    fn distinct_inputs(&self, inputs: &[Value]) -> Result<Option<Vec<Value>>> {
        let mut sources = Vec::with_capacity(inputs.len());
        for input in inputs {
            let producer = self.producer(input)?;
            if producer["class"] != STREAM_DISTINCT {
                return Ok(None);
            }
            let [source] = array(producer, "inputs")? else {
                return Err(Error("expected one stream distinct input".into()));
            };
            sources.push(source.clone());
        }
        Ok(Some(sources))
    }

    fn insert(&mut self, node: &'a Value) -> Result<()> {
        let id = self.edits.remember_type(node)?;
        let inputs = array(node, "inputs")?
            .iter()
            .map(|input| self.mapped(input))
            .collect::<Result<Vec<_>>>()?;
        let class = text(node, "class")?;
        match (class, inputs.as_slice()) {
            (STREAM_DISTINCT | DELTA_DISTINCT, [input]) => {
                let producer = self.producer(input)?;
                let input_class = text(producer, "class")?;
                if matches!(input_class, STREAM_DISTINCT | DELTA_DISTINCT)
                    || !self.is_multiset(input)?
                {
                    self.aliases.insert(id, input.clone());
                    self.edits.replaced(id);
                    return Ok(());
                }
                if class == STREAM_DISTINCT && input_class == POSITIVE {
                    let source = array(producer, "inputs")?.to_vec();
                    let distinct = self.edits.retain_with_inputs(node, id, &source)?;
                    return self.add(distinct);
                }
                if matches!(
                    input_class,
                    "DBSPStreamJoinOperator" | "DBSPMapOperator" | "DBSPSumOperator"
                ) || (class == STREAM_DISTINCT && input_class == "DBSPAtomicSumOperator")
                {
                    if let Some(sources) = self.distinct_inputs(array(producer, "inputs")?)? {
                        let mut producer = producer.clone();
                        let producer_id = self.edits.id()?;
                        producer["id"] = producer_id.into();
                        producer["inputs"] = sources.into();
                        self.add(producer)?;
                        let distinct =
                            self.edits
                                .retain_with_inputs(node, id, &[port(producer_id)])?;
                        return self.add(distinct);
                    }
                }
            }
            (POSITIVE, [input]) => {
                if matches!(
                    self.producer(input)?["class"].as_str(),
                    Some(POSITIVE | DELTA_DISTINCT)
                ) {
                    self.aliases.insert(id, input.clone());
                    self.edits.replaced(id);
                    return Ok(());
                }
            }
            ("DBSPFilterOperator", [input]) => {
                if let Some(sources) = self.distinct_inputs(&inputs)? {
                    let mut distinct = self.producer(input)?.clone();
                    let filter_id = self.edits.id()?;
                    let mut filter = node.clone();
                    filter["id"] = filter_id.into();
                    filter["inputs"] = sources.into();
                    self.add(filter)?;
                    distinct["id"] = id.into();
                    distinct["inputs"] = json!([port(filter_id)]);
                    self.edits.replaced(id);
                    return self.add(distinct);
                }
            }
            ("DBSPStreamJoinOperator", [_, _]) => {
                if let Some(sources) = self.distinct_inputs(&inputs)? {
                    let join_id = self.edits.id()?;
                    let mut join = node.clone();
                    join["id"] = join_id.into();
                    join["inputs"] = sources.into();
                    self.add(join)?;
                    let mut distinct = operator(
                        STREAM_DISTINCT,
                        id,
                        vec![port(join_id)],
                        super::inner_reference(field(node, "outputType")?)?,
                    );
                    copy_origin(node, &mut distinct);
                    self.edits.replaced(id);
                    return self.add(distinct);
                }
            }
            (
                STREAM_DISTINCT
                | DELTA_DISTINCT
                | POSITIVE
                | "DBSPFilterOperator"
                | "DBSPStreamJoinOperator",
                _,
            ) => {
                return Err(Error(format!("unexpected {class} input count")));
            }
            ("DBSPNestedOperator" | "DBSPDeltaOperator" | "DBSPViewDeclarationOperator", _) => {
                return Err(Error(
                    "distinct optimization requires a flat circuit".into(),
                ));
            }
            _ => {}
        }
        let retained = self.edits.retain_with_inputs(node, id, &inputs)?;
        self.add(retained)
    }
}

pub(crate) fn rewrite(wire: &Value) -> Result<Value> {
    let mut circuit = Circuit::new(wire)?;
    for node in array(wire, "allOperators")? {
        circuit.insert(node)?;
    }
    encode_rewrite(wire, &circuit.edits.finish(wire, circuit.operators))
}
