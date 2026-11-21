//! Shared wire mechanics for compiler passes that insert stream boundaries.

use super::{
    array, field, generated_closure, inner_identity, max_outer_id, text, uint, wire_port,
    Definitions,
};
use crate::{provenance::Provenance, Error, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

pub(super) fn port(id: u64) -> Value {
    json!({"operator": {"node": id}, "outputNumber": 0})
}

fn allocate(next: &mut u64) -> Result<u64> {
    *next = next
        .checked_add(1)
        .ok_or_else(|| Error("wire identity exhausted".into()))?;
    Ok(*next)
}

pub(super) fn operator(class: &str, id: u64, inputs: Vec<Value>, ty: Value) -> Value {
    json!({"class": class, "id": id, "annotations": [], "inputs": inputs, "outputType": ty})
}

pub(super) fn output_type_from_input(class: &str) -> bool {
    matches!(
        class,
        "DBSPAntiJoinOperator"
            | "DBSPAtomicSumOperator"
            | "DBSPDelayOperator"
            | "DBSPDifferentiateOperator"
            | "DBSPDistinctOperator"
            | "DBSPFilterOperator"
            | "DBSPIntegrateOperator"
            | "DBSPIntegrateTraceRetainKeysOperator"
            | "DBSPIntegrateTraceRetainNValuesOperator"
            | "DBSPIntegrateTraceRetainValuesOperator"
            | "DBSPNegateOperator"
            | "DBSPNoopOperator"
            | "DBSPPositiveOperator"
            | "DBSPSinkOperator"
            | "DBSPStreamAntiJoinOperator"
            | "DBSPStreamDistinctOperator"
            | "DBSPSubtractOperator"
            | "DBSPSumOperator"
            | "DBSPViewOperator"
            | "DBSPWindowOperator"
    )
}

pub(super) fn copy_origin(from: &Value, to: &mut Value) {
    for name in [
        "provenance",
        "sourceEmpty",
        "sourcePosition",
        "sourcePositions",
    ] {
        if let Some(value) = from.get(name) {
            to[name] = value.clone();
        }
    }
}

pub(super) fn copy_intermediate_origin(
    from: &Value,
    to: &mut Value,
    provenance: &mut Provenance<'_>,
) -> Result<()> {
    if let Some(origin) = from.get("provenance") {
        to["provenance"] = provenance.intermediate(origin)?;
    } else if from.get("sourcePosition").is_some() {
        return Err(Error(
            "expansion requires relational replay provenance".into(),
        ));
    }
    Ok(())
}

pub(super) struct WireEdits<'a> {
    definitions: Definitions<'a>,
    operators: HashMap<u64, &'a Value>,
    types: HashMap<u64, u64>,
    generated_types: HashMap<u64, Value>,
    error_types: HashMap<u64, u64>,
    replaced: HashSet<u64>,
    next_outer: u64,
    next_inner: u64,
    next_name: Option<u64>,
}

impl<'a> WireEdits<'a> {
    pub(super) fn new(circuit: &'a Value) -> Result<Self> {
        let definitions = Definitions::read(circuit)?;
        let next_inner = definitions.inner.keys().copied().max().unwrap_or_default();
        let operators = array(circuit, "allOperators")?
            .iter()
            .map(|node| Ok((uint(node, "id")?, node)))
            .collect::<Result<_>>()?;
        let next_name = circuit
            .get(generated_closure::GENERATED_NAME)
            .map(|_| uint(circuit, generated_closure::GENERATED_NAME))
            .transpose()?;
        Ok(Self {
            definitions,
            operators,
            types: HashMap::new(),
            generated_types: HashMap::new(),
            error_types: HashMap::new(),
            replaced: HashSet::new(),
            next_outer: max_outer_id(circuit)?,
            next_inner,
            next_name,
        })
    }

    pub(super) fn id(&mut self) -> Result<u64> {
        allocate(&mut self.next_outer)
    }

    pub(super) fn inner_id(&mut self) -> Result<u64> {
        allocate(&mut self.next_inner)
    }

    pub(super) fn with_inner_ids<T>(
        &mut self,
        build: impl FnOnce(&Definitions<'a>, &mut dyn FnMut() -> Result<u64>) -> Result<T>,
    ) -> Result<T> {
        let next = &mut self.next_inner;
        build(&self.definitions, &mut || allocate(next))
    }

    pub(super) fn generated_name(&mut self) -> Result<u64> {
        let next = self.next_name.as_mut().ok_or_else(|| {
            Error("generating a variable requires the generated-name counter".into())
        })?;
        let name = *next;
        *next = generated_closure::advance_name(name, 1)?;
        Ok(name)
    }

    pub(super) fn definitions(&self) -> &Definitions<'a> {
        &self.definitions
    }

    pub(super) fn replaced(&mut self, id: u64) {
        self.replaced.insert(id);
    }

    pub(super) fn output_type_id(&self, input: &Value) -> Result<u64> {
        let port = wire_port(input)?;
        if port.output == 0 {
            self.types
                .get(&port.operator)
                .copied()
                .ok_or_else(|| Error("missing type for rewritten input".into()))
        } else {
            if let Some(ty) = self.error_types.get(&port.operator) {
                return Ok(*ty);
            }
            let ty = self
                .operators
                .get(&port.operator)
                .and_then(|node| node.get("errorType"))
                .ok_or_else(|| Error("missing type for rewritten error port".into()))?;
            inner_identity(ty)
        }
    }

    pub(super) fn input_type(&self, input: &Value) -> Result<Value> {
        Ok(json!({"node": self.output_type_id(input)?}))
    }

    pub(super) fn set_output(&mut self, id: u64, ty: u64) {
        self.types.insert(id, ty);
    }

    pub(super) fn remember_error_output(&mut self, operator: u64, output: &Value) -> Result<()> {
        self.error_types.insert(operator, inner_identity(output)?);
        Ok(())
    }

    pub(super) fn remember_generated_output(
        &mut self,
        operator: u64,
        output: &Value,
    ) -> Result<()> {
        let ty = inner_identity(output)?;
        self.set_output(operator, ty);
        self.generated_types.insert(ty, output.clone());
        Ok(())
    }

    pub(super) fn remember_type(&mut self, node: &'a Value) -> Result<u64> {
        let id = uint(node, "id")?;
        let ty = if let Some(ty) = node.get("outputType") {
            Some(inner_identity(ty)?)
        } else if matches!(
            text(node, "class")?,
            "DBSPViewOperator" | "DBSPSinkOperator"
        ) {
            let [input] = array(node, "inputs")? else {
                return Err(Error("invalid view input".into()));
            };
            Some(self.output_type_id(input)?)
        } else {
            None
        };
        if let Some(ty) = ty {
            self.set_output(id, ty);
        }
        Ok(id)
    }

    /// A rebuilt source keeps its wire ID on the replacement output. Java
    /// still rebuilds the consumers because the underlying object changed.
    pub(super) fn retain(&mut self, node: &Value, id: u64) -> Result<Value> {
        self.retain_with_inputs(node, id, array(node, "inputs")?)
    }

    pub(super) fn retain_with_inputs(
        &mut self,
        node: &Value,
        id: u64,
        inputs: &[Value],
    ) -> Result<Value> {
        let mut result = node.clone();
        let changed = array(node, "inputs")? != inputs
            || inputs.iter().try_fold(false, |changed, input| {
                Ok(changed || self.replaced.contains(&wire_port(input)?.operator))
            })?;
        if !changed {
            return Ok(result);
        }
        self.replaced(id);
        let class = text(node, "class")?;
        match class {
            "DBSPNoopOperator" => {
                let [input] = inputs else {
                    return Err(Error("invalid noop input".into()));
                };
                self.rebuild_noop(&mut result, input)?;
            }
            "DBSPDeindexOperator" => {
                let [input] = inputs else {
                    return Err(Error("invalid deindex input".into()));
                };
                self.rebuild_deindex(&mut result, id, input)?;
            }
            "DBSPControlledKeyFilterOperator" | "DBSPNestedOperator" => {
                return Err(Error(
                    "unsupported consumer of a rewritten stream boundary".into(),
                ));
            }
            _ => {}
        }
        result["inputs"] = Value::Array(inputs.to_vec());
        Ok(result)
    }

    pub(super) fn retype_consumer(&mut self, node: &mut Value, id: u64) -> Result<()> {
        if self.replaced.contains(&id) && output_type_from_input(text(node, "class")?) {
            let input = array(node, "inputs")?
                .first()
                .ok_or_else(|| Error("missing input for output type".into()))?;
            let ty = self.output_type_id(input)?;
            self.set_output(id, ty);
            if node.get("outputType").is_some() {
                node["outputType"] = json!({"node": ty});
            }
        }
        Ok(())
    }

    pub(super) fn rebuild_noop(&mut self, node: &mut Value, input: &Value) -> Result<()> {
        let ty = self.output_type_id(input)?;
        let name = self.generated_name()?;
        let collection = self
            .generated_types
            .get(&ty)
            .or_else(|| self.definitions.inner.get(&ty).copied())
            .ok_or_else(|| Error("missing collection type for noop".into()))?;
        node["function"] =
            generated_closure::function(&self.definitions, collection, name, &mut || {
                allocate(&mut self.next_inner)
            })?;
        Ok(())
    }

    pub(super) fn rebuild_deindex(
        &mut self,
        node: &mut Value,
        id: u64,
        input: &Value,
    ) -> Result<()> {
        let ty = self.output_type_id(input)?;
        let name = self.generated_name()?;
        let collection = self
            .definitions
            .inner
            .get(&ty)
            .ok_or_else(|| Error("missing indexed collection for deindex".into()))?;
        let original = self
            .definitions
            .inner
            .get(&inner_identity(field(node, "function")?)?)
            .ok_or_else(|| Error("missing original deindex function".into()))?;
        let (function, output) =
            generated_closure::deindex(&self.definitions, collection, original, name, &mut || {
                allocate(&mut self.next_inner)
            })?;
        self.remember_generated_output(id, &output)?;
        node["function"] = function;
        node["outputType"] = output;
        Ok(())
    }

    pub(super) fn constant_function(&mut self, row: &Value, body: Value) -> Result<Value> {
        let name = self.generated_name()?;
        generated_closure::constant_function(row, body, name, &mut || self.inner_id())
    }

    pub(super) fn finish(self, circuit: &Value, operators: Vec<Value>) -> Value {
        let mut result = circuit.clone();
        result["allOperators"] = Value::Array(operators);
        if let Some(name) = self.next_name {
            result[generated_closure::GENERATED_NAME] = name.into();
        }
        result
    }
}
