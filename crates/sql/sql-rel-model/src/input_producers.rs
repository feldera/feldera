//! Producer topology for checking input environments against ordered row types.
//!
//! This retains graph identity and input slot layout. Operator-specific relational
//! semantics (predicates, grouping, frames, hints and catalog bindings) belong to
//! the full relation model and are not inferred from this topology.
use crate::{
    TypeId, ValidationError, ValidationLimits,
    graph::check_graph,
    input_uses::{InputOwner, InputUses},
    types::{Field, TypeKind},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ProducerVersion {
    #[serde(rename = "input-producers-v0")]
    V0,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum RelationForm {
    LogicalTableScan,
    JdbcTableScan,
    LogicalValues,
    LogicalProject,
    LogicalFilter,
    LogicalUnion,
    LogicalIntersect,
    LogicalMinus,
    LogicalJoin,
    LogicalAsofJoin,
    LogicalAggregate,
    LogicalWindow,
    LogicalSort,
    Uncollect,
    Collect,
    LogicalTableFunctionScan,
    LogicalCorrelate,
}

impl RelationForm {
    fn accepts_inputs(self, count: usize) -> bool {
        match self {
            Self::LogicalTableScan | Self::JdbcTableScan | Self::LogicalValues => count == 0,
            Self::LogicalJoin | Self::LogicalAsofJoin | Self::LogicalCorrelate => count == 2,
            Self::LogicalUnion | Self::LogicalIntersect | Self::LogicalMinus => count > 0,
            Self::LogicalTableFunctionScan => true,
            _ => count == 1,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputProducer {
    pub form: RelationForm,
    pub row: TypeId,
    pub inputs: Vec<u32>,
    /// Window constants follow ordinary input fields in slot numbering.
    pub constants: Vec<TypeId>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputProducers {
    pub schema_version: ProducerVersion,
    pub relations: Vec<InputProducer>,
    pub roots: Vec<u32>,
    pub function_rows: Vec<TypeId>,
}

impl InputProducers {
    /// InputUses validation must run first; this method also performs it so that
    /// callers cannot accidentally skip the referenced type and use checks.
    pub fn validate(
        &self,
        uses: &InputUses,
        limits: ValidationLimits,
        states: usize,
    ) -> Result<(), ValidationError> {
        uses.validate(limits, states)?;
        let error = |message| ValidationError {
            path: "input_producers".into(),
            message,
        };
        if self.relations.len() != uses.relations as usize
            || self.function_rows.len() != uses.functions as usize
        {
            return Err(error("producer arena sizes differ from input-use contract"));
        }
        let row = |ty: TypeId| -> Result<&[Field], ValidationError> {
            uses.type_table.reference(ty, "producer row")?;
            match &uses.type_table.types[ty.0 as usize].kind {
                TypeKind::Row { fields, .. } => Ok(fields),
                _ => Err(error("producer requires a row type")),
            }
        };
        let mut edges = Vec::with_capacity(self.relations.len());
        let mut ordinary = HashSet::new();
        let mut references = self.roots.len() + self.function_rows.len();
        for producer in &self.relations {
            references = references
                .checked_add(1 + producer.inputs.len() + producer.constants.len())
                .ok_or_else(|| error("producer reference count overflow"))?;
            if references > limits.references {
                return Err(error("producer references exceed limit"));
            }
            row(producer.row)?;
            if !producer.form.accepts_inputs(producer.inputs.len()) {
                return Err(error("relation input arity differs from its form"));
            }
            if producer
                .inputs
                .iter()
                .any(|id| *id as usize >= self.relations.len())
            {
                return Err(error("relation input is outside producer arena"));
            }
            for constant in &producer.constants {
                uses.type_table
                    .reference(*constant, "window constant type")?;
            }
            if producer.form != RelationForm::LogicalWindow {
                if !producer.constants.is_empty() {
                    return Err(error("only windows have constant input slots"));
                }
                ordinary.insert(producer.inputs.as_slice());
            }
            edges.push(producer.inputs.iter().map(|id| *id as usize).collect());
        }
        if references > limits.references {
            return Err(error("producer references exceed limit"));
        }
        for root in &self.roots {
            if *root as usize >= self.relations.len() {
                return Err(error("producer root outside arena"));
            }
        }
        check_graph(&edges, limits.depth, "input producers")?;
        for function in &self.function_rows {
            row(*function)?;
        }
        // Avoid constructing a flattened row for each use. Stream across the
        // ordered producers and compare existing type IDs one slot at a time.
        let mut checked_slots = 0usize;
        for environment in &uses.environments {
            let expected = row(environment.row)?;
            checked_slots = checked_slots
                .checked_add(expected.len())
                .ok_or_else(|| error("environment slot count overflow"))?;
            if checked_slots > limits.references {
                return Err(error("environment slot comparisons exceed limit"));
            }
            let (inputs, constants): (&[u32], &[TypeId]) = match &environment.owner {
                InputOwner::Constant {} => (&[], &[]),
                InputOwner::Relations { inputs } => {
                    if !ordinary.contains(inputs.as_slice()) {
                        return Err(error("input environment has no consumer"));
                    }
                    (inputs, &[])
                }
                InputOwner::Window { relation, inputs } => {
                    let producer = &self.relations[*relation as usize];
                    if producer.form != RelationForm::LogicalWindow || producer.inputs != *inputs {
                        return Err(error("window environment differs from its producer"));
                    }
                    (inputs, &producer.constants)
                }
                InputOwner::Function { function } => {
                    if expected != row(self.function_rows[*function as usize])? {
                        return Err(error("function environment differs from its parameters"));
                    }
                    continue;
                }
            };
            let mut offset = 0;
            for input in inputs {
                let fields = row(self.relations[*input as usize].row)?;
                let end = offset + fields.len();
                if expected.get(offset..end) != Some(fields) {
                    return Err(error("environment differs from ordered producer fields"));
                }
                offset = end;
            }
            for constant in constants {
                if expected.get(offset).map(|field| field.ty) != Some(*constant) {
                    return Err(error("window constant slot type differs"));
                }
                offset += 1;
            }
            if offset != expected.len() {
                return Err(error("environment has extra input fields"));
            }
        }
        Ok(())
    }
}
