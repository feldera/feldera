//! Shared input-reference objects and their separate evaluation environments.
//!
//! This is the input-binding part of the whole-program model. Producers remain
//! opaque IDs here; the relation adapter must establish their ordered row types.
use crate::{TypeId, TypeTable, ValidationError, ValidationLimits, types::TypeKind};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum InputUsesVersion {
    #[serde(rename = "input-uses-v1")]
    V1,
}

/// Identity in the input-reference object arena, independent of its uses.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct InputReferenceId(pub u32);

/// Identity in the evaluation-environment arena.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct InputEnvironmentId(pub u32);

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputReference {
    pub ty: TypeId,
    pub field: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum InputOwner {
    Relations {
        inputs: Vec<u32>,
    },
    Window {
        relation: u32,
        inputs: Vec<u32>,
    },
    Function {
        function: u32,
    },
    /// A statement value has no relational inputs or function parameters.
    Constant {},
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputEnvironment {
    pub owner: InputOwner,
    pub row: TypeId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputUse {
    pub object: InputReferenceId,
    pub environment: InputEnvironmentId,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InputUses {
    pub schema_version: InputUsesVersion,
    pub type_table: TypeTable,
    pub relations: u32,
    pub functions: u32,
    pub objects: Vec<InputReference>,
    pub environments: Vec<InputEnvironment>,
    /// One record per distinct object/environment pair, in first-use order.
    pub uses: Vec<InputUse>,
}

impl InputUses {
    /// Validate every recorded use, including uses of otherwise shared objects.
    /// `states` independently bounds the cross-product of objects and contexts.
    pub fn validate(&self, limits: ValidationLimits, states: usize) -> Result<(), ValidationError> {
        let fail = |message| ValidationError {
            path: "input_uses".into(),
            message,
        };
        if self.objects.len() > limits.types
            || self.environments.len() > limits.types
            || self.relations as usize > limits.types
            || self.functions as usize > limits.types
            || self.uses.len() > states
        {
            return Err(fail(
                "input object, environment or validation-state limit exceeded",
            ));
        }
        self.type_table.validate(limits)?;
        let mut owners = HashSet::new();
        let mut references = self
            .uses
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(self.objects.len()))
            .and_then(|n| n.checked_add(self.environments.len()))
            .ok_or_else(|| fail("input reference count overflow"))?;
        for environment in &self.environments {
            let inputs = match &environment.owner {
                InputOwner::Constant {} => &[],
                InputOwner::Relations { inputs } => inputs.as_slice(),
                InputOwner::Window { relation, inputs } => {
                    if *relation >= self.relations {
                        return Err(fail("window owner is outside relation arena"));
                    }
                    references += 1;
                    inputs.as_slice()
                }
                InputOwner::Function { function } => {
                    if *function >= self.functions {
                        return Err(fail("function owner is outside function arena"));
                    }
                    references += 1;
                    &[]
                }
            };
            references = references
                .checked_add(inputs.len())
                .ok_or_else(|| fail("input reference count overflow"))?;
            if references > limits.references {
                return Err(fail("input reference limit exceeded"));
            }
            if inputs.iter().any(|id| *id >= self.relations) {
                return Err(fail("input producer is outside relation arena"));
            }
            if !owners.insert(&environment.owner) {
                return Err(fail("duplicate input environment owner"));
            }
            self.type_table
                .reference(environment.row, "input environment row")?;
            if !matches!(
                self.type_table.types[environment.row.0 as usize].kind,
                TypeKind::Row { .. }
            ) {
                return Err(fail("input environment requires a row type"));
            }
            if matches!(environment.owner, InputOwner::Constant {})
                && matches!(&self.type_table.types[environment.row.0 as usize].kind,
                    TypeKind::Row { fields, .. } if !fields.is_empty())
            {
                return Err(fail("constant environment must have no fields"));
            }
        }
        if references > limits.references {
            return Err(fail("input reference limit exceeded"));
        }
        for object in &self.objects {
            self.type_table.reference(object.ty, "input object type")?;
        }
        let mut seen = HashSet::new();
        let mut used = vec![false; self.objects.len()];
        for usage in &self.uses {
            let object = self
                .objects
                .get(usage.object.0 as usize)
                .ok_or_else(|| fail("input object reference is outside arena"))?;
            let environment = self
                .environments
                .get(usage.environment.0 as usize)
                .ok_or_else(|| fail("input environment reference is outside arena"))?;
            if !seen.insert(*usage) {
                return Err(fail("duplicate object/environment validation state"));
            }
            used[usage.object.0 as usize] = true;
            let TypeKind::Row { fields, .. } =
                &self.type_table.types[environment.row.0 as usize].kind
            else {
                unreachable!()
            };
            let field = fields
                .get(object.field as usize)
                .ok_or_else(|| fail("input field is outside environment row"))?;
            if !self.type_table.reference_type_matches(field.ty, object.ty) {
                return Err(fail("input type differs from its evaluation environment"));
            }
        }
        if used.contains(&false) {
            return Err(fail("input object has no evaluation use"));
        }
        Ok(())
    }
}
