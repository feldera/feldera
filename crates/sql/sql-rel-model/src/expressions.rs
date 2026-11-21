//! Draft resolved expressions. IDs refer to ordered arenas, never display names.
//!
//! Each expression records its evaluation scope. Input rows, correlations and
//! lambda parameters have distinct binding kinds, so a decoder cannot silently
//! turn an outer reference into a local input. Transient planner forms are not
//! part of this lowering fragment and must be rejected by its adapters.

use crate::{
    TypeId, TypeTable,
    literals::Literal,
    operators::{OperatorObject, OperatorObjectId},
    source::Origin,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ExprId(pub u32);

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct ScopeId(pub u32);

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct BindingId(pub u32);

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OperatorId(pub u32);

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ExpressionSchemaVersion {
    #[serde(rename = "expressions-v4")]
    V4,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionTable {
    pub schema_version: ExpressionSchemaVersion,
    pub type_table: TypeTable,
    pub operator_objects: Vec<OperatorObject>,
    pub operators: Vec<ResolvedOperator>,
    pub bindings: Vec<Binding>,
    pub scopes: Vec<Scope>,
    pub expressions: Vec<Expression>,
    pub roots: Vec<ExprId>,
}

/// One instantiated signature references a resolved operator object. Different
/// signatures may share that object. The model checks references and signatures,
/// not builtin registry membership or independent SQL type inference.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResolvedOperator {
    pub object: OperatorObjectId,
    pub parameters: Vec<TypeId>,
    pub result: TypeId,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BindingKind {
    Input,
    Correlation,
    LambdaParameter,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub kind: BindingKind,
    pub ty: TypeId,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Scope {
    /// Every binding belongs to exactly one scope. Parents supply lexical
    /// visibility; ordinary input bindings are usable only in their own scope.
    #[serde(deserialize_with = "required_parent")]
    #[schemars(required, schema_with = "parent_schema")]
    pub parent: Option<ScopeId>,
    pub bindings: Vec<BindingId>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Expression {
    pub ty: TypeId,
    pub scope: ScopeId,
    pub origin: Origin,
    pub kind: ExprKind,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LambdaParameter {
    /// Preserve whether the declaration and a body reference share one node.
    pub declaration: ExprId,
    pub binding: BindingId,
    pub name: String,
    pub index: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ExprKind {
    InputRef {
        binding: BindingId,
        field: u32,
    },
    Correlation {
        binding: BindingId,
    },
    /// Reuse the standalone literal contract. Its type and origin must equal
    /// the expression header, including exact arena IDs.
    Literal {
        literal: Literal,
    },
    FieldAccess {
        input: ExprId,
        field: u32,
    },
    Call {
        operator: OperatorId,
        arguments: Vec<ExprId>,
    },
    /// Aggregate invocation owned by a window group. Its evaluation depends on
    /// that group even when it has no ordinary expression arguments.
    WindowAggregate {
        operator: OperatorId,
        arguments: Vec<ExprId>,
        ordinal: u32,
        distinct: bool,
        ignore_nulls: bool,
    },
    LambdaRef {
        binding: BindingId,
    },
    Lambda {
        parameters: Vec<LambdaParameter>,
        body_scope: ScopeId,
        body: ExprId,
    },
}

impl ExprKind {
    pub(crate) fn call(&self) -> Option<(OperatorId, &[ExprId])> {
        match self {
            Self::Call {
                operator,
                arguments,
            }
            | Self::WindowAggregate {
                operator,
                arguments,
                ..
            } => Some((*operator, arguments)),
            _ => None,
        }
    }

    pub(crate) fn children(&self) -> impl Iterator<Item = ExprId> + '_ {
        let (ordinary, parameters): (&[ExprId], &[LambdaParameter]) = match self {
            Self::FieldAccess { input, .. } => (std::slice::from_ref(input), &[]),
            Self::Call { arguments, .. } | Self::WindowAggregate { arguments, .. } => {
                (arguments, &[])
            }
            Self::Lambda {
                body, parameters, ..
            } => (std::slice::from_ref(body), parameters),
            Self::InputRef { .. }
            | Self::Correlation { .. }
            | Self::Literal { .. }
            | Self::LambdaRef { .. } => (&[], &[]),
        };
        parameters
            .iter()
            .map(|parameter| parameter.declaration)
            .chain(ordinary.iter().copied())
    }
}

// A root spells its parent as null. Serde otherwise treats a missing Option
// field as None, hiding a truncated or outdated wire object.
fn required_parent<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<ScopeId>, D::Error> {
    Option::<ScopeId>::deserialize(deserializer)
}

fn parent_schema(generator: &mut schemars::r#gen::SchemaGenerator) -> schemars::schema::Schema {
    <Option<ScopeId>>::json_schema(generator)
}
