//! Whole-plan expression identities, separate from evaluation environments.
//!
//! A shared input or lambda reference retains its intrinsic ordinal/name. Each
//! use resolves it in its own input and lexical context. Source text likewise
//! belongs to the use; parser coordinates belong to the object.
use crate::{
    TypeId,
    expressions::{ExprId, ResolvedOperator},
    input_uses::{InputEnvironmentId, InputReferenceId},
    literals::Literal,
    operators::OperatorObject,
    relation_metadata::Coordinates,
    source::SourceId,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum ObjectsVersion {
    #[serde(rename = "expression-objects-v2")]
    V2,
}
#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct EnvironmentId(pub u32);

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionObjects {
    pub schema_version: ObjectsVersion,
    pub operator_objects: Vec<OperatorObject>,
    pub operators: Vec<ResolvedOperator>,
    pub expressions: Vec<ExpressionObject>,
    pub environments: Vec<Environment>,
    pub relation_uses: Vec<RelationUse>,
    pub value_uses: Vec<ValueUse>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Environment {
    pub input: InputEnvironmentId,
    pub source: SourceContext,
    pub correlations: Vec<Correlation>,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SourceContext {
    Absent {},
    Present { source: SourceId },
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Correlation {
    pub identity: u32,
    pub row: TypeId,
}

/// Several uses of a shared relation may supply different source/correlation
/// environments. Its local expression object IDs remain identical in each use.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RelationUse {
    pub relation: u32,
    pub environment: EnvironmentId,
    pub roots: Vec<ExprId>,
}

/// Ordered values owned by a statement or catalog declaration. Their ownership
/// and slot meanings are checked by the enclosing program adapter.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValueUse {
    pub environment: EnvironmentId,
    pub roots: Vec<ExprId>,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExpressionObject {
    pub ty: TypeId,
    pub position: ObjectPosition,
    pub kind: ObjectKind,
}
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectPosition {
    Absent {},
    Present {
        position: Coordinates,
        #[serde(deserialize_with = "Option::deserialize")]
        #[schemars(required, schema_with = "crate::catalog::nullable_schema::<bool>")]
        internal: Option<bool>,
    },
}

impl ObjectPosition {
    pub fn user(position: Coordinates) -> Self {
        Self::Present {
            position,
            internal: Some(false),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectKind {
    InputRef {
        object: InputReferenceId,
    },
    Correlation {
        identity: u32,
    },
    Literal {
        literal: Literal,
    },
    SearchArgument {
        null_as: crate::search::UnknownAs,
        ranges: Vec<crate::search::SearchRange>,
    },
    FieldAccess {
        input: ExprId,
        field: u32,
    },
    Call {
        operator: crate::expressions::OperatorId,
        arguments: Vec<ExprId>,
    },
    WindowAggregate {
        operator: crate::expressions::OperatorId,
        arguments: Vec<ExprId>,
        ordinal: u32,
        distinct: bool,
        ignore_nulls: bool,
    },
    LambdaRef {
        index: u32,
        name: String,
    },
    Lambda {
        parameters: Vec<ExprId>,
        body: ExprId,
    },
}

impl ObjectKind {
    pub(crate) fn call(&self) -> Option<(crate::expressions::OperatorId, &[ExprId])> {
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
        let (first, last): (&[ExprId], &[ExprId]) = match self {
            Self::Call { arguments, .. } | Self::WindowAggregate { arguments, .. } => {
                (arguments, &[])
            }
            Self::FieldAccess { input, .. } => (std::slice::from_ref(input), &[]),
            Self::Lambda { parameters, body } => (parameters, std::slice::from_ref(body)),
            _ => (&[], &[]),
        };
        first.iter().chain(last).copied()
    }
}
