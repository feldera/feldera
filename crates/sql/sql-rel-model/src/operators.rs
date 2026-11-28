//! Resolved operator objects, separate from each call's instantiated signature.
//!
//! Builtins resolve in trusted registries. Type constructors are declarative
//! objects: their qualified names and declared types are transported explicitly.
//! Object IDs preserve sharing without interning equal declarations by name.

use crate::{
    TypeId, TypeTable, ValidationError,
    expressions::{BindingId, ExprId, ScopeId},
    source::{Origin, SourceId},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct OperatorObjectId(pub u32);

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorObject {
    /// A separately allocated, stateless SUM0. The standard-table singleton
    /// continues to use Builtin; equal fresh objects must not be interned.
    SumZero {},
    Builtin {
        registry: String,
        registry_version: String,
        key: String,
    },
    TypeConstructor {
        name: QualifiedName,
        declared_type: TypeId,
    },
    RowConstructor {
        name: String,
        fields: RowFieldNames,
    },
    AggregateFunction {
        name: QualifiedName,
        return_type: TypeId,
        parameters: Vec<FunctionParameter>,
        linear: bool,
    },
    Function {
        name: String,
        return_type: TypeId,
        parameters: Vec<FunctionParameter>,
        generated: bool,
        origin: Origin,
        diagnostic: DiagnosticSource,
        body: FunctionBody,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionParameter {
    pub name: String,
    pub index: u32,
    pub ty: TypeId,
}

/// Text used in diagnostics, never an executable function body or SQL fallback.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticSource {
    pub origin: Origin,
    pub text: String,
    pub internal_text: String,
    pub empty: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FunctionBody {
    External {},
    /// Body in the whole-plan object graph. Its input/source environment is
    /// explicit and isolated from callers' lambda frames.
    ObjectSql {
        environment: crate::expression_objects::EnvironmentId,
        root: ExprId,
        source: FunctionSource,
    },
    Sql {
        scope: ScopeId,
        input: BindingId,
        root: ExprId,
        source: FunctionSource,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FunctionSource {
    Inherited {},
    Captured { source: SourceId },
}

/// A missing list means default names for every field. Null entries in an
/// explicit list request the default name for those individual fields.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RowFieldNames {
    Absent {},
    Present { names: Vec<Option<String>> },
}

/// Retain both the overall name position and the optional component positions.
/// An absent component-position list is distinct from a list of absent origins.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QualifiedName {
    pub names: Vec<String>,
    pub position: NamePosition,
    pub components: ComponentPositions,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NamePosition {
    pub origin: Origin,
    pub quoted: bool,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ComponentPositions {
    Absent {},
    Present { positions: Vec<NamePosition> },
}

/// Registry key used at S1 for Feldera's ABS function.
pub const ABS_KEY: &str =
    "org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.functions.CustomFunctions$AbsFunction#abs";

impl OperatorObject {
    pub fn array_constructor() -> Self {
        Self::Builtin {
            registry: "calcite-s1-fixture".into(),
            registry_version: "1".into(),
            key: "SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR".into(),
        }
    }

    pub fn non_bmp_literal() -> Self {
        Self::Builtin {
            registry: "feldera-s1-internals".into(),
            registry_version: "1".into(),
            key: "non-bmp-literal".into(),
        }
    }

    pub(crate) fn validate(&self, types: &TypeTable, path: &str) -> Result<(), ValidationError> {
        let invalid = |message| ValidationError {
            path: path.into(),
            message,
        };
        match self {
            Self::SumZero {} => {}
            Self::Builtin {
                registry,
                registry_version,
                key,
            } => {
                if registry.is_empty() || registry_version.is_empty() || key.is_empty() {
                    return Err(invalid("builtin identity must be nonempty"));
                }
            }
            Self::TypeConstructor {
                name,
                declared_type,
            } => {
                types.reference(*declared_type, path)?;
                name.validate(types, path)?;
            }
            Self::RowConstructor { name, .. } => {
                if name.is_empty() {
                    return Err(invalid("row constructor name must be nonempty"));
                }
            }
            Self::AggregateFunction {
                name,
                return_type,
                parameters,
                ..
            } => {
                name.validate(types, path)?;
                if name.names.len() != 1 {
                    return Err(invalid("aggregate requires a simple name"));
                }
                types.reference(*return_type, path)?;
                for parameter in parameters {
                    if parameter.name.is_empty() {
                        return Err(invalid("empty aggregate parameter name"));
                    }
                    types.reference(parameter.ty, path)?;
                }
            }
            Self::Function {
                name,
                return_type,
                parameters,
                origin,
                diagnostic,
                body,
                ..
            } => {
                if name.is_empty() {
                    return Err(invalid("function name must be nonempty"));
                }
                types.reference(*return_type, path)?;
                types.origin(origin, &format!("{path}.origin"))?;
                types.origin(&diagnostic.origin, &format!("{path}.diagnostic.origin"))?;
                for parameter in parameters {
                    if parameter.name.is_empty() {
                        return Err(invalid("function parameter name must be nonempty"));
                    }
                    types.reference(parameter.ty, path)?;
                }
                if let FunctionBody::Sql {
                    source: FunctionSource::Captured { source },
                    ..
                }
                | FunctionBody::ObjectSql {
                    source: FunctionSource::Captured { source },
                    ..
                } = body
                    && source.0 as usize >= types.sources.len()
                {
                    return Err(invalid("function body source is outside its arena"));
                }
            }
        }
        Ok(())
    }
}

impl QualifiedName {
    pub(crate) fn validate(&self, types: &TypeTable, path: &str) -> Result<(), ValidationError> {
        let name = self;
        let invalid = |message| ValidationError {
            path: path.into(),
            message,
        };
        if name.names.is_empty() || name.names.iter().any(String::is_empty) {
            return Err(invalid("constructor name must have nonempty components"));
        }
        types.origin(
            &name.position.origin,
            &format!("{path}.name.position.origin"),
        )?;
        if let ComponentPositions::Present { positions } = &name.components {
            if positions.len() != name.names.len() {
                return Err(invalid(
                    "constructor component positions differ from name arity",
                ));
            }
            for (index, position) in positions.iter().enumerate() {
                types.origin(
                    &position.origin,
                    &format!("{path}.name.components[{index}].origin"),
                )?;
            }
        }
        Ok(())
    }
}
