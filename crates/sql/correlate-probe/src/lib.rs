//! A bounded port of Calcite's correlation value-generator and frame rules.
//!
//! The input is an independently validated FRel fragment. The output describes
//! relational constructors and explicit expression rewrites; Java only builds
//! those nodes through the S1 adapter. Unsupported forms fail the whole fragment.

pub mod recipe;
mod rewrite;

use feldera_sql_rel_model::{
    LiteralLimits, ObjectLimits, ValidationLimits, catalog::Catalog,
    expression_objects::ExpressionObjects, input_producers::InputProducers, input_uses::InputUses,
    relation_properties::RelationProperties,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Fragment {
    pub id: String,
    pub table: InputUses,
    pub producers: InputProducers,
    pub properties: RelationProperties,
    pub expressions: ExpressionObjects,
    pub catalog: Catalog,
}

#[derive(Debug)]
pub enum Error {
    Invalid(String),
    Unsupported(&'static str),
    Limit,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Invalid(message) => write!(f, "invalid FRel: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported: {message}"),
            Self::Limit => f.write_str("decorrelation work limit exceeded"),
        }
    }
}
impl std::error::Error for Error {}

/// Validate the complete fragment before accessing its arenas. Refusals never
/// carry a partial plan that could be mistaken for a successful decorrelation.
pub fn decorrelate(fragment: &Fragment) -> Result<recipe::Recipe, Error> {
    fragment
        .expressions
        .validate(
            &fragment.table,
            &fragment.producers,
            &fragment.properties,
            ValidationLimits::default(),
            LiteralLimits::default(),
            ObjectLimits::default(),
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
    fragment
        .catalog
        .validate(
            &fragment.table.type_table,
            &fragment.expressions,
            &fragment.producers,
            &fragment.properties,
            ValidationLimits::default(),
        )
        .map_err(|e| Error::Invalid(e.to_string()))?;
    rewrite::rewrite(fragment)
}
