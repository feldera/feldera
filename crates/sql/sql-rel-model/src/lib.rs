//! Planner-independent contracts under development for the P0 S1 experiment.
//!
//! Deserialization checks the wire shape. Call [`program::Program::validate`]
//! for a whole program, or [`TypeTable::validate`] for a standalone type arena:
//! JSON Schema cannot check references, ownership, scopes or acyclicity.
//! These experimental contracts are not an accepted production S1 protocol.

pub mod catalog;
pub mod column_type;
pub mod compilation_context;
pub mod diagnostics;
pub mod expression_objects;
mod expression_validation;
pub mod expressions;
mod function_validation;
mod graph;
pub mod input_producers;
pub mod input_uses;
mod literal_validation;
pub mod literal_insert;
pub mod literals;
mod object_relation_validation;
mod object_validation;
pub mod operators;
pub mod program;
pub mod relation_metadata;
pub mod relation_properties;
pub mod search;
pub mod source;
pub mod statements;
mod typed_literals;
pub mod types;
mod validate;

pub use expression_validation::ExpressionLimits;
pub use literal_validation::LiteralLimits;
pub use object_validation::ObjectLimits;
pub use typed_literals::ValidatedTypes;
pub use types::{TypeId, TypeTable};
pub use validate::{ValidationError, ValidationLimits};
