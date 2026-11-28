//! Disposable P0 constant-fold registry experiment.

mod arrays;
mod binary;
mod casts;
mod comparison;
mod eval;
mod extract;
mod legacy;
mod maps;
mod math;
mod model;
mod numeric;
mod pattern;
mod profile;
mod rounding;
mod selection;
mod spatial;
mod strings;
mod temporal;
mod temporal_rounding;
mod uuid;

pub use math::trigonometric_kernel;
pub use model::{Expression, Refusal, SqlType};
pub use profile::SemanticsProfile;

/// Evaluate only explicitly registered pure operations, under a fixed node/depth
/// budget. A refusal leaves an optimization-only expression for the runtime.
/// Callers must not treat refusals at constant-required sites as successful folds.
pub fn evaluate(expression: &Expression) -> Result<serde_json::Value, Refusal> {
    evaluate_with_profile(expression, SemanticsProfile::Runtime)
}

/// Evaluate with an explicit experimental semantics profile. The default evaluator
/// above continues to follow runtime kernels.
pub fn evaluate_with_profile(
    expression: &Expression,
    profile: SemanticsProfile,
) -> Result<serde_json::Value, Refusal> {
    eval::Evaluator::new(profile)
        .evaluate(expression, 0)
        .map(|value| value.json())
}
