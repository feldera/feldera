//! Disposable, separately implemented type-inference strategies for P0.
//!
//! These rules consume declared types, not SQL or Java oracle output. They do
//! not perform name resolution, constant folding, or runtime evaluation.
pub mod binding;
pub mod extras;
pub mod lowering;
pub mod model;
pub mod rewritten;
pub mod translated;

pub const FUNCTIONS: &[&str] = &[
    "ABS",
    "CEIL",
    "FLOOR",
    "SIGN",
    "SQRT",
    "CBRT",
    "EXP",
    "LN",
    "LOG10",
    "SIN",
    "COS",
    "TAN",
    "ASIN",
    "ACOS",
    "ATAN",
    "COT",
    "DEGREES",
    "RADIANS",
    "ROUND",
    "TRUNCATE",
    "LENGTH",
    "CHAR_LENGTH",
    "OCTET_LENGTH",
    "UPPER",
    "LOWER",
    "TRIM",
    "INITCAP",
    "MD5",
    "REVERSE",
    "ASCII",
    "POWER",
    "MOD",
    "LOG",
    "ATAN2",
    "CONCAT",
    "LEFT",
    "RIGHT",
    "REPEAT",
    "NULLIF",
    "COALESCE",
];

/// Arity sampled by the original manifest, not the complete SQL signature.
pub fn sampled_arity(name: &str) -> Option<usize> {
    FUNCTIONS
        .iter()
        .position(|function| *function == name)
        .map(|index| if index < 30 { 1 } else { 2 })
}
