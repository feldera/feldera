//! P0 native batch-code experiment, isolated from the production compiler.
//!
//! This bounded BIGINT subset exercises object emission, linking, loading and
//! nullable batch evaluation. It is not a SQL compiler or a DBSP row format.

mod archive;
mod codegen;
mod keys;
mod model;
mod runtime;
mod string_archive;
mod string_batch;
mod string_map;
mod string_projection;
mod string_range;

pub use archive::{ArchiveField, ArchiveInteger, ArchiveLayout, ArchivedBatch, CompiledArchive};
pub use keys::{KeyColumn, KeyField, KeyLayout, NormalizedKeys};
pub use model::{Arithmetic, Batch, Comparison, Expr, Program};
pub use runtime::CompiledProgram;
pub use string_map::CompiledNullableStringMap;
pub use string_projection::{CompiledStringProjection, StringSlices};
pub use string_range::CompiledStringRange;

/// Errors never cross the generated C ABI as Rust panics.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    InvalidProgram(String),
    InvalidBatch(String),
    InvalidKeyLayout(String),
    InvalidArchiveLayout(String),
    /// Zero-based index of the first row whose expression overflows.
    Overflow {
        row: usize,
        op: Arithmetic,
        left: i64,
        right: i64,
    },
    Compile(String),
    InvalidNativeResult,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProgram(reason) => write!(f, "invalid native program: {reason}"),
            Self::InvalidBatch(reason) => write!(f, "invalid native batch: {reason}"),
            Self::InvalidKeyLayout(reason) => write!(f, "invalid native key layout: {reason}"),
            Self::InvalidArchiveLayout(reason) => {
                write!(f, "invalid native archive layout: {reason}")
            }
            Self::Overflow {
                op, left, right, ..
            } => write!(f, "'{left} {op} {right}' causes overflow for type BIGINT"),
            Self::Compile(reason) => write!(f, "native compilation failed: {reason}"),
            Self::InvalidNativeResult => {
                f.write_str("generated code returned an invalid error record")
            }
        }
    }
}

impl std::error::Error for Error {}

pub use string_archive::{
    CompiledMixedArchive, CompiledStringArchive, StringArchiveField, StringArchiveLayout,
    StringArchives,
};
