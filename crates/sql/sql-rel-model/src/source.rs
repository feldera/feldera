//! Source coordinates retain Calcite's one-based UTF-16 columns and inclusive
//! end positions. Generated ranges explicitly retain partially unknown, zero
//! components. Adapters must convert deliberately from byte-based parsers.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(transparent)]
pub struct SourceId(pub u32);

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    pub name: String,
    pub text: String,
}

#[derive(
    Clone, Copy, Debug, Deserialize, Eq, JsonSchema, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(deny_unknown_fields)]
pub struct Position {
    #[schemars(range(max = 2147483647))]
    pub line: u32,
    #[schemars(range(max = 2147483647))]
    pub column: u32,
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Origin {
    Absent {},
    Source {
        source: SourceId,
        start: Position,
        end: Position,
    },
    /// A compiler-derived range with partially unknown coordinates. Zero is an
    /// explicit unknown component here, not a one-based source coordinate.
    /// Retain the known components and source identity without inventing text
    /// locations. Fully unknown positions use Absent instead.
    Generated {
        source: SourceId,
        start: Position,
        end: Position,
    },
}

/// Retain the spelling and quoting independently. Case folding belongs to the
/// binder and must not be repeated during decoding.
#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Identifier {
    pub name: String,
    pub quoted: bool,
    pub origin: Origin,
}

impl Origin {
    /// The source is absent only for a completely unknown position.
    pub fn source(&self) -> Option<SourceId> {
        match self {
            Self::Absent {} => None,
            Self::Source { source, .. } | Self::Generated { source, .. } => Some(*source),
        }
    }
}
