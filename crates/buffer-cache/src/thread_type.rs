use enum_map::Enum;
use serde::Serialize;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Type of a DBSP worker thread that owns a buffer-cache slot.
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Enum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ThreadType {
    /// Circuit thread.
    Foreground,

    /// Merger thread.
    MergerTokio,
}

impl Display for ThreadType {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            ThreadType::Foreground => write!(f, "foreground"),
            ThreadType::MergerTokio => write!(f, "merger tokio"),
        }
    }
}
