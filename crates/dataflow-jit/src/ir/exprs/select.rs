use crate::ir::ExprId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Selects between the two given values based on `cond`, roughly
/// equivalent to `if cond { if_true } else { if_false }`
///
/// - `cond` must be a boolean value
/// - `if_true` and `if_false` must be of the same type but otherwise have no
///   constraints upon their types, they can be scalar values or row values
/// - If `cond` is true then the value of the expression will be `if_true`, if
///   `cond` is false then the value of the expression will be `if_false`
/// - If the selected value is initialized then the value of the expression is
///   initialized regardless of the initialized-ness of the unselected value. In
///   other words, if `cond` is true and `if_true` is init, the initialized-ness
///   of `if_false` doesn't matter
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Select {
    cond: ExprId,
    if_true: ExprId,
    if_false: ExprId,
}

impl Select {
    /// Creates a new select instruction
    pub fn new(cond: ExprId, if_true: ExprId, if_false: ExprId) -> Self {
        Self {
            cond,
            if_true,
            if_false,
        }
    }

    pub const fn cond(&self) -> ExprId {
        self.cond
    }

    pub fn cond_mut(&mut self) -> &mut ExprId {
        &mut self.cond
    }

    pub const fn if_true(&self) -> ExprId {
        self.if_true
    }

    pub fn if_true_mut(&mut self) -> &mut ExprId {
        &mut self.if_true
    }

    pub const fn if_false(&self) -> ExprId {
        self.if_false
    }

    pub fn if_false_mut(&mut self) -> &mut ExprId {
        &mut self.if_false
    }
}
