use crate::ir::{BlockId, ExprId, RValue};
use derive_more::From;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A block terminator
///
/// The final instruction within a given basic block must be one that continues
/// control flow somehow, either through branching to another block
/// (conditionally or otherwise), by returning from the function or otherwise
/// changing control flow
#[derive(Debug, Clone, From, PartialEq, Deserialize, Serialize, JsonSchema)]
pub enum Terminator {
    Jump(Jump),
    Branch(Branch),
    Return(Return),
    Unreachable,
    // TODO: Switch
}

impl Terminator {
    /// Returns true if the current terminator is a [`Jump`]
    #[must_use]
    pub const fn is_jump(&self) -> bool {
        matches!(self, Self::Jump(_))
    }

    /// Returns true if the current terminator is a [`Branch`]
    #[must_use]
    pub const fn is_branch(&self) -> bool {
        matches!(self, Self::Branch(_))
    }

    /// Returns true if the current terminator is a [`Return`]
    #[must_use]
    pub const fn is_return(&self) -> bool {
        matches!(self, Self::Return(_))
    }

    /// Returns `true` if the terminator is [`Unreachable`].
    ///
    /// [`Unreachable`]: Terminator::Unreachable
    #[must_use]
    pub const fn is_unreachable(&self) -> bool {
        matches!(self, Self::Unreachable)
    }

    #[must_use]
    pub const fn as_jump(&self) -> Option<&Jump> {
        if let Self::Jump(jump) = self {
            Some(jump)
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_jump_mut(&mut self) -> Option<&mut Jump> {
        if let Self::Jump(jump) = self {
            Some(jump)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_branch(&self) -> Option<&Branch> {
        if let Self::Branch(branch) = self {
            Some(branch)
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_branch_mut(&mut self) -> Option<&mut Branch> {
        if let Self::Branch(branch) = self {
            Some(branch)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn as_return(&self) -> Option<&Return> {
        if let Self::Return(ret) = self {
            Some(ret)
        } else {
            None
        }
    }

    #[must_use]
    pub fn as_return_mut(&mut self) -> Option<&mut Return> {
        if let Self::Return(ret) = self {
            Some(ret)
        } else {
            None
        }
    }
}

/// An unconditional branch instruction
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Jump {
    /// The block being jumped to
    target: BlockId,
    /// The parameters being passed to the target block by the jump
    params: Vec<ExprId>,
}

impl Jump {
    /// Creates a new jump terminator
    pub fn new(target: BlockId, params: Vec<ExprId>) -> Self {
        Self { target, params }
    }

    /// Returns the target of the jump
    pub const fn target(&self) -> BlockId {
        self.target
    }

    /// Returns the parameters passed by the jump
    pub fn params(&self) -> &[ExprId] {
        &self.params
    }

    /// Returns a mutable reference to the parameters passed by the jump
    pub fn params_mut(&mut self) -> &mut [ExprId] {
        &mut self.params
    }
}

/// A conditional branch instruction, chooses between one of two blocks to jump
/// to based off of `cond`
///
/// If `cond` is true, this instruction will jump to the `truthy` block
/// If `cond` is false, this instruction will jump to the `falsy` block
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Branch {
    /// The condition this branch decides upon
    cond: RValue,
    /// The block jumped to if `cond` is true
    truthy: BlockId,
    /// Parameters for the true block
    true_params: Vec<ExprId>,
    /// The block jumped to if `cond` is false
    falsy: BlockId,
    /// Parameters for the false block
    false_params: Vec<ExprId>,
}

impl Branch {
    /// Create a new branch terminator
    pub fn new(
        cond: RValue,
        truthy: BlockId,
        true_params: Vec<ExprId>,
        falsy: BlockId,
        false_params: Vec<ExprId>,
    ) -> Self {
        Self {
            cond,
            truthy,
            true_params,
            falsy,
            false_params,
        }
    }

    /// Gets the condition the branch is determined by
    pub const fn cond(&self) -> &RValue {
        &self.cond
    }

    /// Gets a mutable reference to the condition the branch is determined by
    pub fn cond_mut(&mut self) -> &mut RValue {
        &mut self.cond
    }

    /// Returns the block that will be jumped to if the condition is true
    pub const fn truthy(&self) -> BlockId {
        self.truthy
    }

    /// Returns the block that will be jumped to if the condition is false
    pub const fn falsy(&self) -> BlockId {
        self.falsy
    }

    /// Returns true if both branches lead to the same block
    pub fn targets_are_identical(&self) -> bool {
        self.truthy == self.falsy
    }

    pub fn true_params(&self) -> &[ExprId] {
        &self.true_params
    }

    pub fn false_params(&self) -> &[ExprId] {
        &self.false_params
    }

    pub fn true_params_mut(&mut self) -> &mut Vec<ExprId> {
        &mut self.true_params
    }

    pub fn false_params_mut(&mut self) -> &mut Vec<ExprId> {
        &mut self.false_params
    }
}

/// Returns a value from the current function
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize, JsonSchema)]
pub struct Return {
    value: RValue,
}

impl Return {
    /// Creates a new `Return` terminator
    pub fn new(value: RValue) -> Self {
        Self { value }
    }

    /// Gets the value this instruction will return
    pub const fn value(&self) -> &RValue {
        &self.value
    }

    /// Gets a mutable reference to the value this instruction will return
    pub fn value_mut(&mut self) -> &mut RValue {
        &mut self.value
    }
}
