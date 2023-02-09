use crate::ir::{BlockId, RValue};
use derive_more::From;

/// A block terminator
///
/// The final instruction within a given basic block must be one that continues
/// control flow somehow, either through branching to another block
/// (conditionally or otherwise), by returning from the function or otherwise
/// changing control flow
#[derive(Debug, Clone, From, PartialEq)]
pub enum Terminator {
    Jump(Jump),
    Branch(Branch),
    Return(Return),
    // TODO: Switch
    // TODO: Unreachable could be useful in the future
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
#[derive(Debug, Clone, PartialEq)]
pub struct Jump {
    target: BlockId,
}

impl Jump {
    /// Creates a new jump terminator
    pub fn new(target: BlockId) -> Self {
        Self { target }
    }

    /// Returns the target of the jump
    pub const fn target(&self) -> BlockId {
        self.target
    }
}

/// A conditional branch instruction, chooses between one of two blocks to jump
/// to based off of `cond`
///
/// If `cond` is true, this instruction will jump to the `truthy` block
/// If `cond` is false, this instruction will jump to the `falsy` block
#[derive(Debug, Clone, PartialEq)]
pub struct Branch {
    /// The condition this branch decides upon
    cond: RValue,
    /// The block jumped to if `cond` is true
    truthy: BlockId,
    /// The block jumped to if `cond` is false
    falsy: BlockId,
}

impl Branch {
    /// Create a new branch terminator
    pub fn new(cond: RValue, truthy: BlockId, falsy: BlockId) -> Self {
        Self {
            cond,
            truthy,
            falsy,
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
}

/// Returns a value from the current function
#[derive(Debug, Clone, PartialEq)]
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
