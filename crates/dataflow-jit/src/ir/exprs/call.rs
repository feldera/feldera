use crate::ir::{ColumnType, ExprId, LayoutId};
use serde::{Deserialize, Serialize};

/// The type of a function argument
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum ArgType {
    /// A row type
    Row(LayoutId),
    /// A scalar value's type
    Scalar(ColumnType),
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Call {
    /// The name of the function being called
    function: String,
    /// The arguments passed to the function
    args: Vec<ExprId>,
    /// The types of each argument
    arg_types: Vec<ArgType>,
    /// The function's return type
    // FIXME: Could return a row type as well
    ret_ty: ColumnType,
}

impl Call {
    pub fn new(
        function: String,
        args: Vec<ExprId>,
        arg_types: Vec<ArgType>,
        ret_ty: ColumnType,
    ) -> Self {
        Self {
            function,
            args,
            arg_types,
            ret_ty,
        }
    }

    pub fn function(&self) -> &str {
        &self.function
    }

    pub fn args(&self) -> &[ExprId] {
        &self.args
    }

    pub fn args_mut(&mut self) -> &mut Vec<ExprId> {
        &mut self.args
    }

    pub fn arg_types(&self) -> &[ArgType] {
        &self.arg_types
    }

    pub fn arg_types_mut(&mut self) -> &mut Vec<ArgType> {
        &mut self.arg_types
    }

    pub const fn ret_ty(&self) -> ColumnType {
        self.ret_ty
    }
}
