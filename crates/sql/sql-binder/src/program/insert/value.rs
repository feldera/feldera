//! A VALUES cell can be a literal or a constructor evaluated by a Project.

use super::super::{array::LiteralArray, literal::Cell, source::DeclarationTokens};
use crate::{Expr, Failure, Spanned};
use feldera_sql_rel_model::{
    TypeId, expression_objects::ObjectPosition, expressions::ExprId, program::Program,
};
use std::borrow::Cow;

pub(super) enum InsertValue {
    Scalar(Cell),
    Array(LiteralArray),
}

impl InsertValue {
    pub(super) fn parse(expr: &Expr) -> Result<Self, Failure> {
        match expr {
            Expr::Array(array) => LiteralArray::for_insert(array).map(Self::Array),
            _ => Cell::parse(expr).map(Self::Scalar),
        }
    }

    pub(super) fn literal(&self) -> Option<&Cell> {
        match self {
            Self::Scalar(cell) if !cell.needs_non_bmp_wrapper() => Some(cell),
            _ => None,
        }
    }

    pub(super) fn render(&self, quoted: bool) -> Cow<'_, str> {
        match self {
            Self::Scalar(cell) => cell.text.render(quoted),
            Self::Array(array) => Cow::Owned(array.insert_sql(quoted)),
        }
    }

    pub(super) fn emit(
        &self,
        program: &mut Program,
        tokens: &DeclarationTokens<'_>,
        expr: &Expr,
    ) -> Result<(ExprId, TypeId), Failure> {
        match self {
            Self::Scalar(cell) => cell.emit(program, tokens, expr),
            Self::Array(array) => array.emit(
                program,
                ObjectPosition::Present {
                    position: tokens.scalar_array_position(expr.span(), 0)?,
                    internal: Some(false),
                },
            ),
        }
    }
}
