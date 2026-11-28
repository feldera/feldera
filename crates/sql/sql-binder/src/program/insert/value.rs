//! A VALUES cell can be a literal or a constructor evaluated by a Project.

use super::super::{graph, literal::Cell, source::DeclarationTokens};
use crate::{unsupported, Expr, Failure, Spanned};
use feldera_sql_rel_model::{
    expression_objects::ObjectPosition,
    expressions::ExprId,
    literals::LiteralValue,
    operators::OperatorObject,
    program::Program,
    source::Origin,
    types::{Cardinality, SqlType, TypeKind},
    TypeId,
};
use std::borrow::Cow;

pub(super) enum InsertValue {
    Scalar(Cell),
    Array(Vec<Cell>),
}

impl InsertValue {
    pub(super) fn parse(expr: &Expr) -> Result<Self, Failure> {
        let Expr::Array(array) = expr else {
            return Cell::parse(expr).map(Self::Scalar);
        };
        let elements = array
            .elem
            .iter()
            .map(Cell::parse)
            .collect::<Result<Vec<_>, _>>()?;
        let Some(first) = elements.first() else {
            return Err(unsupported("empty ARRAY requires an inferred element type"));
        };
        if elements.iter().any(|c| {
            c.kind != first.kind
                || matches!(c.value, LiteralValue::Null {})
                || c.needs_non_bmp_wrapper()
        }) {
            return Err(unsupported("ARRAY elements require coercion"));
        }
        Ok(Self::Array(elements))
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
            Self::Array(elements) => {
                let fields: Vec<_> = elements
                    .iter()
                    .map(|cell| cell.text.render(quoted))
                    .collect();
                let sql = format!("ARRAY[{}]", fields.join(", "));
                Cow::Owned(if quoted { sql } else { format!("({sql})") })
            }
        }
    }

    pub(super) fn emit(
        &self,
        program: &mut Program,
        tokens: &DeclarationTokens<'_>,
        expr: &Expr,
    ) -> Result<(ExprId, TypeId), Failure> {
        let elements = match self {
            Self::Array(elements) => elements,
            Self::Scalar(cell) => return cell.emit(program, tokens, expr),
        };
        let element = graph::insert_type(
            &mut program.table.type_table,
            SqlType {
                nullable: false,
                origin: Origin::Absent {},
                kind: elements[0].kind.clone(),
            },
        );
        let result = graph::insert_type(
            &mut program.table.type_table,
            SqlType {
                nullable: false,
                origin: Origin::Absent {},
                kind: TypeKind::Array {
                    element,
                    cardinality: Cardinality::Unbounded,
                },
            },
        );
        let arguments = elements
            .iter()
            .map(|c| graph::append_literal(program, element, c.value.clone()))
            .collect();
        let position = ObjectPosition::Present {
            position: tokens.scalar_array_position(expr.span())?,
            internal: Some(false),
        };
        Ok((
            graph::append_constant_call(
                program,
                OperatorObject::array_constructor(),
                arguments,
                result,
                position,
            ),
            result,
        ))
    }
}
