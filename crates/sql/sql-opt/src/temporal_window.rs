//! Build the functions of one SQL window from its compatible temporal comparisons.
use crate::{WindowEndpoint, temporal_window_bounds, window_projection};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId, TypeId,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum WindowComparison {
    #[serde(rename = "GT")]
    Greater,
    #[serde(rename = "GTE")]
    GreaterEqual,
    #[serde(rename = "LT")]
    Less,
    #[serde(rename = "LTE")]
    LessEqual,
}

#[derive(Default)]
struct Endpoint {
    bound: Option<(ExpressionId, bool)>,
}

impl Endpoint {
    fn tighten(
        &mut self,
        edit: &mut CircuitEditor,
        value: ExpressionId,
        inclusive: bool,
        operation: BinaryOp,
    ) -> Result<(), Error> {
        let value = if let Some((previous, was_inclusive)) = self.bound {
            if was_inclusive != inclusive {
                return Err(Error::new(
                    "window bounds on the same side must agree on inclusivity",
                ));
            }
            edit.binary_expression(
                previous.into(),
                edit.expression_type(previous)?,
                operation,
                previous,
                value,
            )?
        } else {
            value
        };
        self.bound = Some((value, inclusive));
        Ok(())
    }

    fn inclusive(&self) -> bool {
        self.bound.is_none_or(|(_, inclusive)| inclusive)
    }

    fn endpoint(&self, initial: ExpressionId) -> WindowEndpoint {
        WindowEndpoint {
            expression: self.bound.map(|(value, _)| value),
            initial,
        }
    }
}

pub struct WindowKey {
    pub parameter: NodeId,
    pub row: TypeId,
    pub expression: ExpressionId,
    pub nonnull_type: TypeId,
}

pub struct InitialBounds {
    pub minimum_clock: ExpressionId,
    pub lower: ExpressionId,
    pub upper: ExpressionId,
}

pub struct WindowFunctions {
    pub bounds: ExpressionId,
    pub nonnull: Option<ExpressionId>,
    pub index: ExpressionId,
    pub restore: Option<ExpressionId>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
    pub lower_unbounded: bool,
}

pub fn temporal_window(
    edit: &mut CircuitEditor,
    key: WindowKey,
    initial: InitialBounds,
    comparisons: impl IntoIterator<Item = (WindowComparison, ExpressionId)>,
    next: &mut u64,
) -> Result<WindowFunctions, Error> {
    let mut lower = Endpoint::default();
    let mut upper = Endpoint::default();
    for (comparison, value) in comparisons {
        use WindowComparison::*;
        match comparison {
            Greater | GreaterEqual => lower.tighten(
                edit,
                value,
                matches!(comparison, GreaterEqual),
                BinaryOp::Max,
            )?,
            Less | LessEqual => {
                upper.tighten(edit, value, matches!(comparison, LessEqual), BinaryOp::Min)?
            }
        }
    }
    let bounds = temporal_window_bounds(
        edit,
        key.nonnull_type,
        initial.minimum_clock,
        lower.endpoint(initial.lower),
        upper.endpoint(initial.upper),
        next,
    )?;
    let nonnull = if edit.nullable(edit.expression_type(key.expression)?)? {
        let test = edit.null_test(key.expression)?;
        let predicate = edit.make_expr(
            test.into(),
            ExpressionKind::Unary,
            edit.expression_type(test)?,
            [
                ("source", Field::Reference(test.into())),
                ("opcode", Field::Data("NOT".into())),
            ],
        )?;
        Some(edit.closure_with_parameters(predicate, &[key.parameter])?)
    } else {
        None
    };
    let projection = window_projection(
        edit,
        key.parameter,
        key.row,
        key.expression,
        key.nonnull_type,
        next,
    )?;
    Ok(WindowFunctions {
        bounds,
        nonnull,
        index: projection.index,
        restore: projection.restore,
        lower_inclusive: lower.inclusive(),
        upper_inclusive: upper.inclusive(),
        lower_unbounded: lower.bound.is_none(),
    })
}
