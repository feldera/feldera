//! Compute temporal window bounds from the clock stream and the SQL key domain.
use crate::function_calls::{function_name, is_clock_call};
use feldera_sql_expr::{Rewrite, Transformation, Visit, copy_expression};
use feldera_sql_ir::{
    BinaryOp, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    TypeId,
};

struct ReplaceClock(ExpressionId);

impl Transformation for ReplaceClock {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            return Ok(Visit::Keep);
        }
        if let Some(name) = function_name(edit.circuit(), node)? {
            if is_clock_call(&name, node.edges("arguments")?.len()) {
                return Ok(Visit::Replace(self.0.into()));
            }
        }
        Ok(Visit::Children)
    }
}

pub struct WindowEndpoint {
    pub expression: Option<ExpressionId>,
    /// A missing lower bound uses the domain minimum. A missing upper bound uses its maximum.
    /// An explicit bound uses the domain minimum until the clock becomes known.
    pub initial: ExpressionId,
}

impl WindowEndpoint {
    fn boxed(
        &self,
        edit: &mut CircuitEditor,
        site: NodeId,
        clock: ExpressionId,
        known: ExpressionId,
        key_type: TypeId,
    ) -> Result<ExpressionId, Error> {
        let bound = if let Some(expression) = self.expression {
            let value = Rewrite::new(ReplaceClock(clock))
                .apply(edit, expression.into())?;
            let value = edit.cast(value, edit.expression(value)?, key_type)?;
            edit.make_expr(
                site,
                Expr::If,
                edit.expression_type(value)?,
                [
                    ("condition", Field::Reference(known.into())),
                    ("positive", Field::Reference(value.into())),
                    ("negative", Field::Reference(self.initial.into())),
                ],
            )?
        } else {
            self.initial
        };
        let boxed = edit.dynamic_box_type(edit.expression_type(bound)?)?;
        edit.make_expr(
            bound.into(),
            Expr::Unary,
            boxed,
            [
                ("source", Field::Reference(bound.into())),
                ("opcode", Field::Data("TYPEDBOX".into())),
            ],
        )
    }
}

pub fn temporal_window_bounds(
    edit: &mut CircuitEditor,
    key_type: TypeId,
    clock_minimum: ExpressionId,
    lower: WindowEndpoint,
    upper: WindowEndpoint,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let timestamp = edit.expression_type(clock_minimum)?;
    let row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let reference = edit.reference_type(row)?;
    let var = edit.variable(reference, next)?;
    let read_clock = |edit: &mut CircuitEditor| {
        let row = edit.dereference(var)?;
        edit.tuple_field(row, 0)
    };
    let clock = read_clock(edit)?;
    let test = read_clock(edit)?;
    let boolean = edit.bool_type(clock_minimum.into(), false)?;
    let known = edit.binary_expression(var.into(), boolean, BinaryOp::Neq, test, clock_minimum)?;

    let lower_site = lower.expression.map(NodeId::from);
    let upper_site = upper.expression.map(NodeId::from);
    let site = match (lower_site, upper_site) {
        (Some(left), Some(right)) => {
            let location = edit
                .node(left)?
                .field("position")
                .map(Field::source_range)
                .transpose()?;
            if location.is_some_and(|range| range.is_valid()) {
                left
            } else {
                right
            }
        }
        (Some(site), None) | (None, Some(site)) => site,
        (None, None) => return Err(Error::new("a temporal window needs at least one bound")),
    };
    let lower = lower.boxed(edit, lower_site.unwrap_or(site), clock, known, key_type)?;
    let copied = if upper.expression.is_some() {
        let copy = copy_expression(edit, known.into())?;
        edit.expression(copy)?
    } else {
        known
    };
    let upper = upper.boxed(edit, site, clock, copied, key_type)?;
    let pair = edit.tuple_type(
        site,
        true,
        [
            edit.expression_type(lower)?.into(),
            edit.expression_type(upper)?.into(),
        ],
    )?;
    let body = edit.tuple_with_type_at(site, pair, &[lower, upper])?;
    let closure = edit.closure_from_variables(body, &[var])?;
    let decls = edit
        .node(closure.into())?
        .field("parameters")
        .expect("constructed closure")
        .clone();
    edit.make_expr(
        site,
        Expr::Closure,
        edit.expression_type(closure)?,
        [
            ("parameters", decls),
            ("body", Field::Reference(body.into())),
        ],
    )
}
