//! Append the clock to a row for NOW expressions that require a stream join.
use crate::function_calls::{contains_now, function_name, is_clock_call};
use feldera_sql_expr::{Rewrite, Transformation, Visit, flatten_tuple_fields, parameter_paths};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind, OutputPort, TypeId,
};
use std::collections::HashSet;

fn parameter(edit: &mut CircuitEditor, ty: TypeId, next: &mut u64) -> Result<ExpressionId, Error> {
    let reference = edit.reference_type(ty)?;
    edit.variable(reference, next)
}

pub struct ClockJoinInputs {
    pub rows: OutputPort,
    pub row_type: TypeId,
    pub indexed_clock: OutputPort,
}

/// Index the input row on the empty key and join it to the indexed clock.
/// The result carries the timestamp as the final column.
pub fn append_clock_join(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    inputs: ClockJoinInputs,
    empty_key: TypeId,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<NodeId, Error> {
    let row = inputs.row_type;
    let [index_fn, join_fn] = clock_join_functions(edit, row, empty_key, timestamp, next)?;
    let indexed_type = edit.indexed_zset_type(row.into(), empty_key, row)?;
    let index = edit.append_operator(
        body,
        site,
        OperatorKind::MapIndex,
        &[inputs.rows],
        [
            ("outputType", Field::Reference(indexed_type.into())),
            ("function", Field::Reference(index_fn.into())),
        ],
    )?;
    let signature = edit.expression_type(join_fn)?;
    let result = edit.node(signature.into())?.edge("resultType")?;
    let fields = edit.node(result)?.edges("tupFields")?;
    // The join's declared row is a separate tuple sharing the closure's field types.
    let joined_row = edit.tuple_type(result, false, fields)?;
    let output = edit.zset_type(joined_row)?;
    edit.append_operator(
        body,
        site,
        OperatorKind::StreamJoin,
        &[
            OutputPort {
                operator: index,
                number: 0,
            },
            inputs.indexed_clock,
        ],
        [
            ("outputType", Field::Reference(output.into())),
            ("function", Field::Reference(join_fn.into())),
            ("balanced", Field::Data(false.into())),
        ],
    )
}

fn clock_join_functions(
    edit: &mut CircuitEditor,
    row: TypeId,
    empty_key: TypeId,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let input = parameter(edit, row, next)?;
    let value = edit.dereference(input)?;
    let fields = flatten_tuple_fields(edit, value)?;
    let key = edit.tuple_expr(input.into(), false, &[])?;
    let value = edit.tuple_expr(input.into(), false, &fields)?;
    let indexed = edit.tuple_expr(input.into(), true, &[key, value])?;
    let index = edit.closure_from_variables(indexed, &[input])?;

    let key = parameter(edit, empty_key, next)?;
    let left = parameter(edit, row, next)?;
    let clock_row = edit.tuple_type(timestamp.into(), false, [timestamp.into()])?;
    let right = parameter(edit, clock_row, next)?;
    let value = edit.dereference(left)?;
    let mut fields = flatten_tuple_fields(edit, value)?;
    let clock = edit.dereference(right)?;
    fields.push(edit.tuple_field(clock, 0)?);
    let joined = edit.tuple_expr(left.into(), false, &fields)?;
    Ok([
        index,
        edit.closure_from_variables(joined, &[key, left, right])?,
    ])
}

fn drop_clock(
    edit: &mut CircuitEditor,
    joined_row: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let row = parameter(edit, joined_row, next)?;
    let value = edit.dereference(row)?;
    let mut fields = flatten_tuple_fields(edit, value)?;
    fields
        .pop()
        .ok_or_else(|| Error::new("missing clock column on a temporal join"))?;
    let restored = edit.tuple_expr(row.into(), false, &fields)?;
    edit.closure_from_variables(restored, &[row])
}

/// A clock call in a nested lambda refers to a row outside that lambda's scope.
/// Return the original lambda so the caller can report its SQL location.
pub enum ClockRewrite<T> {
    Rewritten(T),
    UnsupportedLambda(ExpressionId),
}

struct JoinedClock {
    uses: HashSet<NodeId>,
    row: ExpressionId,
    clock: ExpressionId,
    unsupported: Option<ExpressionId>,
}

impl Transformation for JoinedClock {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = edit.node(id)?;
        if self.unsupported.is_some() || matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            return Ok(Visit::Keep);
        }
        if matches!(
            node.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) {
            if contains_now(edit.circuit(), &[node.edge("body")?])? {
                self.unsupported = Some(edit.expression(id)?);
            }
            return Ok(Visit::Keep);
        }
        if self.uses.contains(&id) {
            return Ok(Visit::Replace(self.row.into()));
        }
        if let Some(name) = function_name(edit.circuit(), node)? {
            if is_clock_call(&name, node.edges("arguments")?.len()) {
                return Ok(Visit::Replace(self.clock.into()));
            }
        }
        Ok(Visit::Children)
    }
}

/// Widen the input parameter by one timestamp and read NOW() from that column.
fn rewrite_clock_closure(
    edit: &mut CircuitEditor,
    closure: ExpressionId,
    timestamp: TypeId,
) -> Result<ClockRewrite<ExpressionId>, Error> {
    let node = edit.node(closure.into())?;
    let body = node.edge("body")?;
    let [parameter] = node
        .edges("parameters")?
        .as_slice()
        .try_into()
        .map_err(|_| Error::new("a clock join needs a unary closure"))?;
    let decl = edit.node(parameter)?;
    let name = decl
        .data("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::new("clock input parameter requires a name"))?
        .to_owned();
    let row = edit.type_id(edit.node(decl.edge("type")?)?.edge("type")?)?;
    let mut fields = edit.node(row.into())?.edges("tupFields")?;
    let clock_index = fields.len();
    fields.push(timestamp.into());
    let nullable = edit.nullable(row)?;
    let joined = edit.tuple_type(row.into(), false, fields)?;
    let joined = edit.with_nullability(joined, nullable)?;
    let reference = edit.reference_type(joined)?;
    let row = edit.named_variable(reference, &name)?;
    let clock_row = edit.named_variable(reference, &name)?;
    let clock_row = edit.dereference(clock_row)?;
    let clock = edit.tuple_field(clock_row, clock_index)?;
    let mut rewrite = Rewrite::new(JoinedClock {
        uses: parameter_paths(edit, body, parameter)?,
        row,
        clock,
        unsupported: None,
    });
    let body = rewrite.apply(edit, body)?;
    match rewrite.rule.unsupported {
        Some(lambda) => Ok(ClockRewrite::UnsupportedLambda(lambda)),
        None => Ok(ClockRewrite::Rewritten(
            edit.closure_from_variables(edit.expression(body)?, &[row])?,
        )),
    }
}

#[derive(Clone, Copy)]
pub enum ClockConsumer {
    Map,
    Filter,
}

pub struct ClockProjection {
    pub consumer: ClockConsumer,
    pub function: ExpressionId,
    pub output: TypeId,
}

/// A map reads NOW() from the widened row. A filter keeps the widened row
/// until its predicate has run, then projects away the extra timestamp.
pub fn apply_joined_clock(
    edit: &mut CircuitEditor,
    body: NodeId,
    join: NodeId,
    projection: ClockProjection,
    timestamp: TypeId,
    next: &mut u64,
) -> Result<ClockRewrite<NodeId>, Error> {
    let function = match rewrite_clock_closure(edit, projection.function, timestamp)? {
        ClockRewrite::Rewritten(function) => function,
        ClockRewrite::UnsupportedLambda(lambda) => {
            return Ok(ClockRewrite::UnsupportedLambda(lambda));
        }
    };
    let mut input = OutputPort {
        operator: join,
        number: 0,
    };
    let function = match projection.consumer {
        ClockConsumer::Map => function,
        ClockConsumer::Filter => {
            let joined_type = edit.node(join)?.edge("outputType")?;
            let joined_row = edit.type_id(edit.node(joined_type)?.edge("elementType")?)?;
            input.operator = edit.append_operator(
                body,
                join,
                OperatorKind::Filter,
                &[input],
                [
                    ("outputType", Field::Reference(joined_type)),
                    ("function", Field::Reference(function.into())),
                ],
            )?;
            drop_clock(edit, joined_row, next)?
        }
    };
    let result = edit.append_operator(
        body,
        join,
        OperatorKind::Map,
        &[input],
        [
            ("outputType", Field::Reference(projection.output.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    Ok(ClockRewrite::Rewritten(result))
}
