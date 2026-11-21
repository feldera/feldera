//! Compute expensive, time-independent filter operands in an upstream map.
use crate::{fanout::expensive, function_calls::contains_now, terms::Terms};
use feldera_sql_expr::{reduce_closure_body, simplify_closure_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op, OutputPort,
};
use std::collections::HashMap;

/// A connective recurses into its predicates. A comparison or null test considers
/// each immediate operand as one computation, even if it contains a comparison.
enum Predicate {
    Operand(NodeId, Option<usize>),
    Operation(NodeId, Vec<(&'static str, Predicate)>),
}

#[derive(Default)]
struct Hoisted {
    values: Vec<NodeId>,
    indexes: HashMap<usize, usize>,
    terms: Terms,
}

impl Hoisted {
    fn operand(
        &mut self,
        edit: &CircuitEditor,
        id: NodeId,
        scope: &[String],
    ) -> Result<Predicate, Error> {
        let mut value = edit.expression(id)?;
        while edit.expr_kind(value)? == Expr::Clone {
            value = edit.expression(edit.node(value.into())?.edge("expression")?)?;
        }
        let kind = edit.expr_kind(value)?;
        let trivial = kind.wire_name().ends_with("Literal")
            || (kind == Expr::Field
                && edit
                    .expr_kind(edit.expression(edit.node(value.into())?.edge("expression")?)?)?
                    == Expr::Deref);
        let index = if !trivial
            && expensive(edit.circuit(), id)?
            && !contains_now(edit.circuit(), &[id])?
        {
            let shape = self
                .terms
                .field(edit.circuit(), &Field::Reference(id), scope);
            Some(*self.indexes.entry(shape).or_insert_with(|| {
                self.values.push(id);
                self.values.len() - 1
            }))
        } else {
            None
        };
        Ok(Predicate::Operand(id, index))
    }

    fn collect(
        &mut self,
        edit: &CircuitEditor,
        id: NodeId,
        scope: &[String],
        depth: usize,
    ) -> Result<Predicate, Error> {
        if depth > 128 {
            return Err(Error::new("filter exceeds the expression nesting limit"));
        }
        let node = edit.node(id)?;
        let op = node.data("opcode").and_then(|v| v.as_str());
        let (children, recurse): (&[&str], _) = match (edit.expr_kind(edit.expression(id)?)?, op) {
            (Expr::Binary, Some("AND" | "OR")) => (&["left", "right"], true),
            (Expr::Binary, Some("EQ" | "NEQ" | "LT" | "GT" | "LTE" | "GTE" | "IS_DISTINCT")) => {
                (&["left", "right"], false)
            }
            (
                Expr::Unary,
                Some("NOT" | "WRAP_BOOL" | "IS_TRUE" | "IS_FALSE" | "IS_NOT_TRUE" | "IS_NOT_FALSE"),
            ) => (&["source"], true),
            (Expr::IsNull, _) => (&["expression"], false),
            _ => return self.operand(edit, id, scope),
        };
        let children = children
            .iter()
            .map(|&field| {
                let child = node.edge(field)?;
                let value = if recurse {
                    self.collect(edit, child, scope, depth + 1)?
                } else {
                    self.operand(edit, child, scope)?
                };
                Ok((field, value))
            })
            .collect::<Result<_, Error>>()?;
        Ok(Predicate::Operation(id, children))
    }
}

fn row_field(
    edit: &mut CircuitEditor,
    row: ExpressionId,
    index: usize,
) -> Result<ExpressionId, Error> {
    let tuple = edit.dereference(row)?;
    let field = edit.tuple_field(tuple, index)?;
    edit.clone_if_needed(field)
}

impl Predicate {
    fn rewrite(
        &self,
        edit: &mut CircuitEditor,
        row: ExpressionId,
        base: usize,
    ) -> Result<NodeId, Error> {
        match self {
            Self::Operand(id, None) => Ok(*id),
            Self::Operand(_, Some(index)) => row_field(edit, row, base + index).map(Into::into),
            Self::Operation(id, children) => {
                let fields = children
                    .iter()
                    .map(|(field, child)| {
                        Ok((*field, Field::Reference(child.rewrite(edit, row, base)?)))
                    })
                    .collect::<Result<Vec<_>, Error>>()?;
                edit.derive_inner(*id, fields)
            }
        }
    }
}

fn insert_unary(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    kind: Op,
    input: OutputPort,
    fun: ExpressionId,
    output_type: NodeId,
) -> Result<OutputPort, Error> {
    let operator = edit.append_operator(
        body,
        site,
        kind,
        &[input],
        [
            ("outputType", Field::Reference(output_type)),
            ("function", Field::Reference(fun.into())),
        ],
    )?;
    Ok(OutputPort {
        operator,
        number: 0,
    })
}

fn decompose(
    edit: &mut CircuitEditor,
    body: NodeId,
    id: NodeId,
    names: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<(NodeId, OutputPort)>, Error> {
    let filter = edit.node(id)?;
    let input = filter
        .inputs()
        .next()
        .ok_or_else(|| Error::new("filter requires an input"))?;
    let fun = filter.edge("function")?;
    let function = simplify_closure_with_warnings(edit, fun, Some(warnings))?;
    let closure = edit.node(function)?;
    let original = closure.edge("body")?;
    let [param] = closure
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("filter requires one parameter"))?;
    let parameter = edit.node(param)?;
    let name = parameter
        .data("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::new("unnamed filter parameter"))?
        .to_owned();
    let reference = edit.type_id(parameter.edge("type")?)?;
    let mut hoisted = Hoisted::default();
    let predicate = hoisted.collect(edit, original, std::slice::from_ref(&name), 0)?;
    if hoisted.values.is_empty()
        || (hoisted.values.len() == 1 && !contains_now(edit.circuit(), &[function])?)
    {
        return Ok(None);
    }
    let row_type = edit.type_id(edit.node(reference.into())?.edge("type")?)?;
    let width = edit.node(row_type.into())?.edges("tupFields")?.len();
    let path = edit.named_variable(reference, &name)?;
    let original_fields = |edit: &mut CircuitEditor, var| {
        (0..width)
            .map(|i| row_field(edit, var, i))
            .collect::<Result<Vec<_>, Error>>()
    };
    let mut fields = original_fields(edit, path)?;
    for computed in hoisted.values {
        fields.push(edit.expression(computed)?);
    }
    let widened = edit.tuple_expr(id, false, &fields)?;
    let widened_type = edit.expression_type(widened)?;
    let widened_stream = edit.zset_type(widened_type)?;
    let projection = edit.closure_with_parameters(widened, &[param])?;
    let pre = insert_unary(
        edit,
        body,
        id,
        Op::Map,
        input,
        projection,
        widened_stream.into(),
    )?;

    let borrowed = edit.reference_type(widened_type)?;
    let row = edit.variable(borrowed, names)?;
    let predicate = predicate.rewrite(edit, row, width)?;
    let prefix = original_fields(edit, row)?;
    let original_row = edit.tuple_with_type_at(id, row_type, &prefix)?;
    let argument = edit.borrow_value(original_row)?;
    let reduced = reduce_closure_body(edit, predicate, &[param], &[argument.into()])?;
    let predicate = edit.wrap_bool_if_needed(edit.expression(reduced)?)?;
    let fun = edit.closure_from_variables(predicate, &[row])?;
    let filter = insert_unary(edit, body, id, Op::Filter, pre, fun, widened_stream.into())?;

    let borrowed = edit.reference_type(widened_type)?;
    let row = edit.variable(borrowed, names)?;
    let fields_back = original_fields(edit, row)?;
    let narrow = edit.tuple_with_type_at(id, row_type, &fields_back)?;
    let fun = edit.closure_from_variables(narrow, &[row])?;
    let stream = edit.zset_type(row_type)?;
    let post = insert_unary(edit, body, id, Op::Map, filter, fun, stream.into())?;
    edit.annotate(
        post.operator,
        serde_json::json!({"class":"IsProjection","outputSize": fields.len()}),
    )?;
    Ok(Some((pre.operator, post)))
}

pub fn decompose_expensive_filters(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    crate::filters::rewrite_filters(circuit, next, decompose)
}
