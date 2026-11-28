//! Bind positional set operations. The SQL result takes its field names from
//! the left operand, even when an identity projection disappears from the plan.

use super::{BoundQuery, DeclarationTokens, Failure, bind_select, unsupported};
use crate::{SetExpr, program::graph::append_set};
use feldera_sql_rel_model::{ValidationLimits, input_producers::SetKind, program::Program};
use sqlparser_064::ast::{SetOperator, SetQuantifier};
use std::collections::HashSet;

#[derive(Clone, Copy, Eq, PartialEq)]
struct SetOperation {
    kind: SetKind,
    all: bool,
}

impl SetOperation {
    fn from_ast(op: SetOperator, quantifier: SetQuantifier) -> Result<Self, Failure> {
        let kind = match op {
            SetOperator::Union => SetKind::Union,
            SetOperator::Except | SetOperator::Minus => SetKind::Except,
            SetOperator::Intersect => SetKind::Intersect,
        };
        let all = match quantifier {
            SetQuantifier::All => true,
            SetQuantifier::None | SetQuantifier::Distinct => false,
            _ => return Err(unsupported("expected a positional set operation")),
        };
        Ok(Self { kind, all })
    }
}

pub(super) fn bind(
    program: &mut Program,
    body: &SetExpr,
    tokens: &DeclarationTokens<'_>,
    depth: usize,
) -> Result<BoundQuery, Failure> {
    if depth >= ValidationLimits::default().depth {
        return Err(unsupported("set expression exceeds the validation depth"));
    }
    match body {
        SetExpr::Select(select) => bind_select(program, select, tokens),
        SetExpr::SetOperation {
            op, set_quantifier, ..
        } => {
            let operation = SetOperation::from_ast(*op, *set_quantifier)?;
            bind_set(program, operation, body, tokens, depth + 1)
        }
        _ => Err(unsupported("expected SELECT or a positional set operation")),
    }
}

fn bind_set(
    program: &mut Program,
    operation: SetOperation,
    body: &SetExpr,
    tokens: &DeclarationTokens<'_>,
    depth: usize,
) -> Result<BoundQuery, Failure> {
    // Calcite combines a left-associated chain of identical operations into
    // one n-ary relation. EXCEPT preserves the order of the right inputs.
    let mut operands = Vec::new();
    let mut left = body;
    while let SetExpr::SetOperation {
        op,
        set_quantifier,
        left: child,
        right,
    } = left
    {
        if SetOperation::from_ast(*op, *set_quantifier)? != operation {
            break;
        }
        operands.push(right.as_ref());
        left = child;
    }
    let first_scan = program.catalog.tables.len();
    let mut result = bind(program, left, tokens, depth)?;
    let mut inputs = vec![result.relation];
    let keyword = operation.kind.keyword();
    let quantifier = if operation.all { " ALL" } else { "" };
    let separator = format!("\n{keyword}{quantifier}\n");
    for operand in operands.into_iter().rev() {
        let right = bind(program, operand, tokens, depth)?;
        if !result
            .fields
            .iter()
            .map(|f| f.1)
            .eq(right.fields.iter().map(|f| f.1))
        {
            return Err(unsupported("set inputs require column type coercion"));
        }
        inputs.push(right.relation);
        result.text.pretty.push_str(&separator);
        result.text.pretty.push_str(&right.text.pretty);
        result.text.diagnostic.push_str(&separator);
        result.text.diagnostic.push_str(&right.text.diagnostic);
    }
    let mut sources = HashSet::new();
    if program.catalog.tables[first_scan..]
        .iter()
        .any(|table| !sources.insert(table.schema))
    {
        return Err(unsupported(
            "set operation over a repeated source requires relational simplification",
        ));
    }
    result.kind = keyword;
    result.relation = append_set(program, operation.kind.form(), operation.all, inputs)?;
    Ok(result)
}
