//! Preserve Intersect and Minus before DataFusion builds membership joins.
//! DataFusion plans each leaf. Only one explicitly typed INTEGER column is
//! supported by this experiment. The relation tree carries the source span
//! independently of the Arrow type.

use arrow_schema::DataType;
use datafusion_common::{Result, ScalarValue, plan_err};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::ast::{Query, SetExpr, SetOperator, SetQuantifier, Spanned, Statement},
};
use serde::Serialize;
use std::collections::BTreeMap;

#[derive(Debug, Serialize)]
pub struct Relation {
    span: [u64; 4],
    nullable: bool,
    #[serde(flatten)]
    kind: RelationKind,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum RelationKind {
    IntegerValues {
        rows: Vec<Option<i32>>,
    },
    Intersect {
        all: bool,
        left: Box<Relation>,
        right: Box<Relation>,
    },
    Minus {
        all: bool,
        left: Box<Relation>,
        right: Box<Relation>,
    },
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct WeightedRow {
    row: Option<i32>,
    weight: usize,
}

type Bag = BTreeMap<Option<i32>, usize>;

impl Relation {
    pub fn output(&self) -> Vec<WeightedRow> {
        self.bag()
            .into_iter()
            .map(|(row, weight)| WeightedRow { row, weight })
            .collect()
    }

    fn bag(&self) -> Bag {
        match &self.kind {
            RelationKind::IntegerValues { rows } => {
                let mut counts = Bag::new();
                for row in rows {
                    *counts.entry(*row).or_default() += 1;
                }
                counts
            }
            RelationKind::Intersect { all, left, right } => combine(left, right, *all, usize::min),
            RelationKind::Minus { all, left, right } => {
                combine(left, right, *all, usize::saturating_sub)
            }
        }
    }
}

fn combine(left: &Relation, right: &Relation, all: bool, op: fn(usize, usize) -> usize) -> Bag {
    let right = right.bag();
    left.bag()
        .into_iter()
        .filter_map(|(key, left_count)| {
            let right_count = right.get(&key).copied().unwrap_or(0);
            let multiplicity = if all {
                op(left_count, right_count)
            } else {
                op(1, usize::from(right_count != 0))
            };
            (multiplicity != 0).then_some((key, multiplicity))
        })
        .collect()
}

pub fn bind(planner: &SqlToRel<'_, impl ContextProvider>, query: Query) -> Result<Relation> {
    let Query {
        with: None,
        body,
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks,
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators,
    } = query
    else {
        return plan_err!("query modifiers are outside the set operation probe");
    };
    if !locks.is_empty() || !pipe_operators.is_empty() {
        return plan_err!("query locks and pipes are outside the set operation probe");
    }
    bind_body(planner, *body)
}

fn bind_body(planner: &SqlToRel<'_, impl ContextProvider>, body: SetExpr) -> Result<Relation> {
    let span = body.span();
    let span = [
        span.start.line,
        span.start.column,
        span.end.line,
        span.end.column,
    ];
    let (kind, nullable) = match body {
        SetExpr::Query(query) => return bind(planner, *query),
        SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let all = match set_quantifier {
                SetQuantifier::All => true,
                SetQuantifier::None | SetQuantifier::Distinct => false,
                _ => return plan_err!("unsupported set quantifier {set_quantifier}"),
            };
            let left = Box::new(bind_body(planner, *left)?);
            let right = Box::new(bind_body(planner, *right)?);
            match op {
                SetOperator::Intersect => {
                    let nullable = left.nullable && right.nullable;
                    (RelationKind::Intersect { all, left, right }, nullable)
                }
                SetOperator::Except => {
                    let nullable = left.nullable;
                    (RelationKind::Minus { all, left, right }, nullable)
                }
                _ => return plan_err!("unsupported set operator {op}"),
            }
        }
        body @ (SetExpr::Values(_) | SetExpr::Select(_)) => bind_leaf(planner, body)?,
        _ => return plan_err!("unsupported set operand"),
    };
    Ok(Relation {
        span,
        nullable,
        kind,
    })
}

fn bind_leaf(
    planner: &SqlToRel<'_, impl ContextProvider>,
    body: SetExpr,
) -> Result<(RelationKind, bool)> {
    // Build a leaf query directly from the original AST. No reparse or
    // membership join is involved in preserving these set operations.
    let query = Query {
        with: None,
        body: Box::new(body),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };
    let plan = planner.sql_statement_to_plan(Statement::Query(Box::new(query)))?;
    let fields = plan.schema().fields();
    let [field] = fields.as_ref() else {
        return plan_err!("set operation probe requires one INTEGER field");
    };
    if field.data_type() != &DataType::Int32 {
        return plan_err!("set operation probe requires a typed INTEGER field");
    }
    let rows = match &plan {
        LogicalPlan::Values(values) => read_values(&values.values)?,
        LogicalPlan::Projection(projection) => {
            let LogicalPlan::Filter(filter) = projection.input.as_ref() else {
                return plan_err!("expected SELECT over a false filter");
            };
            let LogicalPlan::SubqueryAlias(alias) = filter.input.as_ref() else {
                return plan_err!("expected alias over VALUES");
            };
            let LogicalPlan::Projection(rename) = alias.input.as_ref() else {
                return plan_err!("expected column rename over VALUES");
            };
            let LogicalPlan::Values(values) = rename.input.as_ref() else {
                return plan_err!("expected literal VALUES");
            };
            if !matches!(
                &filter.predicate,
                Expr::Literal(ScalarValue::Boolean(Some(false)), _)
            ) || !matches!(projection.expr.as_slice(), [Expr::Column(_)])
            {
                return plan_err!("expected one column above WHERE FALSE");
            }
            read_values(&values.values)?;
            Vec::new()
        }
        _ => return plan_err!("unsupported plan at a set operation leaf"),
    };
    Ok((RelationKind::IntegerValues { rows }, field.is_nullable()))
}

fn read_values(rows: &[Vec<Expr>]) -> Result<Vec<Option<i32>>> {
    if rows.len() > 1000 {
        return plan_err!("VALUES exceeds 1,000 rows");
    }
    rows.iter()
        .map(|row| {
            let [expr] = row.as_slice() else {
                return plan_err!("expected one value expression per row");
            };
            int_from_expression(expr)
        })
        .collect()
}

fn int_from_expression(expression: &Expr) -> Result<Option<i32>> {
    fn scalar(expression: &Expr) -> Result<ScalarValue> {
        match expression {
            Expr::Literal(
                value @ (ScalarValue::Null | ScalarValue::Int32(_) | ScalarValue::Int64(_)),
                _,
            ) => Ok(value.clone()),
            Expr::Cast(cast) if cast.field.data_type() == &DataType::Int32 => {
                scalar(&cast.expr)?.cast_to(&DataType::Int32)
            }
            Expr::Alias(alias) => scalar(&alias.expr),
            _ => plan_err!("unsupported value expression {expression}"),
        }
    }
    match scalar(expression)? {
        ScalarValue::Int32(value) => Ok(value),
        _ => plan_err!("expected INTEGER value"),
    }
}
