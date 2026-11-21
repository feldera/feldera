//! Explicit constructor output. Original IDs refer to the validated input.
use feldera_sql_rel_model::{TypeId, expressions::ExprId, relation_properties::JoinKind};
use serde::Serialize;

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(transparent)]
pub struct PlanId(pub u32);

#[derive(Clone, Debug, Serialize)]
pub struct Column {
    pub name: String,
    pub ty: TypeId,
}

#[derive(Debug, Serialize)]
pub struct Recipe {
    pub schema_version: &'static str,
    pub id: String,
    pub nodes: Vec<Node>,
    pub roots: Vec<PlanId>,
    pub removed_correlates: usize,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Node {
    Original {
        relation: u32,
    },
    Rebuild {
        relation: u32,
        inputs: Vec<PlanId>,
    },
    /// Some preserves the original Project's alias hints; generated projections
    /// explicitly use None. Column names are exact; internal names are made unique by Rust.
    Project {
        relation: Option<u32>,
        input: PlanId,
        expressions: Vec<Expression>,
        columns: Vec<Column>,
    },
    Filter {
        relation: u32,
        input: PlanId,
        condition: Expression,
    },
    Distinct {
        input: PlanId,
    },
    /// Each key compares a left and right field with IS NOT DISTINCT FROM.
    /// Empty keys mean TRUE. Original SQL predicates remain separate Filters.
    Join {
        left: PlanId,
        right: PlanId,
        join_kind: JoinKind,
        keys: Vec<(usize, usize)>,
    },
    Set {
        relation: u32,
        inputs: Vec<PlanId>,
    },
    /// Select a bounded number of rows independently per correlation key.
    PartitionedFetch {
        relation: u32,
        input: PlanId,
        partition: Vec<usize>,
        order: Vec<usize>,
        limit: u32,
    },
    /// Add the complete correlation domain to an aggregate’s grouping keys.
    /// The adapter preserves the original aggregate operators and remaps args.
    Aggregate {
        relation: u32,
        input: PlanId,
        group: Vec<usize>,
        arguments: Vec<Vec<usize>>,
    },
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Expression {
    Existing {
        expression: ExprId,
    },
    Input {
        field: usize,
        ty: TypeId,
    },
    /// A field on the nullable side of the generated LEFT join.
    OuterInput {
        field: usize,
        ty: TypeId,
    },
    /// Restore COUNT's nonnullable zero after an unmatched domain LEFT join.
    ZeroIfNull {
        input: Box<Expression>,
        ty: TypeId,
    },
    /// Only a top-level nonnullable -> nullable change of the same SQL type.
    WidenNullability {
        input: Box<Expression>,
        ty: TypeId,
    },
    /// Clone the original typed operator/position with the rewritten operands.
    Call {
        expression: ExprId,
        arguments: Vec<Expression>,
    },
    Field {
        expression: ExprId,
        input: Box<Expression>,
    },
}
