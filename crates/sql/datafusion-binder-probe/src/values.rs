//! Intercept derived VALUES before DataFusion requires identical metadata.
use crate::signatures;
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::{
    Expr, ExprSchemable, LogicalPlan,
    expr::Cast,
    logical_plan::Values as PlannedValues,
    planner::{PlannedRelation, RelationPlanner, RelationPlannerContext, RelationPlanning},
};
use datafusion_sql::sqlparser::ast::{Query, SetExpr, TableFactor, Values};
use feldera_type_derivation_probe::{
    extras,
    model::{DerivedType, Outcome},
};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug)]
pub struct DerivedValues;

impl RelationPlanner for DerivedValues {
    fn plan_relation(
        &self,
        relation: TableFactor,
        context: &mut dyn RelationPlannerContext,
    ) -> Result<RelationPlanning> {
        // The probe handles a two-row, one-column derived VALUES without
        // query modifiers. All other relation forms keep normal dispatch.
        let TableFactor::Derived {
            lateral: false,
            subquery,
            alias,
            sample: None,
        } = &relation
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
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
        } = subquery.as_ref()
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let SetExpr::Values(Values {
            explicit_row: false,
            value_keyword: false,
            rows,
        }) = body.as_ref()
        else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let [first, second] = rows.as_slice() else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        let ([first], [second]) = (first.content.as_slice(), second.content.as_slice()) else {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        };
        if !locks.is_empty() || !pipe_operators.is_empty() {
            return Ok(RelationPlanning::Original(Box::new(relation)));
        }

        let empty = DFSchema::empty();
        let first = context.sql_to_expr(first.clone(), &empty)?;
        let second = context.sql_to_expr(second.clone(), &empty)?;
        let first_type = signatures::sql_type(&first.to_field(&empty)?.1)?;
        let second_type = signatures::sql_type(&second.to_field(&empty)?.1)?;
        let Outcome::Bound {
            result: DerivedType::Scalar(common),
            ..
        } = extras::infer("UNION ALL", &[first_type, second_type])
        else {
            return plan_err!("no shared Feldera type for the VALUES column");
        };
        let field = signatures::field("column1", common)?;
        let align = |expression, source| -> Result<Expr> {
            let target = signatures::field(
                "",
                feldera_type_derivation_probe::model::SqlType {
                    nullable: source,
                    ..common
                },
            )?;
            Ok(Expr::Cast(Cast::new_from_field(
                Box::new(expression),
                target,
            )))
        };
        let schema = Arc::new(DFSchema::from_unqualified_fields(
            vec![field].into(),
            HashMap::new(),
        )?);
        let first = if first_type.kind == common.kind {
            first
        } else {
            align(first, first_type.nullable)?
        };
        let second = if second_type.kind == common.kind {
            second
        } else {
            align(second, second_type.nullable)?
        };
        let plan = LogicalPlan::Values(PlannedValues {
            schema,
            values: vec![vec![first], vec![second]],
        });
        Ok(RelationPlanning::Planned(Box::new(PlannedRelation::new(
            plan,
            alias.clone(),
        ))))
    }
}
