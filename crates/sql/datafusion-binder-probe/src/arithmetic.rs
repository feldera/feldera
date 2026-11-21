//! Bind Feldera arithmetic before a parent asks DataFusion for its Arrow type.
use crate::signatures;
use arrow_schema::{DataType, FieldRef};
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ExprSchemable, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, Volatility,
    expr::Cast,
    planner::{ExprPlanner, PlannerResult, RawBinaryExpr},
};
use datafusion_sql::sqlparser::ast::BinaryOperator;
use feldera_type_derivation_probe::{
    extras,
    model::{DerivedType, Outcome},
};

#[derive(Debug)]
pub struct Arithmetic;

impl ExprPlanner for Arithmetic {
    fn plan_binary_op(
        &self,
        expression: RawBinaryExpr,
        schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        let (operator, name) = match expression.op {
            BinaryOperator::Plus => ("+", "feldera_extension_add"),
            BinaryOperator::Minus => ("-", "feldera_extension_subtract"),
            BinaryOperator::Multiply => ("*", "feldera_extension_multiply"),
            BinaryOperator::Divide => ("/", "feldera_extension_divide"),
            _ => return Ok(PlannerResult::Original(expression)),
        };
        let left = signatures::sql_type(&expression.left.to_field(schema)?.1)?;
        let right = signatures::sql_type(&expression.right.to_field(schema)?.1)?;
        let Outcome::Bound {
            result: DerivedType::Scalar(result),
            operands,
        } = extras::infer(operator, &[left, right])
        else {
            return plan_err!("Feldera rejects {left:?} {operator} {right:?}");
        };
        let [left_target, right_target] = operands.as_slice() else {
            return plan_err!("arithmetic inference did not return two operand types");
        };
        let function = ScalarUDF::new_from_impl(BoundArithmetic {
            name,
            result: signatures::field(name, result)?,
            signature: Signature::any(2, Volatility::Immutable),
        });
        let coerce = |expression, source, target| -> Result<Expr> {
            if source == target {
                return Ok(expression);
            }
            let field = signatures::field("", target)?;
            Ok(Expr::Cast(Cast::new_from_field(
                Box::new(expression),
                field,
            )))
        };
        Ok(PlannerResult::Planned(function.call(vec![
            coerce(expression.left, left, *left_target)?,
            coerce(expression.right, right, *right_target)?,
        ])))
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct BoundArithmetic {
    name: &'static str,
    result: FieldRef,
    signature: Signature,
}

impl ScalarUDFImpl for BoundArithmetic {
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(self.result.data_type().clone())
    }
    fn return_field_from_args(&self, _: ReturnFieldArgs<'_>) -> Result<FieldRef> {
        Ok(self.result.clone())
    }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> {
        plan_err!(
            "{} requires lowering to a Feldera arithmetic expression",
            self.name
        )
    }
}
