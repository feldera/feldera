//! Scalar calls over fields of a projected row.

use super::{
    field::DoubleLiteral,
    interval_count::{cast, IntervalCount},
};
use crate::{
    closure::RowParameter,
    integer::SignedInteger,
    source_table::{range, ColumnLayout, Ids},
    Error,
};
use feldera_sql_ir::{BinaryOp, TypeKind as CircuitType};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::{ObjectKind, ObjectPosition},
    expressions::ExprId,
    operators::{OperatorObject, ABS_KEY},
    program::Program,
    types::{FloatFlavor, TypeKind},
    ValidationLimits,
};
use serde_json::{json, Value};

pub(crate) struct Computed {
    call: Call,
    result: ColumnLayout,
    position: Value,
}

enum Operand {
    Column(usize),
    Integer(SignedInteger),
    Call(Box<Computed>),
}

impl Operand {
    fn resolve(
        program: &Program,
        id: ExprId,
        budget: &mut usize,
        depth: usize,
    ) -> Result<Self, Error> {
        match program.expressions.expressions[id.0 as usize].kind {
            ObjectKind::InputRef { object } => Ok(Self::Column(
                program.table.objects[object.0 as usize].field as usize,
            )),
            ObjectKind::Literal { .. } => SignedInteger::resolve(program, id).map(Self::Integer),
            ObjectKind::Call { .. } => Computed::from_expr(program, id, budget, depth)
                .map(Box::new)
                .map(Self::Call),
            _ => Err(Error(
                "expected a column or integer literal in arithmetic".into(),
            )),
        }
    }

    fn digest(&self) -> String {
        match self {
            Self::Column(index) => format!("${index}"),
            Self::Integer(integer) => integer.digest(),
            Self::Call(call) => call.digest(),
        }
    }

    fn sql(&self, input: &CatalogSchema, min_precedence: u8) -> Result<String, Error> {
        match self {
            Self::Column(index) => {
                super::field::column_name(input, *index).map(super::field::quote)
            }
            Self::Integer(integer) => Ok(integer.value.to_string()),
            Self::Call(call) => {
                let sql = call.sql(input)?;
                if call.precedence() < min_precedence {
                    Ok(format!("({sql})"))
                } else {
                    Ok(sql)
                }
            }
        }
    }

    fn lower(&self, ids: &mut Ids, parameter: &mut RowParameter) -> Result<Value, Error> {
        match self {
            Self::Column(index) => parameter.read(ids, *index),
            Self::Integer(integer) => Ok(integer.lower(ids)),
            Self::Call(call) => call.lower(ids, parameter),
        }
    }
}

enum Call {
    Arithmetic {
        left: Operand,
        right: Operand,
        symbol: &'static str,
        opcode: BinaryOp,
        reuse_left_type: bool,
        widen_right: bool,
    },
    IntervalCount(Box<IntervalCount>),
    Trigonometric {
        name: String,
        argument: DoubleLiteral,
    },
    Abs {
        field: usize,
        function: String,
    },
}

impl Computed {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let mut limits = ValidationLimits::default();
        Self::from_expr(program, id, &mut limits.references, limits.depth)
    }

    fn from_expr(
        program: &Program,
        id: ExprId,
        budget: &mut usize,
        depth: usize,
    ) -> Result<Self, Error> {
        let child_depth = depth
            .checked_sub(1)
            .ok_or_else(|| Error("nested projection exceeds the expression depth limit".into()))?;
        *budget = budget.checked_sub(1).ok_or_else(|| {
            Error("expanded projection exceeds the expression reference limit".into())
        })?;
        let expr = &program.expressions.expressions[id.0 as usize];
        let ObjectPosition::Present { position, .. } = &expr.position else {
            return Err(Error("missing scalar call position".into()));
        };
        let ObjectKind::Call {
            operator,
            arguments,
        } = &expr.kind
        else {
            return Err(Error("expected a scalar call".into()));
        };
        let operator = program.expressions.operators[operator.0 as usize].object;
        let result = &program.table.type_table.types[expr.ty.0 as usize];
        let OperatorObject::Builtin {
            registry,
            registry_version,
            key,
        } = &program.expressions.operator_objects[operator.0 as usize]
        else {
            return Err(Error("expected a built-in projection operator".into()));
        };
        if registry_version != "1" {
            return Err(Error(
                "unsupported projection operator registry version".into(),
            ));
        }
        let column = |id: ExprId| {
            let expr = &program.expressions.expressions[id.0 as usize];
            let ObjectKind::InputRef { object } = expr.kind else {
                return Err(Error("computed projection requires column operands".into()));
            };
            let ty = &program.table.type_table.types[expr.ty.0 as usize];
            Ok((program.table.objects[object.0 as usize].field as usize, ty))
        };
        let call = match (registry.as_str(), key.as_str(), arguments.as_slice()) {
            ("calcite-s1-fixture", "SqlStdOperatorTable.CAST", [_]) => {
                Call::IntervalCount(Box::new(IntervalCount::resolve(program, id)?))
            }
            ("calcite-s1-fixture", op, &[left, right]) => {
                let name = match result.kind {
                    TypeKind::Integer { .. } => op.strip_prefix("SqlStdOperatorTable.CHECKED_"),
                    TypeKind::Float {
                        flavor: FloatFlavor::Real | FloatFlavor::Double,
                    } => op.strip_prefix("SqlStdOperatorTable."),
                    _ => None,
                };
                let (symbol, opcode) = match name {
                    Some("PLUS") => ("+", BinaryOp::Add),
                    Some("MINUS") => ("-", BinaryOp::Sub),
                    Some("MULTIPLY") => ("*", BinaryOp::Mul),
                    Some("DIVIDE") => ("/", BinaryOp::Div),
                    _ => return Err(Error("unsupported arithmetic projection operator".into())),
                };
                let ty = |id: ExprId| {
                    let ty = program.expressions.expressions[id.0 as usize].ty;
                    &program.table.type_table.types[ty.0 as usize]
                };
                let left_type = ty(left);
                let right_type = ty(right);
                // A scale factor remains an INTEGER Rex literal even when
                // the interval count and product are BIGINT.
                let widen_right = matches!((&left_type.kind, &right_type.kind),
                    (TypeKind::Integer { width: left, signed: true }, TypeKind::Integer { width: right, signed: true })
                        if left.bits() > right.bits())
                    && matches!(
                        program.expressions.expressions[right.0 as usize].kind,
                        ObjectKind::Literal { .. }
                    )
                    && opcode == BinaryOp::Mul;
                if left_type.kind != result.kind
                    || (!widen_right && right_type.kind != result.kind)
                    || result.nullable != (left_type.nullable || right_type.nullable)
                {
                    return Err(Error(
                        "arithmetic result differs from the inferred common operand type".into(),
                    ));
                }
                Call::Arithmetic {
                    left: Operand::resolve(program, left, budget, child_depth)?,
                    right: Operand::resolve(program, right, budget, child_depth)?,
                    symbol,
                    opcode,
                    widen_right,
                    reuse_left_type: opcode == BinaryOp::Div
                        && left_type.nullable == result.nullable,
                }
            }
            ("calcite-s1-fixture", key, &[arg]) => {
                let name = key
                    .strip_prefix("SqlStdOperatorTable.")
                    .filter(|name| {
                        matches!(
                            *name,
                            "SIN"
                                | "COS"
                                | "TAN"
                                | "COT"
                                | "ASIN"
                                | "ACOS"
                                | "ATAN"
                                | "DEGREES"
                                | "RADIANS"
                        )
                    })
                    .ok_or_else(|| Error("unsupported unary projection call".into()))?;
                let argument = DoubleLiteral::resolve(program, arg)?;
                if !matches!(
                    result.kind,
                    TypeKind::Float {
                        flavor: FloatFlavor::Double
                    }
                ) || result.nullable != argument.is_null()
                {
                    return Err(Error(
                        "trigonometric result differs from its operand type".into(),
                    ));
                }
                Call::Trigonometric {
                    name: name.into(),
                    argument,
                }
            }
            ("feldera-s1-builtins", ABS_KEY, &[arg]) => {
                let (field, input) = column(arg)?;
                if input.kind != result.kind || input.nullable != result.nullable {
                    return Err(Error("ABS result differs from its operand type".into()));
                }
                let suffix = match input.kind {
                    TypeKind::Integer { width, signed } => {
                        format!("{}{}", if signed { "i" } else { "u" }, width.bits())
                    }
                    TypeKind::Float {
                        flavor: FloatFlavor::Real,
                    } => "f".into(),
                    TypeKind::Float {
                        flavor: FloatFlavor::Double,
                    } => "d".into(),
                    _ => return Err(Error("unsupported ABS operand type".into())),
                };
                let nullable = if input.nullable { "N" } else { "" };
                Call::Abs {
                    field,
                    function: format!("abs_{suffix}{nullable}"),
                }
            }
            _ => return Err(Error("unsupported projection call".into())),
        };
        Ok(Self {
            call,
            result: ColumnLayout::scalar(result)?,
            position: range(
                position.line,
                position.column,
                position.end_line,
                position.end_column,
            ),
        })
    }

    pub(super) fn digest(&self) -> String {
        match self.call {
            Call::Arithmetic {
                ref left,
                ref right,
                symbol,
                ..
            } => format!("{symbol}({}, {})", left.digest(), right.digest()),
            Call::IntervalCount(ref count) => count.digest(),
            Call::Abs { field, .. } => format!("abs(${field})"),
            Call::Trigonometric { ref name, argument } => format!("{name}({})", argument.digest()),
        }
    }

    fn precedence(&self) -> u8 {
        match self.call {
            Call::Arithmetic {
                opcode: BinaryOp::Add | BinaryOp::Sub,
                ..
            } => 1,
            Call::Arithmetic { .. } => 2,
            _ => u8::MAX,
        }
    }

    pub(super) fn sql(&self, input: &CatalogSchema) -> Result<String, Error> {
        let field = |i| super::field::column_name(input, i).map(super::field::quote);
        Ok(match self.call {
            Call::Arithmetic {
                ref left,
                ref right,
                symbol,
                ..
            } => format!(
                "{} {symbol} {}",
                left.sql(input, self.precedence())?,
                right.sql(input, self.precedence() + 1)?
            ),
            Call::IntervalCount(ref count) => count.sql(),
            Call::Abs { field: index, .. } => format!("ABS({})", field(index)?),
            Call::Trigonometric { ref name, argument } => format!("{name}({})", argument.sql()),
        })
    }

    pub(super) fn lower(
        &self,
        ids: &mut Ids,
        parameter: &mut RowParameter,
    ) -> Result<Value, Error> {
        let expr = match self.call {
            Call::Arithmetic {
                ref left,
                ref right,
                opcode,
                reuse_left_type,
                widen_right,
                ..
            } => {
                let mut expr = ids.node("DBSPBinaryExpression", &self.position);
                expr["left"] = left.lower(ids, parameter)?;
                expr["right"] = right.lower(ids, parameter)?;
                expr["opcode"] = opcode.wire_name().into();
                // Division uses the common operand type. When nullability
                // already matches, Java shares the left operand's type object.
                expr["type"] = if reuse_left_type {
                    let reference = crate::view::reference(&expr["left"]["type"])?;
                    std::mem::replace(&mut expr["left"]["type"], reference)
                } else if opcode == BinaryOp::Div {
                    self.result.type_node(ids)
                } else {
                    self.result.node(ids)
                };
                if widen_right {
                    let ty = crate::view::reference(&expr["type"])?;
                    let right = expr["right"].take();
                    expr["right"] = cast(ids, &self.position, right, ty);
                }
                expr
            }
            Call::IntervalCount(ref count) => {
                let ty = self.result.node(ids);
                count.lower(ids, ty)?
            }
            Call::Abs {
                field,
                ref function,
            } => {
                let argument = parameter.read(ids, field)?;
                self.apply(ids, function, argument)
            }
            Call::Trigonometric { ref name, argument } => {
                let literal = argument.lower(ids);
                if argument.is_null() {
                    literal
                } else {
                    self.apply(ids, &format!("{}_d", name.to_ascii_lowercase()), literal)
                }
            }
        };
        Ok(expr)
    }

    fn apply(&self, ids: &mut Ids, identifier: &str, argument: Value) -> Value {
        let mut segment = ids.generated("DBSPSimplePathSegment");
        segment["identifier"] = identifier.into();
        segment["genericArgs"] = json!([]);
        let mut path = ids.generated("DBSPPath");
        path["components"] = vec![segment].into();
        let mut function = ids.generated("DBSPPathExpression");
        function["path"] = path;
        function["type"] = ids.singleton(CircuitType::Any, "ANY", false);
        let mut expr = ids.node("DBSPApplyExpression", &self.position);
        expr["function"] = function;
        expr["arguments"] = vec![argument].into();
        expr["type"] = self.result.node(ids);
        expr
    }
}
