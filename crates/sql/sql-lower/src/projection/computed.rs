//! Scalar calls over fields of a projected row.

use super::{
    array::{ArrayConstructor, ArrayLengthArgument},
    interval_count::{cast, IntervalCount},
    temporal_text::TemporalCall,
    trigonometric::TrigonometricArgument,
};
use crate::{
    closure::RowParameter,
    integer::SignedInteger,
    source_table::{range, ColumnLayout, Ids},
    Error,
};
use feldera_sql_ir::{BinaryOp, TypeKind as CircuitType};
use feldera_sql_rel_model::{
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
    nullable_output: Option<ColumnLayout>,
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

    // RexCall omits a literal's type when its other operand identifies the
    // same SQL type. Nullability alone does not distinguish the types.
    fn arithmetic_digest(&self, other: &Self, same_type: bool) -> String {
        match self {
            Self::Integer(integer) if same_type && !matches!(other, Self::Integer(_)) => {
                integer.value.to_string()
            }
            _ => self.digest(),
        }
    }

    fn sql(
        &self,
        field: &impl Fn(usize, u8) -> Result<String, Error>,
        min_precedence: u8,
    ) -> Result<String, Error> {
        match self {
            Self::Column(index) => field(*index, min_precedence),
            Self::Integer(integer) => Ok(integer.value.to_string()),
            Self::Call(call) => {
                let sql = call.sql(field)?;
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
    TemporalText(Box<TemporalCall>),
    ArrayLength {
        name: &'static str,
        argument: ArrayLengthArgument,
    },
    Trigonometric {
        name: String,
        argument: Box<TrigonometricArgument>,
    },
    NullableColumn {
        field: usize,
        sql_type: &'static str,
    },
    Abs {
        argument: Operand,
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
        let mut operation_nullable = result.nullable;
        let call = match (registry.as_str(), key.as_str(), arguments.as_slice()) {
            ("calcite-s1-fixture", "SqlStdOperatorTable.CAST", &[arg])
                if matches!(
                    program.expressions.expressions[arg.0 as usize].kind,
                    ObjectKind::InputRef { .. }
                ) =>
            {
                let source = &program.expressions.expressions[arg.0 as usize];
                let input = &program.table.type_table.types[source.ty.0 as usize];
                let TypeKind::Integer {
                    width,
                    signed: true,
                } = input.kind
                else {
                    return Err(Error("expected an integer column cast".into()));
                };
                if input.nullable || !result.nullable || input.kind != result.kind {
                    return Err(Error("expected a nullability cast".into()));
                }
                let ObjectKind::InputRef { object } = source.kind else {
                    unreachable!()
                };
                Call::NullableColumn {
                    field: program.table.objects[object.0 as usize].field as usize,
                    sql_type: width.signed_sql_name(),
                }
            }
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
                operation_nullable = left_type.nullable || right_type.nullable;
                if left_type.kind != result.kind
                    || (!widen_right && right_type.kind != result.kind)
                    || (!result.nullable && operation_nullable)
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
                        && left_type.nullable == operation_nullable,
                }
            }
            (
                "calcite-s1-fixture",
                key @ ("SqlStdOperatorTable.CARDINALITY"
                | "SqlLibraryOperators.ARRAY_LENGTH"
                | "SqlLibraryOperators.ARRAY_SIZE"),
                &[arg],
            ) => {
                let name = match key {
                    "SqlStdOperatorTable.CARDINALITY" => "CARDINALITY",
                    "SqlLibraryOperators.ARRAY_LENGTH" => "ARRAY_LENGTH",
                    _ => "ARRAY_SIZE",
                };
                let node = &program.expressions.expressions[arg.0 as usize];
                let ty = &program.table.type_table.types[node.ty.0 as usize];
                let argument = match (&ty.kind, &node.kind) {
                    (TypeKind::Null {}, ObjectKind::Literal { .. }) => ArrayLengthArgument::Null,
                    (TypeKind::Array { .. }, _) => ArrayLengthArgument::Constructor(Box::new(
                        ArrayConstructor::resolve(program, arg)?,
                    )),
                    _ => return Err(Error("expected an ARRAY length operand".into())),
                };
                if !matches!(
                    result.kind,
                    TypeKind::Integer {
                        width: feldera_sql_rel_model::types::IntegerWidth::I32,
                        signed: true
                    }
                ) || result.nullable != ty.nullable
                {
                    return Err(Error("unexpected ARRAY length result type".into()));
                }
                Call::ArrayLength { name, argument }
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
                let argument = TrigonometricArgument::resolve(program, arg)?;
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
                    argument: Box::new(argument),
                }
            }
            ("feldera-s1-builtins", key, args) if key != ABS_KEY => {
                let function =
                    feldera_sql_rel_model::operators::TemporalTextFunction::from_key(key)
                        .ok_or_else(|| Error("unsupported Feldera projection function".into()))?;
                Call::TemporalText(Box::new(TemporalCall::resolve(
                    program, function, args, result,
                )?))
            }
            ("feldera-s1-builtins", ABS_KEY, &[arg]) => {
                let input_ty = program.expressions.expressions[arg.0 as usize].ty;
                let input = &program.table.type_table.types[input_ty.0 as usize];
                operation_nullable = input.nullable;
                if input.kind != result.kind || (input.nullable && !result.nullable) {
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
                    argument: Operand::resolve(program, arg, budget, child_depth)?,
                    function: format!("abs_{suffix}{nullable}"),
                }
            }
            _ => return Err(Error("unsupported projection call".into())),
        };
        Ok(Self {
            call,
            result: ColumnLayout::scalar(&feldera_sql_rel_model::types::SqlType {
                nullable: operation_nullable,
                ..result.clone()
            })?,
            nullable_output: (result.nullable != operation_nullable)
                .then(|| ColumnLayout::scalar(result))
                .transpose()?,
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
                widen_right,
                ..
            } => format!(
                "{symbol}({}, {})",
                left.arithmetic_digest(right, !widen_right),
                right.arithmetic_digest(left, !widen_right)
            ),
            Call::TemporalText(ref call) => call.digest(),
            Call::IntervalCount(ref count) => count.digest(),
            Call::ArrayLength { name, ref argument } => format!("{name}({})", argument.digest()),
            Call::Abs { ref argument, .. } => format!("abs({})", argument.digest()),
            Call::NullableColumn { field, sql_type } => format!("CAST(${field}):{sql_type}"),
            Call::Trigonometric {
                ref name,
                ref argument,
            } => format!("{name}({})", argument.digest()),
        }
    }

    pub(crate) fn precedence(&self) -> u8 {
        match self.call {
            Call::Arithmetic {
                opcode: BinaryOp::Add | BinaryOp::Sub,
                ..
            } => 1,
            Call::Arithmetic { .. } => 2,
            _ => u8::MAX,
        }
    }

    pub(crate) fn sql(
        &self,
        column: &impl Fn(usize, u8) -> Result<String, Error>,
    ) -> Result<String, Error> {
        Ok(match self.call {
            Call::Arithmetic {
                ref left,
                ref right,
                symbol,
                ..
            } => format!(
                "{} {symbol} {}",
                left.sql(column, self.precedence())?,
                right.sql(column, self.precedence() + 1)?
            ),
            Call::TemporalText(ref call) => call.sql(),
            Call::IntervalCount(ref count) => count.sql(),
            Call::ArrayLength { name, ref argument } => format!("{name}({})", argument.sql()),
            Call::Abs { ref argument, .. } => format!("ABS({})", argument.sql(column, 0)?),
            Call::NullableColumn { field, sql_type } => {
                format!("CAST({} AS {sql_type})", column(field, 0)?)
            }
            Call::Trigonometric {
                ref name,
                ref argument,
            } => format!("{name}({})", argument.sql()),
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
            Call::ArrayLength { ref argument, .. } => {
                let value = argument.lower(ids)?;
                self.apply(ids, argument.function(), vec![value])
            }
            Call::TemporalText(ref call) => call.lower(ids, self)?,
            Call::IntervalCount(ref count) => {
                let ty = self.result.node(ids);
                count.lower(ids, ty)?
            }
            Call::Abs {
                ref argument,
                ref function,
            } => {
                let argument = argument.lower(ids, parameter)?;
                self.apply(ids, function, vec![argument])
            }
            Call::NullableColumn { field, .. } => {
                let source = parameter.read(ids, field)?;
                let ty = self.result.node(ids);
                cast(ids, &self.position, source, ty)
            }
            Call::Trigonometric {
                ref name,
                ref argument,
            } => {
                let literal = argument.lower(ids, &self.position)?;
                if argument.is_null() {
                    literal
                } else {
                    self.apply(
                        ids,
                        &format!("{}_d", name.to_ascii_lowercase()),
                        vec![literal],
                    )
                }
            }
        };
        Ok(if let Some(output) = &self.nullable_output {
            let ty = output.node(ids);
            cast(ids, &self.position, expr, ty)
        } else {
            expr
        })
    }

    pub(super) fn apply(&self, ids: &mut Ids, identifier: &str, arguments: Vec<Value>) -> Value {
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
        expr["arguments"] = arguments.into();
        expr["type"] = self.result.node(ids);
        expr
    }
}
