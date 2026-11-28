//! Scalar calls over fields of a projected row.

use super::field::DoubleLiteral;
use crate::{
    closure::RowParameter,
    source_table::{range, Ids, RowLayout},
    Error,
};
use feldera_sql_ir::{BinaryOp, TypeKind as CircuitType};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    expression_objects::ObjectKind,
    expressions::ExprId,
    operators::{OperatorObject, OperatorObjectId, ABS_KEY},
    program::Program,
    relation_metadata::Coordinates,
    types::{FloatFlavor, SqlType, TypeKind},
};
use serde_json::{json, Value};

pub(crate) struct Computed {
    call: Call,
    position: Value,
}

enum Call {
    Arithmetic {
        left: usize,
        right: usize,
        symbol: &'static str,
        opcode: BinaryOp,
        reuse_left_type: bool,
    },
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
    pub(super) fn resolve(
        program: &Program,
        operator: OperatorObjectId,
        args: &[ExprId],
        result: &SqlType,
        position: &Coordinates,
    ) -> Result<Self, Error> {
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
        let call = match (registry.as_str(), key.as_str(), args) {
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
                let (left, left_type) = column(left)?;
                let (right, right_type) = column(right)?;
                if left_type.kind != result.kind
                    || right_type.kind != result.kind
                    || result.nullable != (left_type.nullable || right_type.nullable)
                {
                    return Err(Error(
                        "arithmetic result differs from the inferred common operand type".into(),
                    ));
                }
                Call::Arithmetic {
                    left,
                    right,
                    symbol,
                    opcode,
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
                left,
                right,
                symbol,
                ..
            } => format!("{symbol}(${left}, ${right})"),
            Call::Abs { field, .. } => format!("abs(${field})"),
            Call::Trigonometric { ref name, argument } => format!("{name}({})", argument.digest()),
        }
    }

    pub(super) fn sql(&self, input: &CatalogSchema) -> Result<String, Error> {
        let field = |i| super::field::column_name(input, i).map(super::field::quote);
        Ok(match self.call {
            Call::Arithmetic {
                left,
                right,
                symbol,
                ..
            } => format!("{} {symbol} {}", field(left)?, field(right)?),
            Call::Abs { field: index, .. } => format!("ABS({})", field(index)?),
            Call::Trigonometric { ref name, argument } => format!("{name}({})", argument.sql()),
        })
    }

    pub(super) fn lower(
        &self,
        ids: &mut Ids,
        parameter: &mut RowParameter,
        output: &RowLayout<'_>,
        index: usize,
    ) -> Result<Value, Error> {
        let expr = match self.call {
            Call::Arithmetic {
                left,
                right,
                opcode,
                reuse_left_type,
                ..
            } => {
                let mut expr = ids.node("DBSPBinaryExpression", &self.position);
                expr["left"] = parameter.read(ids, left)?;
                expr["right"] = parameter.read(ids, right)?;
                expr["opcode"] = opcode.wire_name().into();
                // Division uses the common operand type. When nullability
                // already matches, Java shares the left operand's type object.
                expr["type"] = if reuse_left_type {
                    expr["left"]["type"].clone()
                } else if opcode == BinaryOp::Div {
                    output.fresh_scalar_type(ids, index)
                } else {
                    output.column_type(ids, index)
                };
                expr
            }
            Call::Abs {
                field,
                ref function,
            } => {
                let argument = parameter.read(ids, field)?;
                self.apply(ids, function, argument, output, index)
            }
            Call::Trigonometric { ref name, argument } => {
                let literal = argument.lower(ids);
                if argument.is_null() {
                    literal
                } else {
                    self.apply(
                        ids,
                        &format!("{}_d", name.to_ascii_lowercase()),
                        literal,
                        output,
                        index,
                    )
                }
            }
        };
        Ok(expr)
    }

    fn apply(
        &self,
        ids: &mut Ids,
        identifier: &str,
        argument: Value,
        output: &RowLayout<'_>,
        index: usize,
    ) -> Value {
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
        expr["type"] = output.column_type(ids, index);
        expr
    }
}
