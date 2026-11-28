//! Scalar calls over fields of a projected row.

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
                let mut segment = ids.generated("DBSPSimplePathSegment");
                segment["identifier"] = function.as_str().into();
                segment["genericArgs"] = json!([]);
                let mut path = ids.generated("DBSPPath");
                path["components"] = vec![segment].into();
                let mut function = ids.generated("DBSPPathExpression");
                function["path"] = path;
                function["type"] = ids.singleton(CircuitType::Any, "ANY", false);
                let mut expr = ids.node("DBSPApplyExpression", &self.position);
                expr["function"] = function;
                expr["arguments"] = vec![parameter.read(ids, field)?].into();
                expr["type"] = output.column_type(ids, index);
                expr
            }
        };
        Ok(expr)
    }
}
