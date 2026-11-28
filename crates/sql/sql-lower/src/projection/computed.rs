//! Scalar calls over fields of a projected row.

use crate::{
    closure::RowParameter,
    source_table::{range, Ids},
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
    },
    Abs {
        field: usize,
        nullable: bool,
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
                let (symbol, opcode) = match op {
                    "SqlStdOperatorTable.CHECKED_PLUS" => ("+", BinaryOp::Add),
                    "SqlStdOperatorTable.CHECKED_MINUS" => ("-", BinaryOp::Sub),
                    "SqlStdOperatorTable.CHECKED_MULTIPLY" => ("*", BinaryOp::Mul),
                    _ => return Err(Error("unsupported binary projection operator".into())),
                };
                let (left, left_type) = column(left)?;
                let (right, right_type) = column(right)?;
                if !matches!(result.kind, TypeKind::Integer { signed: true, .. })
                    || left_type.kind != result.kind
                    || right_type.kind != result.kind
                    || result.nullable != (left_type.nullable || right_type.nullable)
                {
                    return Err(Error("integer projection requires equal operand widths and the inferred nullability".into()));
                }
                Call::Arithmetic {
                    left,
                    right,
                    symbol,
                    opcode,
                }
            }
            ("feldera-s1-builtins", ABS_KEY, &[arg]) => {
                let (field, input) = column(arg)?;
                if !matches!(
                    input.kind,
                    TypeKind::Float {
                        flavor: FloatFlavor::Double
                    }
                ) || input.kind != result.kind
                    || input.nullable != result.nullable
                {
                    return Err(Error(
                        "ABS projection requires a DOUBLE field and matching return type".into(),
                    ));
                }
                Call::Abs {
                    field,
                    nullable: input.nullable,
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
        ty: Value,
    ) -> Result<Value, Error> {
        let mut expr = match self.call {
            Call::Arithmetic {
                left,
                right,
                opcode,
                ..
            } => {
                let mut expr = ids.node("DBSPBinaryExpression", &self.position);
                expr["left"] = parameter.read(ids, left)?;
                expr["right"] = parameter.read(ids, right)?;
                expr["opcode"] = opcode.wire_name().into();
                expr
            }
            Call::Abs { field, nullable } => {
                let mut segment = ids.generated("DBSPSimplePathSegment");
                segment["identifier"] = if nullable { "abs_dN" } else { "abs_d" }.into();
                segment["genericArgs"] = json!([]);
                let mut path = ids.generated("DBSPPath");
                path["components"] = vec![segment].into();
                let mut function = ids.generated("DBSPPathExpression");
                function["path"] = path;
                function["type"] = ids.singleton(CircuitType::Any, "ANY", false);
                let mut expr = ids.node("DBSPApplyExpression", &self.position);
                expr["function"] = function;
                expr["arguments"] = vec![parameter.read(ids, field)?].into();
                expr
            }
        };
        expr["type"] = ty;
        Ok(expr)
    }
}
