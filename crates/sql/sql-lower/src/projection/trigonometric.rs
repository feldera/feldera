//! Unreduced trigonometric calls convert an exact argument to DOUBLE at runtime.

use super::{field::DoubleLiteral, interval_count::cast};
use crate::{
    source_table::{expression_range, range, ColumnLayout, Ids},
    values::{scalar::ScalarLiteral, type_name},
    Error,
};
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    types::{FloatFlavor, IntegerWidth, SqlType, TypeKind},
};
use serde_json::Value;

pub(super) enum TrigonometricArgument {
    Double(DoubleLiteral),
    Null,
    Exact(Box<ExactArgument>),
}

pub(super) struct ExactArgument {
    ty: SqlType,
    value: ScalarLiteral,
    digest: String,
    cast_position: Option<Value>,
}

impl TrigonometricArgument {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let types = &program.table.type_table;
        let get = |id: ExprId| &program.expressions.expressions[id.0 as usize];
        let mut expr = get(id);
        let mut cast_position = None;
        if let ObjectKind::Call {
            operator,
            arguments,
        } = &expr.kind
        {
            let op = &program.expressions.operators[operator.0 as usize];
            let builtin = &program.expressions.operator_objects[op.object.0 as usize];
            let [source] = arguments.as_slice() else {
                return Err(Error("expected a unary numeric cast".into()));
            };
            if !matches!(builtin, OperatorObject::Builtin { registry, registry_version, key }
                if registry == "calcite-s1-fixture" && registry_version == "1" && key == "SqlStdOperatorTable.CAST")
                || !matches!(
                    types.types[expr.ty.0 as usize].kind,
                    TypeKind::Float {
                        flavor: FloatFlavor::Double
                    }
                )
                || types.types[expr.ty.0 as usize].nullable
            {
                return Err(Error("expected a required DOUBLE cast".into()));
            }
            cast_position = Some(expression_range(&expr.position));
            expr = get(*source);
        }
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected a literal trigonometric argument".into()));
        };
        let ty = &types.types[expr.ty.0 as usize];
        match ty.kind {
            TypeKind::Float {
                flavor: FloatFlavor::Double,
            } if cast_position.is_none() => DoubleLiteral::resolve(program, id).map(Self::Double),
            TypeKind::Null {} if cast_position.is_none() => Ok(Self::Null),
            TypeKind::Integer { signed: true, .. } | TypeKind::Decimal { .. } if !ty.nullable => {
                let value = ScalarLiteral::resolve(ty, &literal.value)?;
                let mut digest = if matches!(
                    ty.kind,
                    TypeKind::Integer {
                        width: IntegerWidth::I32,
                        ..
                    }
                ) {
                    value.digest.clone()
                } else {
                    format!("{}:{}", value.digest, type_name::render(types, expr.ty)?)
                };
                if cast_position.is_some() {
                    digest = format!("CAST({digest}):DOUBLE NOT NULL");
                }
                Ok(Self::Exact(Box::new(ExactArgument {
                    ty: ty.clone(),
                    value,
                    digest,
                    cast_position,
                })))
            }
            _ => Err(Error("unsupported trigonometric argument".into())),
        }
    }

    pub(super) fn is_null(&self) -> bool {
        match self {
            Self::Double(n) => n.is_null(),
            Self::Null => true,
            Self::Exact(_) => false,
        }
    }

    pub(super) fn digest(&self) -> String {
        match self {
            Self::Double(n) => n.digest(),
            Self::Null => "null:NULL".into(),
            Self::Exact(arg) => arg.digest.clone(),
        }
    }

    pub(super) fn sql(&self) -> String {
        match self {
            Self::Double(n) => n.sql(),
            Self::Null => "NULL".into(),
            Self::Exact(arg) if arg.cast_position.is_some() => {
                format!("CAST({} AS DOUBLE)", arg.value.sql)
            }
            Self::Exact(arg) => arg.value.sql.clone(),
        }
    }

    pub(super) fn lower(&self, ids: &mut Ids, position: &Value) -> Result<Value, Error> {
        match self {
            Self::Double(n) => Ok(n.lower(ids)),
            Self::Null => Ok(DoubleLiteral::null().lower(ids)),
            Self::Exact(arg) => {
                let ExactArgument {
                    ty,
                    value,
                    cast_position,
                    ..
                } = arg.as_ref();
                let mut scalar = ColumnLayout::scalar(ty)?.node(ids);
                let literal = value.expression(ids, ty, &mut scalar, None)?;
                let target = if cast_position.is_some() {
                    ids.singleton(CircuitType::Double, "DOUBLE", false)
                } else {
                    // Implicit casts inherit the SQL source kind of their operand's DBSP type.
                    let mut node = if matches!(ty.kind, TypeKind::Decimal { .. }) {
                        ids.node("DBSPTypeDouble", &range(0, 0, 0, 0))
                    } else {
                        ids.generated("DBSPTypeDouble")
                    };
                    node["code"] = "DOUBLE".into();
                    node["mayBeNull"] = false.into();
                    node
                };
                Ok(cast(
                    ids,
                    cast_position.as_ref().unwrap_or(position),
                    literal,
                    target,
                ))
            }
        }
    }
}
