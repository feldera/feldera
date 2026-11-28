//! Unreduced trigonometric calls convert an exact argument to DOUBLE at runtime.

use super::{interval_count::cast, literal::ProjectedLiteral};
use crate::{
    source_table::{expression_range, range, Ids},
    Error,
};
use feldera_sql_ir::TypeKind as CircuitType;
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    operators::OperatorObject,
    program::Program,
    types::{FloatFlavor, TypeKind},
};
use serde_json::Value;

pub(super) struct TrigonometricArgument {
    literal: ProjectedLiteral,
    cast_position: Option<Value>,
}

impl TrigonometricArgument {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let types = &program.table.type_table;
        let expr = &program.expressions.expressions[id.0 as usize];
        let (input, cast_position) = match &expr.kind {
            ObjectKind::Call {
                operator,
                arguments,
            } => {
                let op = &program.expressions.operators[operator.0 as usize];
                let builtin = &program.expressions.operator_objects[op.object.0 as usize];
                let [source] = arguments.as_slice() else {
                    return Err(Error("expected a unary numeric cast".into()));
                };
                let ty = &types.types[expr.ty.0 as usize];
                if !matches!(builtin, OperatorObject::Builtin { registry, registry_version, key }
                    if registry == "calcite-s1-fixture" && registry_version == "1" && key == "SqlStdOperatorTable.CAST")
                    || !matches!(
                        ty.kind,
                        TypeKind::Float {
                            flavor: FloatFlavor::Double
                        }
                    )
                    || ty.nullable
                {
                    return Err(Error("expected a required DOUBLE cast".into()));
                }
                (*source, Some(expression_range(&expr.position)))
            }
            _ => (id, None),
        };
        let literal = ProjectedLiteral::resolve(program, input)?;
        match literal.ty.kind {
            TypeKind::Float {
                flavor: FloatFlavor::Double,
            }
            | TypeKind::Null {}
                if cast_position.is_none() => {}
            TypeKind::Integer { signed: true, .. } | TypeKind::Decimal { .. }
                if !literal.ty.nullable => {}
            _ => return Err(Error("unsupported trigonometric argument".into())),
        }
        Ok(Self {
            literal,
            cast_position,
        })
    }

    pub(super) fn is_null(&self) -> bool {
        self.literal.ty.nullable
    }

    pub(super) fn digest(&self) -> String {
        if self.cast_position.is_some() {
            format!("CAST({}):DOUBLE NOT NULL", self.literal.digest)
        } else {
            self.literal.digest.clone()
        }
    }

    pub(super) fn sql(&self) -> String {
        if self.cast_position.is_some() {
            format!("CAST({} AS DOUBLE)", self.literal.value.sql)
        } else {
            self.literal.value.sql.clone()
        }
    }

    pub(super) fn lower(&self, ids: &mut Ids, position: &Value) -> Result<Value, Error> {
        if matches!(self.literal.ty.kind, TypeKind::Null {}) {
            let mut literal = ids.generated("DBSPDoubleLiteral");
            literal["type"] = ids.singleton(CircuitType::Double, "DOUBLE", true);
            return Ok(literal);
        }
        let literal = self.literal.lower(ids)?;
        if matches!(self.literal.ty.kind, TypeKind::Float { .. }) {
            return Ok(literal);
        }
        let target = if self.cast_position.is_some() {
            ids.singleton(CircuitType::Double, "DOUBLE", false)
        } else {
            // An implicit cast inherits the source of its operand's DBSP type.
            let mut node = if matches!(self.literal.ty.kind, TypeKind::Decimal { .. }) {
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
            self.cast_position.as_ref().unwrap_or(position),
            literal,
            target,
        ))
    }
}
