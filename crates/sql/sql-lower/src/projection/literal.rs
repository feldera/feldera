//! Rex literal digests include types unless Calcite can infer them from the spelling.

use crate::{
    source_table::{ColumnLayout, Ids},
    values::{scalar::ScalarLiteral, type_name},
    Error,
};
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    literals::LiteralValue,
    program::Program,
    types::{IntegerWidth, Precision, SqlType, TypeKind},
};
use serde_json::Value;

pub(crate) struct ProjectedLiteral {
    pub(super) ty: SqlType,
    pub(super) value: ScalarLiteral,
    type_text: String,
    pub(super) digest: String,
}

impl ProjectedLiteral {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected a scalar literal".into()));
        };
        let types = &program.table.type_table;
        let ty = &types.types[expr.ty.0 as usize];
        let value = ScalarLiteral::resolve(ty, &literal.value)?;
        let mut type_text = type_name::render(types, expr.ty)?;
        if matches!(ty.kind, TypeKind::Character { .. }) {
            type_text.push_str(" CHARACTER SET \"UTF-8\"");
        }
        let skip_type = !ty.nullable
            && match (&ty.kind, &literal.value) {
                (
                    TypeKind::Boolean {}
                    | TypeKind::Date {}
                    | TypeKind::Integer {
                        width: IntegerWidth::I32,
                        signed: true,
                    }
                    | TypeKind::Time {
                        precision: Precision::Specified(0),
                        ..
                    }
                    | TypeKind::Timestamp {
                        precision: Precision::Specified(0),
                        ..
                    },
                    _,
                ) => true,
                (
                    TypeKind::Character {
                        fixed: true,
                        length: Precision::Specified(n),
                        ..
                    },
                    LiteralValue::Character { value },
                ) => value.encode_utf16().count() == *n as usize,
                _ => false,
            };
        let digest = if skip_type {
            value.digest.clone()
        } else {
            format!("{}:{type_text}", value.digest)
        };
        Ok(Self {
            ty: ty.clone(),
            value,
            type_text,
            digest,
        })
    }

    pub(super) fn sql(&self) -> String {
        if self.ty.nullable && !matches!(self.ty.kind, TypeKind::Null {}) {
            self.cast_sql()
        } else {
            self.value.sql.clone()
        }
    }

    pub(super) fn cast_sql(&self) -> String {
        let ty = self.type_text.replace('"', "`");
        format!("CAST({} AS {ty})", self.value.sql)
    }

    pub(super) fn cast_digest(&self) -> String {
        format!("CAST({}):{}", self.digest, self.type_text)
    }

    pub(super) fn lower(&self, ids: &mut Ids) -> Result<Value, Error> {
        let mut ty = ColumnLayout::scalar(&self.ty)?.node(ids);
        self.value.expression(ids, &self.ty, &mut ty, None)
    }
}
