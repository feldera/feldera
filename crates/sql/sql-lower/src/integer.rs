//! Signed Rex literals carry their coerced width into DBSP and plan digests.

use super::{source_table::Ids, Error};
use feldera_sql_rel_model::{
    expression_objects::ObjectKind,
    expressions::ExprId,
    literals::LiteralValue,
    program::Program,
    types::{IntegerWidth, TypeKind},
};
use serde_json::Value;

#[derive(Clone, Copy)]
pub(super) struct SignedInteger {
    pub value: i64,
    pub width: IntegerWidth,
}

impl SignedInteger {
    pub(super) fn resolve(program: &Program, id: ExprId) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        let ObjectKind::Literal { literal } = &expr.kind else {
            return Err(Error("expected an integer literal".into()));
        };
        let LiteralValue::Integer { value } = &literal.value else {
            return Err(Error("expected an integer literal".into()));
        };
        let TypeKind::Integer {
            width,
            signed: true,
        } = program.table.type_table.types[literal.ty.0 as usize].kind
        else {
            return Err(Error("expected a signed integer literal".into()));
        };
        let value = value
            .parse()
            .map_err(|_| Error("signed integer literal outside range".into()))?;
        Ok(Self { value, width })
    }

    pub(super) fn sql_type(self) -> &'static str {
        self.width.signed_sql_name()
    }

    pub(super) fn digest(self) -> String {
        if self.width == IntegerWidth::I32 {
            self.value.to_string()
        } else {
            format!("{}:{}", self.value, self.sql_type())
        }
    }

    pub(super) fn lower(self, ids: &mut Ids) -> Value {
        let bits = self.width.bits();
        let mut ty = ids.generated("DBSPTypeInteger");
        ty["width"] = bits.into();
        ty["signed"] = true.into();
        ty["code"] = format!("INT{bits}").into();
        ty["mayBeNull"] = false.into();
        let mut literal = ids.generated(&format!("DBSPI{bits}Literal"));
        literal["type"] = ty;
        literal["value"] = self.value.to_string().into();
        literal
    }
}
