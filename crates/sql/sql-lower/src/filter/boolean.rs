//! Read a Boolean field as a filter operand. A bare nullable field needs a
//! WRAP_BOOL at the filter boundary, while IS TRUE is an explicit SQL call.
use super::{range, reference, Error, Ids, Operand, RowParameter};
use feldera_sql_rel_model::{
    catalog::CatalogSchema, expression_objects::ObjectKind, expressions::ExprId, program::Program,
    types::TypeKind,
};
use serde_json::Value;

pub(super) struct BooleanField {
    index: usize,
    nullable: bool,
    is_true: bool,
}

impl BooleanField {
    pub(super) fn resolve(program: &Program, id: ExprId, is_true: bool) -> Result<Self, Error> {
        let expr = &program.expressions.expressions[id.0 as usize];
        let ty = &program.table.type_table.types[expr.ty.0 as usize];
        if let ObjectKind::InputRef { object } = expr.kind {
            if matches!(ty.kind, TypeKind::Boolean {}) {
                return Ok(Self {
                    index: program.table.objects[object.0 as usize].field as usize,
                    nullable: ty.nullable,
                    is_true,
                });
            }
        }
        Err(Error("expected a BOOLEAN field".into()))
    }

    pub(super) fn render(&self, input: &CatalogSchema) -> (String, String) {
        let (field, sql) = Operand::Column(self.index).render(input);
        if self.is_true {
            (format!("IS TRUE({field})"), format!("{sql} IS TRUE"))
        } else {
            (field, sql)
        }
    }

    pub(super) fn lower(
        &self,
        ids: &mut Ids,
        parameter: &mut RowParameter,
        position: &Value,
    ) -> Result<(Value, Value), Error> {
        let field = parameter.read(ids, self.index)?;
        let result = if self.nullable {
            let mut ty = ids.node("DBSPTypeBool", &range(0, 0, 0, 0));
            ty["code"] = "BOOL".into();
            ty["mayBeNull"] = false.into();
            ty
        } else {
            parameter.take_result_type(self.index)?
        };
        let (mut unary, opcode) = if self.is_true {
            (ids.node("DBSPUnaryExpression", position), "IS_TRUE")
        } else if self.nullable {
            (ids.generated("DBSPUnaryExpression"), "WRAP_BOOL")
        } else {
            return Ok((result, field));
        };
        unary["opcode"] = opcode.into();
        unary["source"] = field;
        unary["type"] = reference(&result)?;
        Ok((result, unary))
    }
}
