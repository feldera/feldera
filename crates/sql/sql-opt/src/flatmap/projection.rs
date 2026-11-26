use super::link;
use feldera_sql_expr::{apply_closure, copy_expression, SimplifyWarning};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId, TypeId,
    TypeKind as Ty,
};

pub(super) struct Element {
    pub(super) var: ExpressionId,
    ty: TypeId,
    ordinal: Option<ExpressionId>,
}

impl Element {
    pub(super) fn new(
        edit: &mut CircuitEditor,
        site: NodeId,
        ty: TypeId,
        ordinal: Option<TypeId>,
        next: &mut u64,
    ) -> Result<Self, Error> {
        let Some(index_type) = ordinal else {
            return Ok(Self {
                var: edit.variable(ty, next)?,
                ty,
                ordinal: None,
            });
        };
        let usize = edit.standard_type(site, Ty::USize)?;
        let pair = edit.tuple_type(site, true, [usize.into(), ty.into()])?;
        let var = edit.variable(pair, next)?;
        let index = edit.tuple_field(var, 0)?;
        let one = edit.make_expr(
            usize.into(),
            Expr::USizeLiteral,
            usize,
            [("value", Field::Data("1".into()))],
        )?;
        // SQL ordinals start at one. Do the addition in usize before casting to the SQL type.
        let ord = edit.make_expr(
            site,
            Expr::Binary,
            usize,
            [
                ("left", link(index)),
                ("right", link(one)),
                ("opcode", Field::Data("ADD".into())),
            ],
        )?;
        Ok(Self {
            var,
            ty,
            ordinal: Some(edit.cast(site, ord, index_type)?),
        })
    }

    fn value(&self, edit: &mut CircuitEditor) -> Result<ExpressionId, Error> {
        if self.ordinal.is_some() {
            edit.tuple_field(self.var, 1)
        } else {
            Ok(self.var)
        }
    }

    fn fields(
        &self,
        edit: &mut CircuitEditor,
        base: ExpressionId,
        copy_base: bool,
    ) -> Result<Vec<ExpressionId>, Error> {
        let count = edit.node(self.ty.into())?.edges("tupFields")?.len();
        (0..count)
            .map(|i| {
                let row = if copy_base {
                    let copy = copy_expression(edit, base.into())?;
                    edit.expression(copy)?
                } else {
                    base
                };
                let field = edit.tuple_field(row, i)?;
                edit.clone_if_needed(field)
            })
            .collect()
    }

    pub(super) fn columns(
        &self,
        edit: &mut CircuitEditor,
        site: NodeId,
        post: Option<&[NodeId]>,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Vec<ExpressionId>, Error> {
        let kind = edit.type_kind(self.ty)?;
        let tuple = matches!(kind, Ty::Tuple | Ty::RawTuple);
        if let Some(post) = post {
            let nullable = edit.nullable(self.ty)? && self.ordinal.is_none();
            return post
                .iter()
                .map(|&closure| {
                    let base = self.value(edit)?;
                    let mut fields = if tuple {
                        self.fields(edit, base, true)?
                    } else {
                        vec![edit.clone_if_needed(base)?]
                    };
                    fields.extend(self.ordinal);
                    let value = edit.tuple_expr(site, false, &fields)?;
                    let value = if nullable {
                        let ty = edit.with_nullability(edit.expression_type(value)?, true)?;
                        edit.tuple_with_type(ty, &fields)?
                    } else {
                        value
                    };
                    let arg = edit.borrow_value(value)?;
                    let result = apply_closure(edit, closure, &[arg], warnings)?;
                    edit.clone_if_needed(result)
                })
                .collect();
        }
        let element = self.value(edit)?;
        let mut values = if tuple && (self.ordinal.is_none() || kind == Ty::Tuple) {
            self.fields(edit, element, false)?
        } else if self.ordinal.is_some() {
            vec![edit.clone_if_needed(element)?]
        } else {
            vec![element]
        };
        values.extend(self.ordinal);
        Ok(values)
    }
}
