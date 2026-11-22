//! Index a temporal window by its timestamp and reconstruct its input row.
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, NodeId, TypeId};

pub struct TemporalWindowProjection {
    pub index: ExpressionId,
    /// A computed key leaves the entire row in the value and needs only a deindex.
    pub restore: Option<ExpressionId>,
}

pub(crate) fn key_column(edit: &CircuitEditor, key: ExpressionId, name: &str) -> Result<Option<usize>, Error> {
    if edit.expr_kind(key)? != Expr::Field {
        return Ok(None);
    }
    let field = edit.node(key.into())?;
    let base = edit.expression(field.edge("expression")?)?;
    if edit.expr_kind(base)? != Expr::Deref {
        return Ok(None);
    }
    let row = edit.expression(edit.node(base.into())?.edge("expression")?)?;
    if edit.expr_kind(row)? != Expr::VariablePath
        || edit
            .node(row.into())?
            .data("variable")
            .and_then(|v| v.as_str())
            != Some(name)
    {
        return Ok(None);
    }
    let column = field
        .data("fieldNo")
        .and_then(|v| v.as_u64())
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| Error::new("invalid window key column"))?;
    Ok(Some(column))
}

pub fn window_projection(
    edit: &mut CircuitEditor,
    parameter: NodeId,
    row: TypeId,
    key: ExpressionId,
    key_type: TypeId,
    next: &mut u64,
) -> Result<TemporalWindowProjection, Error> {
    let param = edit.node(parameter)?;
    let reference = edit.type_id(param.edge("type")?)?;
    let name = param
        .data("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| Error::new("missing window row parameter"))?
        .to_owned();
    let column = key_column(edit, key, &name)?;
    let fields = edit.node(row.into())?.edges("tupFields")?;
    let read_row = |edit: &mut CircuitEditor| {
        let path = edit.named_variable(reference, &name)?;
        edit.dereference(path)
    };
    let value = match column {
        Some(column) => {
            if column >= fields.len() {
                return Err(Error::new("window key column outside row"));
            }
            let mut values = Vec::with_capacity(fields.len() - 1);
            for i in (0..fields.len()).filter(|&i| i != column) {
                let row = read_row(edit)?;
                let field = edit.tuple_field(row, i)?;
                values.push(edit.clone_if_needed(field)?);
            }
            edit.tuple_expr(key.into(), false, &values)?
        }
        None => {
            let row = read_row(edit)?;
            edit.clone_value(row)?
        }
    };
    let owned_key = edit.cast(key.into(), key, key_type)?;
    let body = edit.tuple_expr(key.into(), true, &[owned_key, value])?;
    let index = edit.closure_with_parameters(body, &[parameter])?;
    let Some(column) = column else {
        return Ok(TemporalWindowProjection {
            index,
            restore: None,
        });
    };

    let k = edit.reference_type(key_type)?;
    let v = edit.reference_type(edit.expression_type(value)?)?;
    let pair = edit.tuple_type(row.into(), true, [k.into(), v.into()])?;
    let pair = edit.variable(pair, next)?;
    let mut restored = Vec::with_capacity(fields.len());
    for (i, ty) in fields.into_iter().enumerate() {
        let field = if i == column {
            let k = edit.tuple_field(pair, 0)?;
            let k = edit.dereference(k)?;
            let k = edit.clone_if_needed(k)?;
            edit.cast(key.into(), k, edit.type_id(ty)?)?
        } else {
            let v = edit.tuple_field(pair, 1)?;
            let v = edit.dereference(v)?;
            let field = edit.tuple_field(v, if i < column { i } else { i - 1 })?;
            edit.clone_if_needed(field)?
        };
        restored.push(field);
    }
    let body = edit.tuple_with_type(row, &restored)?;
    let restore = edit.closure_from_variables(body, &[pair])?;
    Ok(TemporalWindowProjection {
        index,
        restore: Some(restore),
    })
}
