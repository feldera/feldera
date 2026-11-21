//! Build the row predicate and error payload for a LATENESS filter.

use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

pub(super) fn arg(
    edit: &mut CircuitEditor,
    ty: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let reference = edit.reference_type(ty)?;
    edit.variable(reference, next)
}

fn node(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

pub(super) fn bool_type(edit: &mut CircuitEditor, sample: NodeId) -> Result<TypeId, Error> {
    let ty = edit.empty_origin_node(
        sample,
        InnerKind::Type(Ty::Bool),
        [
            ("code", Field::Data("BOOL".into())),
            ("mayBeNull", Field::Data(false.into())),
        ],
    )?;
    edit.type_id(ty)
}

pub(super) fn truth(edit: &mut CircuitEditor, sample: NodeId) -> Result<ExpressionId, Error> {
    let ty = bool_type(edit, sample)?;
    edit.make_expr(
        ty.into(),
        Expr::BoolLiteral,
        ty,
        [("value", Field::Data(true.into()))],
    )
}

fn binding(
    edit: &mut CircuitEditor,
    name: &str,
    value: ExpressionId,
) -> Result<(NodeId, ExpressionId), Error> {
    let reference = edit.reference_type(edit.expression_type(value)?)?;
    let borrow = edit.make_expr(
        value.into(),
        Expr::Borrow,
        reference,
        [
            ("expression", node(value)),
            ("mut", Field::Data(false.into())),
        ],
    )?;
    let declaration = edit.empty_origin_node(
        value.into(),
        InnerKind::Other("DBSPLetStatement".into()),
        [
            ("type", node(reference)),
            ("initializer", node(borrow)),
            ("mutable", Field::Data(false.into())),
            ("variable", Field::Data(name.into())),
        ],
    )?;
    let path = edit.named_variable(reference, name)?;
    Ok((declaration, edit.dereference(path)?))
}

fn compare_fields(
    edit: &mut CircuitEditor,
    mut predicate: ExpressionId,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<ExpressionId, Error> {
    let ty = edit.expression_type(left)?;
    let other = edit.expression_type(right)?;
    match edit.type_kind(ty)? {
        Ty::Tuple | Ty::RawTuple => {
            let size = edit.node(ty.into())?.edges("tupFields")?.len();
            if !matches!(edit.type_kind(other)?, Ty::Tuple | Ty::RawTuple)
                || size != edit.node(other.into())?.edges("tupFields")?.len()
            {
                return Err(Error::new(
                    "waterline comparison requires tuples of equal width",
                ));
            }
            for index in 0..size {
                let a = edit.tuple_field(left, index)?;
                let b = edit.tuple_field(right, index)?;
                predicate = compare_fields(edit, predicate, a, b)?;
            }
            Ok(predicate)
        }
        Ty::Ref => {
            let a = edit.dereference(left)?;
            let b = edit.dereference(right)?;
            compare_fields(edit, predicate, a, b)
        }
        Ty::Binary
        | Ty::Bool
        | Ty::Date
        | Ty::Decimal
        | Ty::Double
        | Ty::GeoPoint
        | Ty::Integer
        | Ty::LongInterval
        | Ty::Null
        | Ty::Real
        | Ty::ShortInterval
        | Ty::String
        | Ty::Time
        | Ty::Timestamp
        | Ty::TimestampTz
        | Ty::USize
        | Ty::Uuid
        | Ty::Variant => {
            if !edit.same_type_ignoring_nullability(ty, other)? {
                return Err(Error::new(
                    "waterline comparison requires matching scalar types",
                ));
            }
            let boolean = edit.standard_type(left.into(), Ty::Bool)?;
            let cmp = edit.make_expr(
                boolean.into(),
                Expr::Binary,
                boolean,
                [
                    ("left", node(left)),
                    ("right", node(right)),
                    ("opcode", Field::Data("CONTROLLED_FILTER_GTE".into())),
                ],
            )?;
            let ty = bool_type(edit, left.into())?;
            edit.make_expr(
                ty.into(),
                Expr::Binary,
                ty,
                [
                    ("left", node(predicate)),
                    ("right", node(cmp)),
                    ("opcode", Field::Data("AND".into())),
                ],
            )
        }
        kind => Err(Error::new(format!(
            "unsupported waterline comparison type {kind:?}"
        ))),
    }
}

pub(super) fn tuple_compare(
    edit: &mut CircuitEditor,
    left: ExpressionId,
    right: ExpressionId,
) -> Result<ExpressionId, Error> {
    let (a, left) = binding(edit, "left", left)?;
    let (b, right) = binding(edit, "right", right)?;
    let first = truth(edit, left.into())?;
    let result = compare_fields(edit, first, left, right)?;
    let ty = edit.expression_type(result)?;
    let block = edit.empty_origin_node(
        left.into(),
        InnerKind::Expression(Expr::Block),
        [
            ("contents", Field::Array(vec![node(a), node(b)])),
            ("lastExpression", node(result)),
            ("type", node(ty)),
        ],
    )?;
    edit.expression(block)
}

fn path(edit: &mut CircuitEditor, ty: TypeId, name: &str) -> Result<ExpressionId, Error> {
    let segment = edit.empty_origin_node(
        ty.into(),
        InnerKind::Other("DBSPSimplePathSegment".into()),
        [
            ("identifier", Field::Data(name.into())),
            ("genericArgs", Field::Array(vec![])),
        ],
    )?;
    let path = edit.empty_origin_node(
        ty.into(),
        InnerKind::Other("DBSPPath".into()),
        [("components", Field::Array(vec![node(segment)]))],
    )?;
    edit.make_expr(ty.into(), Expr::Path, ty, [("path", node(path))])
}

pub(super) fn late_error(
    edit: &mut CircuitEditor,
    table: &str,
    data: ExpressionId,
) -> Result<ExpressionId, Error> {
    let string = edit.standard_type(data.into(), Ty::String)?;
    let str_ty = edit.standard_type(data.into(), Ty::Str)?;
    let any = edit.standard_type(data.into(), Ty::Any)?;
    let mut fields = Vec::new();
    for text in [table, "Late row"] {
        fields.push(edit.make_expr(
            string.into(),
            Expr::StringLiteral,
            string,
            [
                ("charset", Field::Data("UTF-8".into())),
                ("value", Field::Data(text.into())),
            ],
        )?);
    }
    let debug = edit.make_expr(
        str_ty.into(),
        Expr::StrLiteral,
        str_ty,
        [
            ("value", Field::Data("{:?}".into())),
            ("raw", Field::Data(false.into())),
        ],
    )?;
    let format = path(edit, any, "format!")?;
    let formatted = edit.make_expr(
        any.into(),
        Expr::Apply,
        any,
        [
            ("function", node(format)),
            ("arguments", Field::Array(vec![node(debug), node(data)])),
        ],
    )?;
    let into = path(edit, any, "into")?;
    fields.push(edit.make_expr(
        any.into(),
        Expr::ApplyMethod,
        string,
        [
            ("self", node(formatted)),
            ("function", node(into)),
            ("arguments", Field::Array(vec![])),
        ],
    )?);
    edit.tuple_expr(data.into(), false, &fields)
}

/// The predicate and error-row closures used by a LATENESS input or view.
pub struct RowFilter {
    pub function: ExpressionId,
    pub error: ExpressionId,
    pub(super) error_type: TypeId,
}

/// Compare the declared columns, in row order, against the compacted waterline.
pub fn row_filter(
    edit: &mut CircuitEditor,
    table: &str,
    row: TypeId,
    columns: &[usize],
    waterline: TypeId,
    next: &mut u64,
) -> Result<RowFilter, Error> {
    let data = arg(edit, row, next)?;
    let source = edit.dereference(data)?;
    let fields = columns
        .iter()
        .map(|&i| edit.tuple_field(source, i))
        .collect::<Result<Vec<_>, _>>()?;
    let projected = edit.tuple_expr(source.into(), false, &fields)?;
    let control = arg(edit, waterline, next)?;
    let bound = edit.dereference(control)?;
    let predicate = tuple_compare(edit, projected, bound)?;
    let function = edit.closure_from_variables(predicate, &[control, data])?;

    let row_arg = edit.variable(edit.expression_type(data)?, next)?;
    let empty = edit.standard_type(data.into(), Ty::RawTuple)?;
    let value = arg(edit, empty, next)?;
    let weight_type = edit.standard_type(data.into(), Ty::Weight)?;
    let weight = edit.variable(weight_type, next)?;
    let body = late_error(edit, table, row_arg)?;
    let error = edit.closure_from_variables(body, &[control, row_arg, value, weight])?;
    let error_type = edit.expression_type(body)?;
    Ok(RowFilter {
        function,
        error,
        error_type,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn row_filter_checks_waterline_width_and_leaf_types() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": []
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class": "DBSPTypeInteger", "id": 1, "width": 32, "signed": true,
            "mayBeNull": false, "code": "INT32"
        })])?;
        let mut edit = CircuitEditor::new(circuit);
        let int = edit.type_id(roots[0])?;
        let nullable_int = edit.with_nullability(int, true)?;
        let boolean = edit.bool_type(int.into(), false)?;
        let row = edit.tuple_type(int.into(), false, [nullable_int.into()])?;
        let mut next = 0;
        let control = edit.tuple_type(int.into(), false, [int.into()])?;
        row_filter(&mut edit, "t", row, &[0], control, &mut next)?;

        let wider = edit.tuple_type(int.into(), false, [int.into(), int.into()])?;
        let different = edit.tuple_type(int.into(), false, [boolean.into()])?;
        for (bound, error) in [
            (wider, "waterline comparison requires tuples of equal width"),
            (
                different,
                "waterline comparison requires matching scalar types",
            ),
        ] {
            let result = row_filter(&mut edit, "t", row, &[0], bound, &mut next);
            assert_eq!(result.map(|_| ()).unwrap_err().to_string(), error);
        }
        let any = edit.standard_type(int.into(), Ty::Any)?;
        let unresolved = edit.tuple_type(int.into(), false, [any.into()])?;
        let result = row_filter(&mut edit, "t", unresolved, &[0], unresolved, &mut next);
        assert_eq!(
            result.map(|_| ()).unwrap_err().to_string(),
            "unsupported waterline comparison type Any"
        );
        Ok(())
    }
}
