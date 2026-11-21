use super::link;
use feldera_sql_expr::{SimplifyWarning, apply_closure, simplify_closure_with_warnings};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

/// Construct the owned collection returned by a nullable UNNEST on a null input.
fn empty_collection(edit: &mut CircuitEditor, ty: TypeId) -> Result<ExpressionId, Error> {
    let result = edit.collection_storage_type(ty, false)?;
    if edit.type_kind(result)? == Ty::Vec {
        let path = edit.function_path(result.into(), "vec!")?;
        return edit.make_expr(
            path.into(),
            Expr::Apply,
            result,
            [
                ("function", link(path)),
                ("arguments", Field::Array(vec![])),
            ],
        );
    }
    let args = edit.node(result.into())?.edges("typeArgs")?;
    let generics = Field::Array(args.into_iter().map(link).collect());
    let mut segments = Vec::new();
    for (identifier, args) in [("BTreeMap", generics), ("new", Field::Array(vec![]))] {
        let segment = edit.empty_origin_node(
            result.into(),
            InnerKind::Other("DBSPSimplePathSegment".into()),
            [
                ("identifier", Field::Data(identifier.into())),
                ("genericArgs", args),
            ],
        )?;
        segments.push(link(segment));
    }
    let path = edit.empty_origin_node(
        result.into(),
        InnerKind::Other("DBSPPath".into()),
        [("components", Field::Array(segments))],
    )?;
    let function = edit.make_expr(result.into(), Expr::Path, result, [("path", link(path))])?;
    let value = edit.inner_at(
        result.into(),
        InnerKind::Expression(Expr::Constructor),
        [
            ("type", link(result)),
            ("function", link(function)),
            ("arguments", Field::Array(vec![])),
        ],
    )?;
    edit.expression(value)
}

pub(super) fn extract(
    edit: &mut CircuitEditor,
    site: NodeId,
    closure: NodeId,
    row: ExpressionId,
    statements: &mut Vec<Field>,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let value = apply_closure(edit, closure, &[row], warnings)?;
    let cloned = edit.clone_value(value)?;
    let simplified = simplify_closure_with_warnings(edit, cloned.into(), Some(warnings))?;
    let collection = edit.expression(simplified)?;
    statements.push(link(edit.let_statement("collection", collection)?));
    let ty = edit.expression_type(collection)?;
    let var = edit.named_variable(ty, "collection")?;
    let owned = if edit.nullable(ty)? {
        let condition = edit.null_test(var)?;
        let empty = empty_collection(edit, ty)?;
        let var = edit.named_variable(ty, "collection")?;
        let required = edit.with_nullability(ty, false)?;
        let inner = edit.make_expr(
            collection.into(),
            Expr::Unwrap,
            required,
            [
                ("expression", link(var)),
                (
                    "message",
                    Field::Data("Collection UNNESTed should not be NULL".into()),
                ),
            ],
        )?;
        let contents = edit.dereference(inner)?;
        let contents = edit.clone_value(contents)?;
        edit.make_expr(
            site,
            Expr::If,
            edit.expression_type(empty)?,
            [
                ("condition", link(condition)),
                ("positive", link(empty)),
                ("negative", link(contents)),
            ],
        )?
    } else {
        let contents = edit.dereference(var)?;
        edit.clone_value(contents)?
    };
    statements.push(link(edit.let_statement("collection_clone", owned)?));
    edit.named_variable(edit.expression_type(owned)?, "collection_clone")
}
