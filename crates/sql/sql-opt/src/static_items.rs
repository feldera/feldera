//! Name static initializers and construct the declarations referenced by generated code.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, NodeId,
};
use sha2::{Digest, Sha256};

/// The classic printer renders the initializer and its type in the operator's context.
pub fn static_initializer(
    edit: &mut CircuitEditor,
    origin: NodeId,
    initializer: ExpressionId,
    rendered: &[u8],
) -> Result<ExpressionId, Error> {
    let digest = Sha256::digest(rendered);
    let prefix = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has 32 bytes"));
    let name = format!("STATIC_{prefix:016x}");
    let ty = edit.expression_type(initializer)?;
    edit.make_expr(
        origin,
        ExpressionKind::Static,
        ty,
        [
            ("initializer", Field::Reference(initializer.into())),
            ("name", Field::Data(name.into())),
        ],
    )
}

pub fn static_item(
    edit: &mut CircuitEditor,
    expression: ExpressionId,
) -> Result<(NodeId, ExpressionId), Error> {
    let id = expression.into();
    let name = edit
        .node(id)?
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("a static declaration needs a name"))?
        .to_owned();
    let ty = edit.expression_type(expression)?;
    let item = edit.inner_at(
        id,
        InnerKind::Other("DBSPStaticItem".into()),
        [("expression", Field::Reference(id))],
    )?;
    Ok((item, edit.named_path(ty, &name)?))
}
