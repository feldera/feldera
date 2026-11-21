//! Translate SQL array indexing into the signed, zero-based offset used by the runtime.
use crate::{casts::simplify_scalar, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, NodeId,
};

struct AdjustIndex;

impl Transformation for AdjustIndex {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        Ok(
            if matches!(editor.node(id)?.inner_kind(), Some(InnerKind::Type(_))) {
                Visit::Keep
            } else {
                Visit::Children
            },
        )
    }

    fn postorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = editor.node(id)?;
        if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Binary))) {
            return Ok(id);
        }
        let one_based = match node.data("opcode").and_then(|op| op.as_str()) {
            Some("SQL_INDEX") => true,
            Some("SAFE_RUST_INDEX") => false,
            _ => return Ok(id),
        };
        let offset = editor.expression(node.edge("right")?)?;
        let ty = editor.expression_type(offset)?;
        let offset = if one_based {
            let one = editor.index_one(ty)?;
            editor.binary_expression(id, ty, BinaryOp::Sub, offset, one)?
        } else {
            offset
        };
        let target = editor.isize_type(ty)?;
        let index = if editor.is_null_literal(offset)? {
            editor.typed_null(target)?
        } else {
            editor.cast(id, offset, target)?
        };
        let mut index = simplify_scalar(editor, index.into())?;
        // Simplify.castInteger attributes a folded literal to the original
        // operand of the cast, even when subtraction folded to a new literal.
        if let Some(value) = editor.node(index)?.data("value").cloned() {
            if matches!(
                editor.node(index)?.inner_kind(),
                Some(InnerKind::Expression(Expr::ISizeLiteral))
            ) {
                index = editor
                    .make_expr(
                        offset.into(),
                        Expr::ISizeLiteral,
                        target,
                        [("value", Field::Data(value))],
                    )?
                    .into();
            }
        }
        editor.derive_inner(
            id,
            [
                ("right", Field::Reference(index)),
                ("opcode", Field::Data("RUST_INDEX".into())),
            ],
        )
    }
}

pub fn adjust_sql_index(circuit: Circuit) -> Result<Circuit, Error> {
    CircuitEditor::new(circuit).rewrite_inner_roots(|editor, id| {
        Rewrite::translating_expressions(AdjustIndex).apply(editor, id)
    })
}
