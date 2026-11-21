//! Assign runtime error indices in first-use order within each operator.
use crate::{Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

use std::collections::HashMap;

#[derive(Default)]
pub struct ErrorSites {
    indices: HashMap<(i32, i32), usize>,
    in_operator: bool,
}

impl ErrorSites {
    pub fn for_operator() -> Self {
        Self {
            in_operator: true,
            ..Self::default()
        }
    }

    /// One expression root gets its own translation map. The indices span
    /// every root of the same operator.
    pub fn wrap(&mut self, editor: &mut CircuitEditor, root: NodeId) -> Result<NodeId, Error> {
        Rewrite::translating_expressions(WrapErrors(self)).apply(editor, root)
    }
}

struct WrapErrors<'a>(&'a mut ErrorSites);

impl Transformation for WrapErrors<'_> {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        crate::casts::visit_expression(editor, id)
    }

    fn postorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = editor.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Cast)) => {
                let safe = match node.data("safe").and_then(|v| v.as_str()) {
                    Some("UnwrapResultSafe") => true,
                    Some("UnwrapResultUnsafe") => false,
                    _ => return Ok(id),
                };
                let source = editor.expression(node.edge("source")?)?;
                let result = editor.expression_type(source)?;
                let parameters = editor.node(result.into())?.edges("typeArgs")?;
                let [ty] = parameters.as_slice() else {
                    return Err(Error::new("expected SqlResult<T>"));
                };
                let target = editor.type_id(*ty)?;
                if editor.type_kind(result)? != Ty::SqlResult
                    || !editor.same_type(target, editor.type_id(node.edge("type")?)?)?
                {
                    return Err(Error::new("result unwrap requires its wrapped type"));
                }
                self.0.handler(editor, id, source, target, safe)
            }
            Some(InnerKind::Expression(Expr::Unwrap)) => {
                let message = node
                    .data("message")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::new("unwrap requires a message"))?
                    .to_owned();
                let value = editor.expression(node.edge("expression")?)?;
                let owned = editor.clone_if_needed(value)?;
                let target = editor.with_nullability(editor.expression_type(owned)?, false)?;
                let unwrap = editor.make_expr(
                    id,
                    Expr::Unwrap,
                    target,
                    [
                        ("expression", Field::Reference(owned.into())),
                        ("message", Field::Data(message.clone().into())),
                    ],
                )?;
                if message.is_empty() {
                    Ok(if owned == value { id } else { unwrap.into() })
                } else {
                    self.0.handler(editor, id, unwrap, target, false)
                }
            }
            _ => Ok(id),
        }
    }
}

impl ErrorSites {
    fn handler(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
        safe: bool,
    ) -> Result<NodeId, Error> {
        let position = editor
            .node(origin)?
            .field("position")
            .map(Field::source_range)
            .transpose()?
            .unwrap_or_default();
        let allocated = if position.start_line_number > 0 && position.start_column > 0 {
            let next = self.indices.len();
            *self
                .indices
                .entry((position.start_line_number, position.start_column))
                .or_insert(next)
        } else {
            0
        };
        // Every located site reserves a slot. Only sources which can report
        // their position at runtime carry that slot into the emitted handler.
        let operand = editor.node(source.into())?;
        let index = if self.in_operator
            && operand
                .field("position")
                .map(Field::source_range)
                .transpose()?
                .is_some_and(|range| range.is_valid())
            && !matches!(
                operand.inner_kind(),
                Some(InnerKind::Expression(Expr::Binary))
            )
            && !(matches!(
                operand.inner_kind(),
                Some(InnerKind::Expression(Expr::Cast))
            ) && operand.data("safe").and_then(|v| v.as_str()) == Some("SqlSafe"))
        {
            allocated
        } else {
            0
        };
        let behavior = if safe {
            "ReturnNone"
        } else if self.in_operator && position.is_valid() {
            "PanicWithSource"
        } else {
            "Panic"
        };
        editor
            .make_expr(
                origin,
                Expr::HandleError,
                target,
                [
                    ("source", Field::Reference(source.into())),
                    ("runtimeBehavior", Field::Data(behavior.into())),
                    ("hasSourcePosition", Field::Data(self.in_operator.into())),
                    ("index", Field::Data(index.into())),
                ],
            )
            .map(Into::into)
    }
}
