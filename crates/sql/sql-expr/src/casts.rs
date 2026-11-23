//! Expand SQL casts in expressions and prepare column initializers for the backend.

use super::{Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind as Ty,
};

mod arithmetic;
mod collections;
mod comparison;
pub use comparison::{decimal_literal_parts, literal_is_positive, numeric_value_is_positive, same_literal_value};
mod literals;
mod simplify;
pub(crate) use simplify::SimplifyDefault;
pub use simplify::SimplifyWarning;

pub(crate) fn simplify_scalar(editor: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    simplify_scalar_with_warnings(editor, id, None)
}

pub(crate) fn simplify_scalar_with_warnings(
    editor: &mut CircuitEditor,
    id: NodeId,
    warnings: Option<&mut Vec<SimplifyWarning>>,
) -> Result<NodeId, Error> {
    Rewrite::translating_expressions(SimplifyDefault { warnings }).apply(editor, id)
}

/// Prepare a default for the backend, advancing the compiler's name allocator on success.
pub fn prepare_column_default(
    editor: &mut CircuitEditor,
    default: ExpressionId,
    next_name: &mut u64,
) -> Result<ExpressionId, Error> {
    let mut root = simplify_scalar(editor, default.into())?;
    let mut name = *next_name;
    // Java allocates converter parameters for all safe casts before visiting unsafe casts.
    for safety in [CastSafety::Safe, CastSafety::Unsafe] {
        let mut converged = false;
        for _ in 0..100 {
            let result = expand_sql_casts(editor, root, safety, &mut name)?;
            if result == root {
                converged = true;
                break;
            }
            root = result;
        }
        if !converged {
            return Err(Error::new(
                "column initializer cast expansion did not converge",
            ));
        }
    }
    let wrapped = crate::ErrorSites::default().wrap(editor, root)?;
    let result = editor.expression(wrapped)?;
    *next_name = name;
    Ok(result)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum CastSafety {
    Safe,
    Unsafe,
}

/// Expand casts of the selected kind in one expression root. A new root starts
/// with an empty expression translation map and the same generated-name sequence.
pub fn expand_sql_casts(
    editor: &mut CircuitEditor,
    root: NodeId,
    safety: CastSafety,
    next: &mut u64,
) -> Result<NodeId, Error> {
    let mut rewrite = Rewrite::translating_expressions(ExpandCasts {
        next_name: *next,
        safe: safety == CastSafety::Safe,
    });
    let root = rewrite.apply(editor, root)?;
    *next = rewrite.rule.next_name;
    Ok(root)
}

pub(crate) fn visit_expression(editor: &CircuitEditor, id: NodeId) -> Result<Visit, Error> {
    Ok(match editor.node(id)?.inner_kind() {
        Some(InnerKind::Type(_)) => Visit::Keep,
        _ => Visit::Children,
    })
}

struct ExpandCasts {
    next_name: u64,
    safe: bool,
}

impl Transformation for ExpandCasts {
    fn rebuild_null_test(
        &mut self,
        editor: &mut CircuitEditor,
        node: NodeId,
    ) -> Result<NodeId, Error> {
        editor.rebuild_written_null_test(node)
    }

    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        visit_expression(editor, id)
    }

    fn postorder_rebuilt(
        &mut self,
        editor: &mut CircuitEditor,
        original: NodeId,
        id: NodeId,
    ) -> Result<NodeId, Error> {
        let node = editor.node(id)?;
        if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Cast))) {
            return Ok(id);
        }
        let safe = match node.data("safe").and_then(|value| value.as_str()) {
            Some("SqlSafe") => true,
            Some("SqlUnsafe") => false,
            Some("UnwrapResultSafe" | "UnwrapResultUnsafe" | "RustCast" | "Saturating") => {
                return Ok(id);
            }
            _ => return Err(Error::new("unknown SQL cast mode")),
        };
        if safe != self.safe {
            return Ok(id);
        }
        let source = editor.expression(node.edge("source")?)?;
        let target = editor.type_id(node.edge("type")?)?;
        let from = editor.expression_type(source)?;
        let source = if editor.same_type_ignoring_nullability(from, target)? {
            editor.expression(editor.node(original)?.edge("source")?)?
        } else {
            source
        };
        self.expand(editor, id, source, target, safe, 0)
    }
}

impl ExpandCasts {
    fn expand(
        &mut self,
        editor: &mut CircuitEditor,
        origin: NodeId,
        source: ExpressionId,
        target: TypeId,
        safe: bool,
        depth: usize,
    ) -> Result<NodeId, Error> {
        if depth > 128 {
            return Err(Error::new("SQL cast exceeds the nesting limit"));
        }
        let from = editor.expression_type(source)?;
        if safe && !editor.nullable(target)? {
            return Err(Error::new("SAFE_CAST requires a nullable result"));
        }
        if editor.same_type(from, target)? {
            return Ok(source.into());
        }
        if editor.same_type_ignoring_nullability(from, target)? {
            if editor.nullable(target)? {
                let owned = editor.clone_if_needed(source)?;
                return editor.some_at(owned.into(), owned).map(Into::into);
            }
            if editor.type_kind(target)? != Ty::Variant {
                let unwrapped = unwrap_option(
                    editor,
                    origin,
                    source,
                    "NULL value should be impossible here",
                )?;
                return editor
                    .clone_if_needed(editor.expression(unwrapped)?)
                    .map(Into::into);
            }
        }
        let from_kind = editor.type_kind(from)?;
        let to_kind = editor.type_kind(target)?;
        if safe && to_kind == Ty::Variant {
            // VARIANT conversion is lowered by the following unsafe-cast stage.
            return editor.cast(origin, source, target).map(Into::into);
        }
        if from_kind == Ty::Null && to_kind == Ty::Array {
            return editor.typed_null(target).map(Into::into);
        }
        if matches!(
            (from_kind, to_kind),
            (Ty::Array, Ty::Array) | (Ty::Map, Ty::Map)
        ) {
            return self.collection(editor, origin, source, target, safe, depth + 1);
        }
        if matches!(from_kind, Ty::RawTuple | Ty::Tuple | Ty::Variant)
            && matches!(to_kind, Ty::RawTuple | Ty::Tuple)
        {
            return self.row(editor, origin, source, target, safe, depth + 1);
        }
        if from_kind == Ty::Array && to_kind == Ty::Variant {
            let members = editor.node(from.into())?.edges("typeArgs")?;
            let [member] = members.as_slice() else {
                return Err(Error::new("ARRAY requires an element type"));
            };
            if editor.type_kind(editor.type_id(*member)?)? != Ty::Variant {
                let variant = editor.variant_type(origin, false)?;
                return self.array_to_variant(editor, origin, source, variant, target);
            }
        }
        if from_kind == Ty::Tuple && to_kind == Ty::Variant {
            return Self::record_to_variant(editor, origin, source, target);
        }
        if from_kind == Ty::Variant && to_kind == Ty::Array {
            let members = editor.node(target.into())?.edges("typeArgs")?;
            let [member] = members.as_slice() else {
                return Err(Error::new("ARRAY requires an element type"));
            };
            if matches!(
                editor.type_kind(editor.type_id(*member)?)?,
                Ty::Array | Ty::Map | Ty::Tuple
            ) {
                let variant = editor.variant_type(origin, false)?;
                let array = collections::array_type(editor, variant, editor.nullable(from)?)?;
                // The fixed-point driver expands this intermediate cast on its next iteration.
                let input = editor.make_expr(
                    origin,
                    Expr::Cast,
                    array,
                    [
                        ("source", Field::Reference(source.into())),
                        (
                            "safe",
                            Field::Data(if safe { "SqlSafe" } else { "SqlUnsafe" }.into()),
                        ),
                    ],
                )?;
                return self.convert_array(editor, origin, input, target, safe, depth + 1);
            }
        }
        if matches!(
            from_kind,
            Ty::Tuple | Ty::RawTuple | Ty::Struct | Ty::Array | Ty::Map
        ) || matches!(
            to_kind,
            Ty::Tuple | Ty::RawTuple | Ty::Struct | Ty::Array | Ty::Map
        ) {
            // ARRAY<VARIANT> and MAP conversions to/from VARIANT use the scalar runtime helper.
            if from_kind != Ty::Variant && to_kind != Ty::Variant {
                return Err(Error::new("unsupported compound SQL cast"));
            }
        }
        let result_type = sql_result(editor, target)?;
        let converted = editor.make_expr(
            origin,
            Expr::Cast,
            result_type,
            [
                ("source", Field::Reference(source.into())),
                ("safe", Field::Data("RustCast".into())),
            ],
        )?;
        // Conversion to VARIANT uses the unsafe path even for SAFE_CAST.
        handle_result(
            editor,
            origin,
            converted,
            target,
            safe && to_kind != Ty::Variant,
        )
    }
}

fn sql_result(editor: &mut CircuitEditor, target: TypeId) -> Result<TypeId, Error> {
    let ty = editor.inner_at(
        target.into(),
        InnerKind::Type(Ty::SqlResult),
        [
            ("name", Field::Data("SqlResult".into())),
            ("code", Field::Data("RESULT".into())),
            ("mayBeNull", Field::Data(false.into())),
            (
                "typeArgs",
                Field::Array(vec![Field::Reference(target.into())]),
            ),
        ],
    )?;
    editor.type_id(ty)
}

fn handle_result(
    editor: &mut CircuitEditor,
    origin: NodeId,
    source: ExpressionId,
    target: TypeId,
    safe: bool,
) -> Result<NodeId, Error> {
    let ty = editor.expression_type(source)?;
    if editor.type_kind(ty)? != Ty::SqlResult {
        return Err(Error::new("result unwrap requires a SqlResult source"));
    }
    let parameters = editor.node(ty.into())?.edges("typeArgs")?;
    let [wrapped] = parameters.as_slice() else {
        return Err(Error::new("SqlResult requires one type argument"));
    };
    let wrapped = editor.type_id(*wrapped)?;
    if !editor.same_type(wrapped, target)? || (safe && !editor.nullable(target)?) {
        return Err(Error::new("result unwrap has an incompatible target type"));
    }
    editor
        .make_expr(
            origin,
            Expr::Cast,
            target,
            [
                ("source", Field::Reference(source.into())),
                (
                    "safe",
                    Field::Data(
                        if safe {
                            "UnwrapResultSafe"
                        } else {
                            "UnwrapResultUnsafe"
                        }
                        .into(),
                    ),
                ),
            ],
        )
        .map(Into::into)
}

fn unwrap_option(
    editor: &mut CircuitEditor,
    origin: NodeId,
    source: ExpressionId,
    message: &str,
) -> Result<NodeId, Error> {
    let ty = editor.expression_type(source)?;
    if !editor.nullable(ty)? {
        return Err(Error::new("option unwrap requires a nullable source"));
    }
    let kind = editor.expr_kind(source)?;
    if kind == Expr::Some {
        return editor.node(source.into())?.edge("expression");
    }
    let tuple = matches!(kind, Expr::Tuple | Expr::RawTuple);
    if (tuple && editor.node(source.into())?.field("fields").is_none())
        || editor.is_null_literal(source)?
        || kind == Expr::Fail
    {
        let result = editor.with_nullability(ty, false)?;
        return editor
            .make_expr(
                origin,
                Expr::Fail,
                result,
                [("message", Field::Data(message.into()))],
            )
            .map(Into::into);
    }
    if kind.wire_name().ends_with("Literal") {
        let result = editor.with_nullability(ty, false)?;
        return editor.derive_inner(source.into(), [("type", Field::Reference(result.into()))]);
    }
    if tuple {
        let fields = editor
            .node(source.into())?
            .edges("fields")?
            .into_iter()
            .map(|id| editor.expression(id))
            .collect::<Result<Vec<_>, _>>()?;
        return editor
            .tuple_expr(source.into(), kind == Expr::RawTuple, &fields)
            .map(Into::into);
    }
    // Simplifying a surviving unwrap retains it and all references to its
    // result type. Cast expansion also calls this helper to create new unwraps.
    if matches!(
        editor.node(origin)?.inner_kind(),
        Some(InnerKind::Expression(Expr::Unwrap))
    ) && editor.node(origin)?.edge("expression")? == source.into()
    {
        return Ok(origin);
    }
    let result = editor.with_nullability(ty, false)?;
    let unwrap = editor.make_expr(
        origin,
        Expr::Unwrap,
        result,
        [
            ("expression", Field::Reference(source.into())),
            ("message", Field::Data(message.into())),
        ],
    )?;
    Ok(unwrap.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::Circuit;
    use serde_json::json;

    #[test]
    fn expanding_a_nullable_cast_preserves_the_enclosing_null_test() {
        let position = json!({
            "start_line_number": 1, "start_column": 8,
            "end_line_number": 1, "end_column": 23
        });
        let circuit = Circuit::from_json(json!({
            "class": "DBSPCircuit", "id": 0, "declarations": [], "allOperators": [{
                "class": "DBSPConstantOperator", "id": 1, "inputs": [], "function": {
                    "class": "DBSPIsNullExpression", "id": 2, "position": position,
                    "type": {"class":"DBSPTypeBool", "id":3, "code":"BOOL", "mayBeNull":false, "sourceEmpty":true},
                    "expression": {
                        "class": "DBSPCastExpression", "id": 4, "safe": "SqlUnsafe",
                        "type": {"class":"DBSPTypeBool", "id":5, "code":"BOOL", "mayBeNull":true},
                        "source": {"class":"DBSPVariablePath", "id":6, "variable":"v", "type":{"node":3}}
                    }
                }
            }]
        })).unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let original = NodeId::Inner(2);
        let expanded = expand_sql_casts(&mut edit, original, CastSafety::Unsafe, &mut 0).unwrap();
        let test = edit.node(expanded).unwrap();
        assert_eq!(test.class(), "DBSPIsNullExpression");
        assert_eq!(
            test.field("position"),
            edit.node(original).unwrap().field("position")
        );
        assert_ne!(
            test.edge("type").unwrap(),
            edit.node(original).unwrap().edge("type").unwrap()
        );
        assert_eq!(
            edit.node(test.edge("expression").unwrap()).unwrap().class(),
            "DBSPSomeExpression"
        );
    }
}
