//! Shared transformations of circuit expressions and types.

pub mod constant_rows;
mod canonical;
pub use canonical::canonicalize_names;
mod tree;
pub use tree::{copy_expression, ensure_expression_tree, first_repeated_expression, flatten_tuple_fields, is_expression_tree};
mod beta;
pub use beta::{
    apply_closure, compose_closures, parameter_bindings, parameter_paths, parameter_references, reduce_closure_body, reduce_closure_body_with_warnings,
    simplify_closure_expression, simplify_closure_with_warnings,
};
mod casts;
mod error_sites;
mod field_use;
mod flatmap;
pub use field_use::{input_field_paths, live_parameter_field_paths, parameter_field_paths};
pub use error_sites::{error_source_reports_position, ErrorSites};
pub use flatmap::{flatmap_generated_types, project_flatmap, rebuild_flatmap, replace_flatmap_input, FlatmapColumns};
mod sql_index;
pub use casts::{
    decimal_literal_parts, expand_sql_casts, prepare_column_default, literal_is_positive, numeric_value_is_positive, same_literal_value, CastSafety, SimplifyWarning,
};
pub use sql_index::{adjust_index_root, adjust_sql_index};

use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind, Field, InnerKind, NodeId, TypeKind};
mod rewrite;
pub use rewrite::{Rewrite, Transformation, Visit};

/// The aggregate-list constructor caches a fresh tuple of the rewritten empty-set values.
fn rebuild_aggregate_cache(arena: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
    let list = arena.node(id)?;
    let Some(cached) = list.field("cachedEmptySetResult").and_then(Field::node_id) else {
        return Ok(id);
    };
    let values = list
        .edges("aggregates")?
        .into_iter()
        .map(|part| arena.node(part)?.edge("emptySetResult"))
        .collect::<Result<Vec<_>, _>>()?;
    let types = values
        .iter()
        .map(|&value| arena.node(value)?.edge("type"))
        .collect::<Result<Vec<_>, _>>()?;
    let ty = arena.tuple_type(id, false, types)?;
    let tuple = arena.derive_inner(
        cached,
        [
            (
                "fields",
                Field::Array(values.into_iter().map(Field::Reference).collect()),
            ),
            ("type", Field::Reference(ty.into())),
        ],
    )?;
    arena.derive_inner(id, [("cachedEmptySetResult", Field::Reference(tuple))])
}

/// These Java constructors derive their result type from their rewritten children.
fn rebuild_result_type(
    arena: &mut CircuitEditor,
    node: NodeId,
    expr: Option<ExpressionKind>,
    expression_translation: bool,
) -> Result<NodeId, Error> {
    let value = arena.node(node)?;
    let ty = match expr {
        Some(ExpressionKind::RawTuple) if expression_translation => return Ok(node),
        Some(ExpressionKind::Deref) => {
            let source = arena.expression(value.edge("expression")?)?;
            return arena.dereference(source).map(Into::into);
        }
        Some(ExpressionKind::Clone) if expression_translation => {
            let child = arena.expression(value.edge("expression")?)?;
            return arena.clone_value(child).map(Into::into);
        }
        Some(ExpressionKind::Clone) => arena.node(value.edge("expression")?)?.edge("type")?,
        Some(ExpressionKind::If) => arena.node(value.edge("positive")?)?.edge("type")?,
        Some(ExpressionKind::Let) => arena.node(value.edge("consumer")?)?.edge("type")?,
        Some(ExpressionKind::Block) => match value.field("lastExpression").and_then(Field::node_id)
        {
            Some(last) => arena.node(last)?.edge("type")?,
            None => return Ok(node),
        },
        Some(ExpressionKind::Field) => {
            let source = arena.expression(value.edge("expression")?)?;
            let field = value
                .data("fieldNo")
                .and_then(|n| n.as_u64())
                .and_then(|n| usize::try_from(n).ok())
                .ok_or_else(|| Error::new("invalid tuple field"))?;
            if !expression_translation {
                return arena.tuple_field(source, field).map(Into::into);
            }
            arena
                .tuple_field_type(arena.expression_type(source)?, field)?
                .into()
        }
        Some(ExpressionKind::IsNull) => {
            let operand = arena.expression(value.edge("expression")?)?;
            return arena.null_test(operand).map(Into::into);
        }
        Some(ExpressionKind::Lazy) => {
            let result = arena.node(value.edge("expression")?)?.edge("type")?;
            let previous_type = value.edge("type")?;
            let arguments = arena.node(previous_type)?.edges("typeArgs")?;
            let [_, any] = arguments.as_slice() else {
                return Err(Error::new("lazy expression requires two type arguments"));
            };
            arena
                .lazy_type(arena.type_id(result)?, arena.type_id(*any)?)?
                .into()
        }
        Some(ExpressionKind::HandleError) => {
            let source = arena.node(value.edge("source")?)?.edge("type")?;
            if arena.type_kind(arena.type_id(source)?)? == TypeKind::SqlResult {
                let args = arena.node(source)?.edges("typeArgs")?;
                let [wrapped] = args.as_slice() else {
                    return Err(Error::new("SqlResult requires one type argument"));
                };
                *wrapped
            } else {
                source
            }
        }
        Some(ExpressionKind::UnsignedWrap) => {
            // Its constructor computes a fresh unsigned integer result type.
            arena.derive_inner(value.edge("type")?, [])?
        }
        Some(ExpressionKind::Some) => {
            let child = arena.expression(value.edge("expression")?)?;
            return arena.some_at(node, child).map(Into::into);
        }
        Some(ExpressionKind::Unwrap) => {
            let child = arena.expression(value.edge("expression")?)?;
            arena
                .with_nullability(arena.expression_type(child)?, false)?
                .into()
        }
        Some(ExpressionKind::Borrow) => {
            let child = arena.expression(value.edge("expression")?)?;
            let mutable = value.data("mut").cloned();
            let ty = arena.reference_type(arena.expression_type(child)?)?;
            if expression_translation {
                let mutable = mutable.ok_or_else(|| Error::new("borrow requires mutability"))?;
                return arena
                    .make_expr(
                        child.into(),
                        ExpressionKind::Borrow,
                        ty,
                        [
                            ("expression", Field::Reference(child.into())),
                            ("mut", Field::Data(mutable)),
                        ],
                    )
                    .map(Into::into);
            }
            ty.into()
        }
        Some(ExpressionKind::RawTuple) => {
            let types = value
                .edges("fields")?
                .into_iter()
                .map(|field| arena.node(field)?.edge("type"))
                .collect::<Result<Vec<_>, Error>>()?;
            arena.tuple_type(node, true, types)?.into()
        }
        Some(ExpressionKind::Closure) => {
            let body = arena.expression(value.edge("body")?)?;
            let types = value
                .edges("parameters")?
                .into_iter()
                .map(|p| arena.node(p)?.edge("type"))
                .collect::<Result<Vec<_>, Error>>()?;
            arena.closure_type(body, &types)?
        }
        Some(ExpressionKind::CustomOrd) => {
            let source = arena.node(value.edge("source")?)?.edge("type")?;
            let comparator = arena.node(value.edge("comparator")?)?.edge("type")?;
            arena.custom_order_type(node, arena.type_id(source)?, arena.type_id(comparator)?)?.into()
        }
        _ => return Ok(node),
    };
    arena.derive_inner(node, [("type", Field::Reference(ty))])
}

#[derive(Clone, Copy)]
pub enum Rule {
    EliminateStructs,
    RemoveClones,
    RemoveTypedBox,
}

impl Transformation for Rule {
    fn preorder(&mut self, editor: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        let node = editor.node(id)?;
        Ok(match (*self, node.inner_kind()) {
            (Rule::RemoveClones, Some(InnerKind::Type(_))) => Visit::Keep,
            (Rule::RemoveClones, Some(InnerKind::Expression(ExpressionKind::Clone))) => {
                Visit::Follow(node.edge("expression")?)
            }
            (Rule::RemoveTypedBox, Some(InnerKind::Expression(ExpressionKind::Unary)))
                if node.data("opcode").and_then(|v| v.as_str()) == Some("TYPEDBOX") =>
            {
                Visit::Follow(node.edge("source")?)
            }
            (Rule::RemoveTypedBox, Some(InnerKind::Type(TypeKind::User | TypeKind::TypedBox)))
                if node.data("code").and_then(|v| v.as_str()) == Some("TYPEDBOX") =>
            {
                Visit::Follow(
                    *node
                        .edges("typeArgs")?
                        .first()
                        .ok_or_else(|| Error::new("TypedBox has no element type"))?,
                )
            }
            _ => Visit::Children,
        })
    }

    fn postorder(&mut self, arena: &mut CircuitEditor, id: NodeId) -> Result<NodeId, Error> {
        let node = arena.node(id)?;
        if !matches!(
            (*self, node.inner_kind()),
            (
                Rule::EliminateStructs,
                Some(InnerKind::Type(TypeKind::Struct))
            )
        ) {
            return Ok(id);
        }
        let nullable = arena.nullable(arena.type_id(id)?)?;
        let fields = node
            .edges("fields")?
            .into_iter()
            .map(|field| Ok(Field::Reference(arena.node(field)?.edge("type")?)))
            .collect::<Result<_, Error>>()?;
        arena.inner_at(
            id,
            InnerKind::Type(TypeKind::Tuple),
            [
                ("tupFields", Field::Array(fields)),
                ("code", Field::Data("TUPLE".into())),
                ("mayBeNull", Field::Data(nullable.into())),
            ],
        )
    }
}

/// Search structural children once per identity, excluding replay-only links.
pub fn contains_type(
    circuit: &feldera_sql_ir::Circuit,
    root: NodeId,
    kind: TypeKind,
) -> Result<bool, Error> {
    let mut seen = std::collections::HashSet::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing inner node"))?;
        if matches!(node.inner_kind(), Some(InnerKind::Type(found)) if *found == kind) {
            return Ok(true);
        }
        let mut fields: Vec<_> = node
            .fields()
            .filter(|(name, _)| *name != "originalStruct")
            .map(|(_, f)| f)
            .collect();
        while let Some(field) = fields.pop() {
            match field {
                Field::Reference(child) | Field::Definition(child) => pending.push(*child),
                Field::Array(items) => fields.extend(items),
                Field::Object(items) => fields.extend(items.values()),
                Field::Data(_) => (),
            }
        }
    }
    Ok(false)
}
