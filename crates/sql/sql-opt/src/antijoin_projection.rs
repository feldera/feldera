//! Build the input projections pushed through an antijoin. Both inputs retain the key.
use feldera_sql_expr::{
    apply_closure, parameter_paths, Rewrite, SimplifyWarning, Transformation, Visit,
};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, InnerKind, NodeId, TypeKind,
};

use std::collections::HashSet;

/// A value projection runs on the left input. The right input needs only a
/// key and an empty value. The left map expands nested key fields at the
/// projection's source position.
pub fn antijoin_projection(
    edit: &mut CircuitEditor,
    stream: NodeId,
    projection: Option<NodeId>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let indexed = edit.node(stream)?;
    let key = edit.type_id(indexed.edge("keyType")?)?;
    let value = edit.type_id(indexed.edge("elementType")?)?;
    let key_ref = edit.reference_type(key)?;
    let value_ref = edit.reference_type(value)?;
    let input = edit.tuple_type(stream, true, [key_ref.into(), value_ref.into()])?;
    let pair = edit.variable(input, next)?;
    let key_field = edit.tuple_field(pair, 0)?;
    let key_value = edit.dereference(key_field)?;
    let fields = if let Some(projection) = projection {
        let retained = edit.expand_tuple(projection, key_value)?;
        let projected = apply_closure(edit, projection, &[pair], warnings)?;
        [retained, projected]
    } else {
        let retained = tuple_of_fields(edit, key_value)?;
        let empty = edit.tuple_expr(stream, false, &[])?;
        [retained, empty]
    };
    let indexed_row = edit.tuple_expr(stream, true, &fields)?;
    edit.closure_from_variables(indexed_row, &[pair])
}

/// Flatten one level and clone non-Copy fields of the borrowed row.
fn tuple_of_fields(edit: &mut CircuitEditor, row: ExpressionId) -> Result<ExpressionId, Error> {
    let ty = edit.expression_type(row)?;
    let fields = if matches!(edit.type_kind(ty)?, TypeKind::Tuple | TypeKind::RawTuple) {
        let count = edit.node(ty.into())?.edges("tupFields")?.len();
        (0..count)
            .map(|i| {
                let field = edit.tuple_field(row, i)?;
                edit.clone_if_needed(field)
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        vec![row]
    };
    edit.tuple_expr(row.into(), false, &fields)
}

struct RebindKey {
    uses: HashSet<NodeId>,
    pair: ExpressionId,
}

impl Transformation for RebindKey {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if self.uses.contains(&id) {
            return Ok(Visit::Replace(self.pair.into()));
        }
        let node = edit.node(id)?;
        match node.inner_kind() {
            Some(InnerKind::Type(_)) => Ok(Visit::Keep),
            Some(InnerKind::Expression(Expr::Field))
                if node.data("fieldNo").and_then(|v| v.as_u64()) == Some(1)
                    && self.uses.contains(&node.edge("expression")?) =>
            {
                Err(Error::new("antijoin key projection reads the input value"))
            }
            _ => Ok(Visit::Children),
        }
    }
}

/// Split (key, value) -> (new_key, new_value) at the antijoin. The input
/// projection preserves the old key. The output projection can compute its
/// new key solely from the old key, after the join has discarded the old value.
pub fn split_antijoin_projection(
    edit: &mut CircuitEditor,
    closure: NodeId,
) -> Result<[ExpressionId; 2], Error> {
    let node = edit.node(closure)?;
    let [param] = node
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("antijoin projection requires one parameter"))?;
    let [key, value] = edit
        .node(node.edge("body")?)?
        .edges("fields")?
        .try_into()
        .map_err(|_| Error::new("antijoin projection must produce a key and value"))?;
    let declaration = edit.node(param)?;
    let name = declaration
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("antijoin parameter has no name"))?
        .to_owned();
    let input = edit.type_id(declaration.edge("type")?)?;
    let [key_ref, _] = edit
        .node(input.into())?
        .edges("tupFields")?
        .try_into()
        .map_err(|_| Error::new("antijoin projection requires a key/value input"))?;
    let pair = edit.named_variable(input, &name)?;
    let borrowed = edit.tuple_field(pair, 0)?;
    let old_key = edit.dereference(borrowed)?;
    let retained = if matches!(
        edit.type_kind(edit.expression_type(old_key)?)?,
        TypeKind::Tuple | TypeKind::RawTuple
    ) {
        tuple_of_fields(edit, old_key)?
    } else {
        edit.clone_if_needed(old_key)?
    };
    let value = edit.expression(value)?;
    let projected = edit.clone_if_needed(value)?;
    let first = edit.tuple_expr(closure, true, &[retained, projected])?;
    let first = edit.closure_with_parameters(first, &[param])?;

    let value_ref = edit.reference_type(edit.expression_type(value)?)?;
    let new_input = edit.tuple_type(closure, true, [key_ref, value_ref.into()])?;
    let new_pair = edit.named_variable(new_input, &name)?;
    let uses = parameter_paths(edit, key, param)?;
    let key = Rewrite::new(RebindKey {
        uses,
        pair: new_pair,
    })
    .apply(edit, key)?;
    let key = edit.clone_if_needed(edit.expression(key)?)?;
    let value = edit.tuple_field(new_pair, 1)?;
    let value = edit.dereference(value)?;
    let retained = tuple_of_fields(edit, value)?;
    let second = edit.tuple_expr(closure, true, &[key, retained])?;
    let second = edit.closure_from_variables(second, &[new_pair])?;
    Ok([first, second])
}
