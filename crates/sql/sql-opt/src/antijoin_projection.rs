//! Build the input projections pushed through an antijoin. Both inputs retain the key.
use crate::row_projection::tuple_of_fields;
use crate::{identity_closure, rebuild::deindex_function, stream_rewrites::append_map};
use feldera_sql_expr::{
    apply_closure, parameter_paths, Rewrite, SimplifyWarning, Transformation, Visit,
};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, OutputPort, TypeId, TypeKind,
};

use std::collections::HashSet;

/// A projection above an antijoin can reduce the left value without changing
/// the keys used to test membership. Both source ports can belong to one node.
pub struct AntijoinReader {
    pub join: Op,
    pub inputs: [OutputPort; 2],
    pub streams: [TypeId; 2],
    pub function: NodeId,
    pub indexed: bool,
    pub width: usize,
}

pub enum AntijoinRewrite {
    CannotSplitKey,
    KeepReader,
    ReplaceReader,
}

impl AntijoinReader {
    fn project_input(
        &self,
        edit: &mut CircuitEditor,
        site: NodeId,
        input: OutputPort,
        stream: TypeId,
        function: ExpressionId,
        next: &mut u64,
    ) -> Result<(OutputPort, TypeId), Error> {
        if identity_closure(edit, function.into(), next)? {
            return Ok((input, stream));
        }
        let (id, result) = append_map(
            edit,
            edit.circuit().root_id(),
            site,
            Op::MapIndex,
            input,
            function,
        )?;
        edit.annotate(
            id,
            serde_json::json!({"class":"IsProjection", "outputSize":self.width}),
        )?;
        Ok((
            OutputPort {
                operator: id,
                number: 0,
            },
            result,
        ))
    }

    pub fn append(
        &self,
        edit: &mut CircuitEditor,
        site: NodeId,
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<AntijoinRewrite, Error> {
        if !matches!(self.join, Op::AntiJoin | Op::StreamAntiJoin) {
            return Err(Error::new("expected an antijoin"));
        }
        let [left, right] = self.streams;
        let (first, after) = if self.indexed {
            let Some([first, after]) = split_antijoin_projection(edit, self.function)? else {
                return Ok(AntijoinRewrite::CannotSplitKey);
            };
            (first, Some(after))
        } else {
            (
                antijoin_projection(edit, left.into(), Some(self.function), next, warnings)?,
                None,
            )
        };
        let body = edit.circuit().root_id();
        let (left_port, output) =
            self.project_input(edit, site, self.inputs[0], left, first, next)?;
        let keys = antijoin_projection(edit, right.into(), None, next, warnings)?;
        let (right_port, _) = self.project_input(edit, site, self.inputs[1], right, keys, next)?;

        // The reference visitor adds both input maps before checking the new
        // left type. They are visible until the next dead-code pass.
        if edit.same_type(left, output)? {
            return Ok(AntijoinRewrite::KeepReader);
        }
        let join = edit.append_operator(
            body,
            site,
            self.join,
            &[left_port, right_port],
            [("outputType", Field::Reference(output.into()))],
        )?;
        let joined = OutputPort {
            operator: join,
            number: 0,
        };
        if let Some(after) = after {
            if !identity_closure(edit, after.into(), next)? {
                append_map(edit, body, site, Op::MapIndex, joined, after)?;
            }
        } else {
            let index = edit.node(output.into())?;
            let value = edit.type_id(index.edge("elementType")?)?;
            let key = edit.type_id(index.edge("keyType")?)?;
            let function = deindex_function(edit, site, key, value, next)?;
            let stream = edit.zset_type(value)?;
            edit.append_operator(
                body,
                site,
                Op::Deindex,
                &[joined],
                [
                    ("outputType", Field::Reference(stream.into())),
                    ("function", Field::Reference(function.into())),
                ],
            )?;
        }
        Ok(AntijoinRewrite::ReplaceReader)
    }
}

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
        Ok(match node.inner_kind() {
            Some(InnerKind::Type(_)) => Visit::Keep,
            _ => Visit::Children,
        })
    }
}

/// The original visitor checks the written parameter name before rebinding the key.
/// This also detects a read of a shadowing variable with the same name.
fn key_reads_value(edit: &CircuitEditor, key: NodeId, parameter: &str) -> Result<bool, Error> {
    let mut pending = vec![key];
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let node = edit.node(id)?;
        if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Field)))
            && node.data("fieldNo").and_then(|v| v.as_u64()) == Some(1)
        {
            let base = edit.node(node.edge("expression")?)?;
            if matches!(
                base.inner_kind(),
                Some(InnerKind::Expression(Expr::VariablePath))
            ) && base.data("variable").and_then(|v| v.as_str()) == Some(parameter)
            {
                return Ok(true);
            }
        }
        feldera_sql_expr::lexical_scope::push_expression_children(node, &mut pending);
    }
    Ok(false)
}

/// Split (key, value) -> (new_key, new_value) at the antijoin. The input
/// projection preserves the old key. The output projection can compute its
/// new key solely from the old key, after the join has discarded the old value.
/// Return `None` when the key depends on the input value.
pub fn split_antijoin_projection(
    edit: &mut CircuitEditor,
    closure: NodeId,
) -> Result<Option<[ExpressionId; 2]>, Error> {
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
    if key_reads_value(edit, key, &name)? {
        return Ok(None);
    }
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
    Ok(Some([first, second]))
}
