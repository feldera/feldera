//! A rolling aggregate carries (order, Option<results>). Its following index
//! computes the empty-window value lazily. Preserve that COALESCE when pushing
//! a reader expression that can fail on the empty set.

use super::{
    build::{closure, link, links, rebind, typed, Parameters},
    reader::{kind, Access, Binding, Reads},
};
use crate::terms::Terms;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionKind as Expr, Field, NodeId};
use std::collections::BTreeMap;

#[derive(serde::Deserialize, serde::Serialize)]
pub(super) struct WindowIndex {
    pub operator: NodeId,
    function: NodeId,
    body: NodeId,
    key: NodeId,
    unwrap: NodeId,
    coalesce: NodeId,
    empty: NodeId,
    left: NodeId,
    read: Access,
    binding: Binding,
}

impl WindowIndex {
    pub fn recognize(
        edit: &CircuitEditor,
        index: NodeId,
        producer: NodeId,
    ) -> Result<Option<Self>, Error> {
        let Some(list) = edit
            .node(producer)?
            .field("aggregate")
            .and_then(Field::node_id)
        else {
            return Ok(None);
        };
        let function = edit.node(index)?.edge("function")?;
        if kind(edit, function) != Some(Expr::Closure) {
            return Ok(None);
        }
        let body = edit.node(function)?.edge("body")?;
        if kind(edit, body) != Some(Expr::RawTuple) {
            return Ok(None);
        }
        let parts = edit.node(body)?.edges("fields")?;
        let [key, unwrap] = parts.as_slice() else {
            return Ok(None);
        };
        if kind(edit, *unwrap) != Some(Expr::Unwrap) {
            return Ok(None);
        }
        let coalesce = edit.node(*unwrap)?.edge("expression")?;
        if kind(edit, coalesce) != Some(Expr::Binary)
            || edit.node(coalesce)?.data("opcode").and_then(|v| v.as_str()) != Some("COALESCE")
        {
            return Ok(None);
        }
        let empty = edit.node(coalesce)?.edge("right")?;
        if kind(edit, empty) != Some(Expr::Tuple) {
            return Ok(None);
        }
        let fields = edit.node(empty)?.edges("fields")?;
        let aggregates = edit.node(list)?.edges("aggregates")?;
        if fields.len() != aggregates.len() {
            return Ok(None);
        }
        let mut terms = Terms::default();
        for (a, b) in fields.into_iter().zip(aggregates) {
            let expected = edit.node(b)?.edge("emptySetResult")?;
            if terms.field(edit.circuit(), &link(a), &[])
                != terms.field(edit.circuit(), &link(expected), &[])
            {
                return Ok(None);
            }
        }
        let binding = Binding::new(edit, function, true, 0)?;
        let left = edit.node(coalesce)?.edge("left")?;
        let expression = if kind(edit, left) == Some(Expr::Clone) {
            edit.node(left)?.edge("expression")?
        } else {
            left
        };
        let Some(read) = binding.access(edit, expression) else {
            return Ok(None);
        };
        if read.part != 1 || read.field != 1 {
            return Ok(None);
        }
        let mut columns = Vec::new();
        if !super::build::columns(edit, *key, &mut columns)? {
            return Ok(None);
        }
        let mut uses = Reads::default();
        for expr in columns {
            uses.collect(edit, expr, &binding)?;
        }
        if uses.unsupported || uses.values.contains(&1) {
            return Ok(None);
        }
        Ok(Some(Self {
            operator: index,
            function,
            body,
            key: *key,
            unwrap: *unwrap,
            coalesce,
            empty,
            left,
            read,
            binding,
        }))
    }

    pub fn rewrite(
        &self,
        edit: &mut CircuitEditor,
        output: NodeId,
        value: NodeId,
        empties: &[NodeId],
        next: &mut u64,
    ) -> Result<(NodeId, NodeId), Error> {
        let old_row = edit.node(output)?.edge("elementType")?;
        let order = edit.node(old_row)?.edges("tupFields")?[0];
        let nullable = edit.derive_inner(value, [("mayBeNull", Field::Data(true.into()))])?;
        let row = edit.derive_inner(old_row, [("tupFields", links(&[order, nullable]))])?;
        let collection = edit.derive_inner(output, [("elementType", link(row))])?;
        let old_deref = edit.node(self.read.expression)?.edge("expression")?;
        let old_part = edit.node(old_deref)?.edge("expression")?;
        let old_var = edit.node(old_part)?.edge("expression")?;
        let mut params = Parameters::window(edit, &self.binding, collection, old_var, next)?;
        let key = rebind(
            edit,
            self.key,
            &self.binding,
            &mut params,
            &mut BTreeMap::new(),
        )?;
        let projection = params.project(edit, self.read, false)?;
        let left = if kind(edit, self.left) == Some(Expr::Clone) {
            edit.derive_inner(
                self.left,
                [("type", link(nullable)), ("expression", link(projection))],
            )?
        } else {
            projection
        };
        let right_ty = edit.derive_inner(value, [("mayBeNull", Field::Data(true.into()))])?;
        let right = edit.derive_inner(
            self.empty,
            [("type", link(right_ty)), ("fields", links(empties))],
        )?;
        let coalesce = edit.derive_inner(
            self.coalesce,
            [
                ("type", link(nullable)),
                ("left", link(left)),
                ("right", link(right)),
            ],
        )?;
        let value_type = edit.derive_inner(nullable, [("mayBeNull", Field::Data(false.into()))])?;
        let result = edit.derive_inner(
            self.unwrap,
            [("type", link(value_type)), ("expression", link(coalesce))],
        )?;
        let key_type = typed(edit, key)?;
        let body_type = edit.derive_inner(
            typed(edit, self.body)?,
            [("tupFields", links(&[key_type, value_type]))],
        )?;
        let body = edit.derive_inner(
            self.body,
            [("type", link(body_type)), ("fields", links(&[key, result]))],
        )?;
        let closure = closure(edit, self.function, body, &params.ids)?;
        let index_type = edit.derive_inner(
            edit.node(self.operator)?.edge("outputType")?,
            [
                ("keyType", link(key_type)),
                ("elementType", link(value_type)),
            ],
        )?;
        edit.set_operator_field(self.operator, "outputType", link(index_type))?;
        edit.set_operator_field(self.operator, "function", link(closure))?;
        Ok((collection, value_type))
    }
}
