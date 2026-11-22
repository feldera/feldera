//! Rebind a closure to parameters whose top-level unused columns have been removed.
use feldera_sql_expr::{parameter_bindings, Rewrite, Transformation, Visit};
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId, TypeId,
    TypeKind,
};
use std::collections::{HashMap, HashSet};

struct Parameter {
    name: String,
    ty: TypeId,
    offsets: Vec<usize>,
}

struct Reader {
    bindings: HashMap<NodeId, usize>,
    parameters: HashMap<usize, Parameter>,
}

impl Transformation for Reader {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if let Some(param) = self.bindings.get(&id).and_then(|i| self.parameters.get(i)) {
            return if edit.expression_type(edit.expression(id)?)? == param.ty {
                Ok(Visit::Keep)
            } else {
                Ok(Visit::Replace(
                    edit.named_variable(param.ty, &param.name)?.into(),
                ))
            };
        }
        match edit.node(id)?.inner_kind() {
            Some(InnerKind::Type(_) | InnerKind::Expression(Expr::Closure)) => Ok(Visit::Keep),
            _ => Ok(Visit::Children),
        }
    }

    fn rebuild_field(
        &mut self,
        edit: &mut CircuitEditor,
        original: NodeId,
        rebuilt: NodeId,
        _translating: bool,
    ) -> Result<NodeId, Error> {
        let field = edit.node(original)?;
        let mut index = field
            .data("fieldNo")
            .and_then(|v| v.as_u64())
            .and_then(|v| v.try_into().ok())
            .ok_or_else(|| Error::new("missing projected reader field number"))?;
        let mut receiver = field.edge("expression")?;
        // Dereferencing and unwrapping change the row type without entering a field.
        // Once the receiver is a field expression itself, its children keep their offsets.
        while matches!(
            edit.node(receiver)?.inner_kind(),
            Some(InnerKind::Expression(
                Expr::Deref | Expr::Unwrap | Expr::Clone
            ))
        ) {
            receiver = edit.node(receiver)?.edge("expression")?;
        }
        if let Some(param) = self
            .bindings
            .get(&receiver)
            .and_then(|i| self.parameters.get(i))
        {
            index = *param
                .offsets
                .get(index)
                .ok_or_else(|| Error::new("reader field exceeds the input arity"))?;
        }
        let value = edit.expression(edit.node(rebuilt)?.edge("expression")?)?;
        Ok(edit.tuple_field(value, index)?.into())
    }
}

/// Parameter declarations with equivalent types retain their identities. Variable
/// expressions use the replacement type object, including when it is equivalent.
pub fn rebind_projected_reader(
    edit: &mut CircuitEditor,
    closure: NodeId,
    replacements: &[NodeId],
    usage: &[Vec<bool>],
) -> Result<ExpressionId, Error> {
    if edit.node(closure)?.edges("parameters")?.len() != replacements.len()
        || replacements.len() != usage.len()
    {
        return Err(Error::new(
            "reader parameter count differs from its field projections",
        ));
    }
    let projected = replacements
        .iter()
        .zip(usage)
        .enumerate()
        .map(|(slot, (&new, used))| (slot, new, used.as_slice()));
    rebind_parameters(edit, closure, projected)
}

fn rebind_parameters<'a>(
    edit: &mut CircuitEditor,
    closure: NodeId,
    projected: impl IntoIterator<Item = (usize, NodeId, &'a [bool])>,
) -> Result<ExpressionId, Error> {
    let function = edit.node(closure)?;
    let original = function.edges("parameters")?;
    let body = function.edge("body")?;
    let bindings = parameter_bindings(edit, body, &original)?;
    let mut declarations = original.clone();
    let mut parameters = HashMap::new();
    for (slot, new, used) in projected {
        let old = *original
            .get(slot)
            .ok_or_else(|| Error::new("missing projected reader parameter"))?;
        let param = edit.node(new)?;
        let name = param
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing projected reader parameter name"))?
            .to_owned();
        let ty = edit.type_id(param.edge("type")?)?;
        let old_type = edit.type_id(edit.node(old)?.edge("type")?)?;
        if !edit.same_type(old_type, ty)? {
            declarations[slot] = new;
        }
        let mut row = old_type;
        let mut seen = HashSet::new();
        while edit.type_kind(row)? == TypeKind::Ref {
            if !seen.insert(NodeId::from(row)) {
                return Err(Error::new("cycle in projected reader parameter type"));
            }
            row = edit.type_id(edit.node(row.into())?.edge("type")?)?;
        }
        let raw = edit.type_kind(row)? == TypeKind::RawTuple;
        let mut next = 0;
        let offsets = used
            .iter()
            .map(|&keep| {
                let index = next;
                next += usize::from(keep || raw);
                index
            })
            .collect();
        parameters.insert(slot, Parameter { name, ty, offsets });
    }
    let result = Rewrite::new(Reader {
        bindings,
        parameters,
    })
    .apply(edit, body)?;
    if body == result && declarations == original {
        return edit.expression(closure);
    }
    let types = declarations
        .iter()
        .map(|&p| edit.node(p)?.edge("type"))
        .collect::<Result<Vec<_>, _>>()?;
    let signature = edit.closure_type(edit.expression(result)?, &types)?;
    let function = edit.derive_inner(
        closure,
        [
            (
                "parameters",
                Field::Array(declarations.into_iter().map(Field::Reference).collect()),
            ),
            ("type", Field::Reference(signature)),
            ("body", Field::Reference(result)),
        ],
    )?;
    edit.expression(function)
}

/// Narrow the input row in each aggregate closure that reads it. Accumulators,
/// weights and the final conversion retain their original declarations.
pub fn rebind_projected_aggregate(
    edit: &mut CircuitEditor,
    aggregate: NodeId,
    replacements: &[NodeId],
    used: &[bool],
) -> Result<ExpressionId, Error> {
    let readers = crate::aggregate_groups::row_parameters(edit.node(aggregate)?)?;
    if readers.len() != replacements.len() {
        return Err(Error::new(
            "aggregate row parameter count differs from its replacements",
        ));
    }
    let mut changes = Vec::new();
    for (&(field, slot), &replacement) in readers.iter().zip(replacements) {
        let closure = edit.node(aggregate)?.edge(field)?;
        let narrowed = rebind_parameters(edit, closure, [(slot, replacement, used)])?;
        if NodeId::from(narrowed) != closure {
            changes.push((field, Field::Reference(narrowed.into())));
        }
    }
    if changes.is_empty() {
        return edit.expression(aggregate);
    }
    let rewritten = edit.derive_inner(aggregate, changes)?;
    edit.expression(rewritten)
}
