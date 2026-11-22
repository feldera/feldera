//! Rebind a closure to parameters whose top-level unused columns have been removed.
use feldera_sql_expr::{Rewrite, Transformation, Visit, parameter_bindings};
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
    parameters: Vec<Parameter>,
}

impl Transformation for Reader {
    fn preorder(&mut self, edit: &mut CircuitEditor, id: NodeId) -> Result<Visit, Error> {
        if let Some(&index) = self.bindings.get(&id) {
            let param = &self.parameters[index];
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
        if let Some(&i) = self.bindings.get(&receiver) {
            index = *self.parameters[i]
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
    let function = edit.node(closure)?;
    let original = function.edges("parameters")?;
    let body = function.edge("body")?;
    if original.len() != replacements.len() || original.len() != usage.len() {
        return Err(Error::new(
            "reader parameter count differs from its field projections",
        ));
    }
    let bindings = parameter_bindings(edit, body, &original)?;
    let mut declarations = Vec::with_capacity(original.len());
    let mut parameters = Vec::with_capacity(original.len());
    for ((&old, &new), used) in original.iter().zip(replacements).zip(usage) {
        let param = edit.node(new)?;
        let name = param
            .data("name")
            .and_then(|v| v.as_str())
            .ok_or_else(|| Error::new("missing projected reader parameter name"))?
            .to_owned();
        let ty = edit.type_id(param.edge("type")?)?;
        let old_type = edit.type_id(edit.node(old)?.edge("type")?)?;
        declarations.push(if edit.same_type(old_type, ty)? {
            old
        } else {
            new
        });
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
        parameters.push(Parameter { name, ty, offsets });
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
