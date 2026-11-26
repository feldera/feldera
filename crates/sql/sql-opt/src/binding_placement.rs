//! Choose where pending lazy bindings become visible during the bottom-up CSE rewrite.
use feldera_sql_ir::{
    CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, NodeId, TypeKind,
};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct PendingBinding {
    pub(crate) owner: u64,
    pub(crate) dependencies: Vec<u64>,
}

#[derive(Deserialize)]
pub struct BindingScope {
    /// Block lets in source order, a single let expression, or closure parameters.
    pub(crate) declarations: Vec<u64>,
    pub(crate) closure: Option<u64>,
    pub(crate) outermost: bool,
}

impl BindingScope {
    /// A block inserts a binding after its last local dependency. A closure
    /// inserts all captured bindings at its entrance, in creation order.
    /// An unplaced binding travels out to the next enclosing scope.
    pub fn placements(&self, bindings: &[PendingBinding]) -> Vec<Option<usize>> {
        self.positions(bindings.iter())
    }

    pub(crate) fn positions<'a>(
        &self,
        bindings: impl Iterator<Item = &'a PendingBinding>,
    ) -> Vec<Option<usize>> {
        let positions: HashMap<_, _> = self.declarations.iter().copied().zip(0..).collect();
        bindings
            .map(|binding| {
                let mut local = binding
                    .dependencies
                    .iter()
                    .filter_map(|id| positions.get(id));
                match self.closure {
                    Some(owner) => {
                        (self.outermost || binding.owner == owner || local.next().is_some())
                            .then_some(0)
                    }
                    None => local.max().copied(),
                }
            })
            .collect()
    }
}

/// A result type starts a new lazy cell. Subsequent reads pass the variable
/// itself so the initializer and every use share one binding.
pub fn read_lazy_value(
    edit: &mut CircuitEditor,
    source: NodeId,
    borrowed: bool,
    next_name: &mut u64,
) -> Result<[ExpressionId; 2], Error> {
    let variable = match edit.node(source)?.inner_kind() {
        Some(InnerKind::Type(_)) => {
            let result = edit.type_id(source)?;
            let inferred = edit.standard_type(source, TypeKind::Any)?;
            let cell = edit.lazy_type(result, inferred)?;
            edit.variable(cell, next_name)?
        }
        Some(InnerKind::Expression(ExpressionKind::VariablePath)) => {
            let variable = edit.expression(source)?;
            if edit.type_kind(edit.expression_type(variable)?)? != TypeKind::Lazy {
                return Err(Error::new("CSE read requires a lazy variable"));
            }
            variable
        }
        _ => {
            return Err(Error::new(
                "CSE read requires a result type or a lazy variable",
            ))
        }
    };
    let mut value = edit.dereference(variable)?;
    if !borrowed {
        value = edit.clone_if_needed(value)?;
    }
    Ok([variable, value])
}

/// A previously allocated CSE variable and the expression computed on its first read.
struct LazyBinding {
    variable: ExpressionId,
    initializer: ExpressionId,
}

impl LazyBinding {
    fn new(edit: &mut CircuitEditor, [variable, value]: [NodeId; 2]) -> Result<Self, Error> {
        let variable = edit.expression(variable)?;
        let cell = edit.expression_type(variable)?;
        if edit.expr_kind(variable)? != ExpressionKind::VariablePath
            || edit.type_kind(cell)? != TypeKind::Lazy
        {
            return Err(Error::new("CSE requires a lazy variable"));
        }
        let [expected, closure] = edit
            .node(cell.into())?
            .edges("typeArgs")?
            .try_into()
            .map_err(|_| Error::new("lazy variable requires two type arguments"))?;
        let value = edit.expression(value)?;
        let result = edit.expression_type(value)?;
        if !edit.same_type(edit.type_id(expected)?, result)? {
            return Err(Error::new(
                "lazy variable and initializer have different value types",
            ));
        }
        let ty = edit.lazy_type(result, edit.type_id(closure)?)?;
        let initializer = edit.make_expr(
            value.into(),
            ExpressionKind::Lazy,
            ty,
            [("expression", Field::Reference(value.into()))],
        )?;
        Ok(Self {
            variable,
            initializer,
        })
    }
}

/// Nest earlier bindings around later ones, whose initializers may read them.
pub fn bind_lazy_expression(
    edit: &mut CircuitEditor,
    pairs: &[[NodeId; 2]],
    mut body: ExpressionId,
) -> Result<ExpressionId, Error> {
    for &pair in pairs.iter().rev() {
        let binding = LazyBinding::new(edit, pair)?;
        body = edit.let_expression(binding.variable, binding.initializer, body)?;
    }
    Ok(body)
}

/// Statements in one block follow their last local dependency in creation order.
pub fn lazy_statements(
    edit: &mut CircuitEditor,
    pairs: &[[NodeId; 2]],
) -> Result<Vec<NodeId>, Error> {
    pairs
        .iter()
        .map(|&pair| {
            let binding = LazyBinding::new(edit, pair)?;
            let name = edit
                .node(binding.variable.into())?
                .data("variable")
                .and_then(|value| value.as_str())
                .ok_or_else(|| Error::new("CSE variable requires a name"))?
                .to_owned();
            edit.let_statement(&name, binding.initializer)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lazy_reads_share_a_cell_and_clone_only_owned_noncopy_values() -> Result<(), Error> {
        let circuit = feldera_sql_ir::Circuit::from_json(serde_json::json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let text = edit.standard_type(site, TypeKind::String)?;
        let mut next = 7;
        let [cell, borrowed] = read_lazy_value(&mut edit, text.into(), true, &mut next)?;
        let [reused, owned] = read_lazy_value(&mut edit, cell.into(), false, &mut next)?;
        assert_eq!(next, 8);
        assert_eq!(cell, reused);
        assert_eq!(edit.expr_kind(borrowed)?, ExpressionKind::Deref);
        assert_eq!(edit.node(borrowed.into())?.edge("expression")?, cell.into());
        assert_eq!(edit.expr_kind(owned)?, ExpressionKind::Clone);
        let deref = edit.node(owned.into())?.edge("expression")?;
        assert_eq!(edit.node(deref)?.edge("expression")?, cell.into());
        let boolean = edit.bool_type(site, false)?;
        let [_, copied] = read_lazy_value(&mut edit, boolean.into(), false, &mut next)?;
        assert_eq!(edit.expr_kind(copied)?, ExpressionKind::Deref);
        assert_eq!(next, 9);
        Ok(())
    }

    #[test]
    fn inner_scopes_consume_only_the_bindings_they_own() {
        let bindings = [
            PendingBinding {
                owner: 8,
                dependencies: vec![52, 4],
            },
            PendingBinding {
                owner: 9,
                dependencies: vec![],
            },
            PendingBinding {
                owner: 8,
                dependencies: vec![4],
            },
        ];
        let mut scope = BindingScope {
            declarations: vec![4, 52],
            closure: None,
            outermost: false,
        };
        assert_eq!(scope.placements(&bindings), [Some(1), None, Some(0)]);
        scope.closure = Some(9);
        scope.declarations = vec![52];
        assert_eq!(scope.placements(&bindings), [Some(0), Some(0), None]);
        scope.outermost = true;
        assert_eq!(scope.placements(&bindings), [Some(0); 3]);
    }
}
