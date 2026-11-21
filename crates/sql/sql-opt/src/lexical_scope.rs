//! Lexical declarations and captures in a serialized closure.
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId};
use std::collections::{HashMap, HashSet};

pub(crate) type References = HashMap<NodeId, NodeId>;

fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| Error::new(format!("{} needs a {field} name", node.class())))
}

fn record(references: &mut References, variable: NodeId, declaration: NodeId) -> Result<(), Error> {
    if references
        .insert(variable, declaration)
        .is_some_and(|prior| prior != declaration)
    {
        return Err(Error::new(format!(
            "shared variable {variable:?} refers to two declarations"
        )));
    }
    Ok(())
}

enum Visit<'a> {
    Enter(NodeId),
    Leave(NodeId),
    Bind(&'a str, NodeId),
    Restore(usize),
}

pub(crate) fn resolve(circuit: &Circuit, closure: NodeId) -> Result<References, Error> {
    let mut references = HashMap::new();
    let mut bindings = Vec::new();
    let mut path = HashSet::new();
    let mut pending = vec![Visit::Enter(closure)];
    while let Some(visit) = pending.pop() {
        let id = match visit {
            Visit::Enter(id) => id,
            Visit::Leave(id) => {
                path.remove(&id);
                continue;
            }
            Visit::Bind(name, id) => {
                bindings.push((name, id));
                continue;
            }
            Visit::Restore(size) => {
                bindings.truncate(size);
                continue;
            }
        };
        if !path.insert(id) {
            return Err(Error::new("cycle in a closure"));
        }
        pending.push(Visit::Leave(id));
        let node = circuit.node(id).expect("validated inner edge");
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::VariablePath)) => {
                let variable = name(node, "variable")?;
                let declaration = bindings
                    .iter()
                    .rev()
                    .find_map(|&(bound, id)| (bound == variable).then_some(id))
                    .ok_or_else(|| Error::new(format!("unbound variable {variable} in closure")))?;
                record(&mut references, id, declaration)?;
            }
            Some(InnerKind::Expression(Expr::Closure)) => {
                pending.push(Visit::Restore(bindings.len()));
                for parameter in node.edges("parameters")? {
                    let param = circuit.node(parameter).expect("validated parameter");
                    bindings.push((name(param, "name")?, parameter));
                }
                pending.push(Visit::Enter(node.edge("body")?));
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                let variable = node.edge("variable")?;
                let path = circuit.node(variable).expect("validated let variable");
                record(&mut references, variable, id)?;
                // Its initializer sees the outer binding of the same name.
                pending.push(Visit::Restore(bindings.len()));
                pending.push(Visit::Enter(node.edge("consumer")?));
                pending.push(Visit::Bind(name(path, "variable")?, id));
                pending.push(Visit::Enter(node.edge("initializer")?));
            }
            _ => {
                if matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Block))) {
                    pending.push(Visit::Restore(bindings.len()));
                }
                if node.class() == "DBSPLetStatement" {
                    pending.push(Visit::Bind(name(node, "variable")?, id));
                }
                pending.extend(
                    expression_children(node)
                        .into_iter()
                        .rev()
                        .map(Visit::Enter),
                );
            }
        }
    }
    Ok(references)
}

pub(crate) fn expression_children(node: &Node) -> Vec<NodeId> {
    let mut children = Vec::new();
    if !matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
        for (name, field) in node.fields() {
            if !matches!(name, "type" | "originalStruct") {
                field.collect_inner_edges(&mut children);
            }
        }
    }
    children
}

/// A movable expression may refer to its own declarations and an optional outer parameter.
pub(crate) fn closed_over(
    circuit: &Circuit,
    root: NodeId,
    parameter: Option<NodeId>,
    references: &References,
) -> Result<bool, Error> {
    let mut inside = HashSet::new();
    let mut required = Vec::new();
    let mut pending = vec![root];
    while let Some(id) = pending.pop() {
        if !inside.insert(id) {
            continue;
        }
        let node = circuit.node(id).expect("validated inner edge");
        if matches!(
            node.inner_kind(),
            Some(InnerKind::Expression(Expr::VariablePath))
        ) {
            let declaration = references
                .get(&id)
                .ok_or_else(|| Error::new("unresolved variable"))?;
            required.push(*declaration);
        }
        pending.extend(expression_children(node));
    }
    Ok(required
        .into_iter()
        .all(|id| Some(id) == parameter || inside.contains(&id)))
}
