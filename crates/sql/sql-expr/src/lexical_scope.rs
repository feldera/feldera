//! Lexical declarations and captures in a serialized closure.
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId};
use std::collections::{HashMap, HashSet};

pub type References = HashMap<NodeId, NodeId>;

fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| Error::new(format!("{} needs a {field} name", node.class())))
}

enum Visit<'a> {
    Enter(NodeId),
    Leave(NodeId, usize),
    Bind(&'a str, NodeId),
    Record(NodeId, NodeId),
    Restore(usize, usize),
}

pub enum Resolution<'a> {
    Bound(HashMap<NodeId, NodeId>),
    Free {
        variable: NodeId,
        name: &'a str,
    },
    Conflict {
        variable: NodeId,
        previous: NodeId,
        current: NodeId,
    },
}

/// All bindings recorded before the first diagnostic. Open expressions can
/// contain free variables while still resolving their local declarations.
#[derive(Default)]
pub struct ReferenceInspection<'a> {
    pub bindings: HashMap<NodeId, NodeId>,
    pub has_free_variables: bool,
    pub diagnostic: Option<BindingDiagnostic<'a>>,
}

#[derive(Debug)]
pub enum BindingDiagnostic<'a> {
    Free { variable: NodeId, name: &'a str },
    Conflict { variable: NodeId, previous: NodeId, current: NodeId },
}

impl<'a> ReferenceInspection<'a> {
    fn resolution(self) -> Resolution<'a> {
        match self.diagnostic {
            Some(BindingDiagnostic::Free { variable, name }) => Resolution::Free { variable, name },
            Some(BindingDiagnostic::Conflict { variable, previous, current }) =>
                Resolution::Conflict { variable, previous, current },
            None => Resolution::Bound(self.bindings),
        }
    }
}

pub fn inspect_references(
    circuit: &Circuit,
    root: NodeId,
    allow_free: bool,
) -> Result<ReferenceInspection<'_>, Error> {
    resolve_variables(circuit, [root], Vec::new(), allow_free)
}

impl Resolution<'_> {
    fn required(self) -> Result<References, Error> {
        match self {
            Self::Bound(references) => Ok(references),
            Self::Free { name, .. } => {
                Err(Error::new(format!("unbound variable {name} in closure")))
            }
            Self::Conflict { variable, .. } => Err(Error::new(format!(
                "shared variable {variable:?} refers to two declarations"
            ))),
        }
    }
}

/// Keep the identities of failed bindings so the caller can report their live source locations.
pub fn lexical_references(circuit: &Circuit, root: NodeId) -> Result<Resolution<'_>, Error> {
    Ok(inspect_references(circuit, root, false)?.resolution())
}

pub fn resolve(circuit: &Circuit, closure: NodeId) -> Result<References, Error> {
    lexical_references(circuit, closure)?.required()
}

/// A detached closure body may have separate key, left and right parameters.
pub fn resolve_body(
    circuit: &Circuit,
    parameters: impl IntoIterator<Item = NodeId>,
    body: NodeId,
) -> Result<Resolution<'_>, Error> {
    let mut bindings = Vec::new();
    for id in parameters {
        let parameter = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing closure parameter"))?;
        bindings.push((name(parameter, "name")?, id));
    }
    Ok(resolve_variables(circuit, [body], bindings, false)?.resolution())
}

/// A row-map composition reduces `|args| after(&before.body)`. The callee is
/// visited first and the argument second, both inside the producer's parameter scope.
pub fn inspect_composition(
    circuit: &Circuit,
    before: NodeId,
    after: NodeId,
) -> Result<Option<BindingDiagnostic<'_>>, Error> {
    let before = circuit.node(before).ok_or_else(|| Error::new("missing producer closure"))?;
    let mut parameters = Vec::new();
    for id in before.edges("parameters")? {
        let parameter = circuit.node(id).expect("validated parameter");
        parameters.push((name(parameter, "name")?, id));
    }
    Ok(resolve_variables(circuit, [after, before.edge("body")?], parameters, true)?.diagnostic)
}

/// An unbound name is a normal outcome when testing a partially projected closure.
fn resolve_variables<'a>(
    circuit: &'a Circuit,
    roots: impl IntoIterator<Item = NodeId, IntoIter: DoubleEndedIterator>,
    mut bindings: Vec<(&'a str, NodeId)>,
    allow_free: bool,
) -> Result<ReferenceInspection<'a>, Error> {
    let mut result = ReferenceInspection::default();
    let mut path = HashSet::new();
    // A shared expression can resolve differently under a shadowing declaration.
    // Reuse a traversal only in its original scope and only if it left that
    // scope intact. Statement and parameter declarations must still take effect.
    let mut completed = HashSet::new();
    let mut scope = 0;
    let mut next_scope = 1;
    let mut pending: Vec<_> = roots.into_iter().rev().map(Visit::Enter).collect();
    while let Some(visit) = pending.pop() {
        let id = match visit {
            Visit::Enter(id) => id,
            Visit::Leave(id, entry_scope) => {
                path.remove(&id);
                if scope == entry_scope {
                    completed.insert((id, scope));
                }
                continue;
            }
            Visit::Bind(name, id) => {
                bindings.push((name, id));
                scope = next_scope;
                next_scope += 1;
                continue;
            }
            Visit::Record(variable, current) => {
                let previous = *result.bindings.entry(variable).or_insert(current);
                if previous != current {
                    result.diagnostic = Some(BindingDiagnostic::Conflict { variable, previous, current });
                    break;
                }
                continue;
            }
            Visit::Restore(size, outer) => {
                scope = outer;
                bindings.truncate(size);
                continue;
            }
        };
        if completed.contains(&(id, scope)) {
            continue;
        }
        if !path.insert(id) {
            return Err(Error::new("cycle in a closure"));
        }
        pending.push(Visit::Leave(id, scope));
        let node = circuit.node(id).expect("validated inner edge");
        match node.inner_kind() {
            Some(InnerKind::Expression(Expr::VariablePath)) => {
                let variable = name(node, "variable")?;
                let Some(declaration) = bindings
                    .iter()
                    .rev()
                    .find_map(|&(bound, id)| (bound == variable).then_some(id))
                else {
                    if allow_free {
                        result.has_free_variables = true;
                        continue;
                    }
                    result.diagnostic = Some(BindingDiagnostic::Free { variable: id, name: variable });
                    break;
                };
                pending.push(Visit::Record(id, declaration));
            }
            Some(InnerKind::Expression(Expr::Closure)) => {
                pending.push(Visit::Restore(bindings.len(), scope));
                pending.push(Visit::Enter(node.edge("body")?));
                pending.extend(
                    node.edges("parameters")?
                        .into_iter()
                        .rev()
                        .map(Visit::Enter),
                );
            }
            Some(InnerKind::Parameter) => {
                pending.push(Visit::Bind(name(node, "name")?, id));
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                let variable = node.edge("variable")?;
                let path = circuit.node(variable).expect("validated let variable");
                // Its initializer sees the outer binding of the same name.
                pending.push(Visit::Restore(bindings.len(), scope));
                pending.push(Visit::Enter(node.edge("consumer")?));
                pending.push(Visit::Record(variable, id));
                pending.push(Visit::Bind(name(path, "variable")?, id));
                pending.push(Visit::Enter(node.edge("initializer")?));
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                pending.push(Visit::Restore(bindings.len(), scope));
                if let Some(last) = node.field("lastExpression").and_then(|f| f.node_id()) {
                    pending.push(Visit::Enter(last));
                }
                pending.extend(node.edges("contents")?.into_iter().rev().map(Visit::Enter));
            }
            _ => {
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
    Ok(result)
}

pub fn expression_children(node: &Node) -> Vec<NodeId> {
    let mut children = Vec::new();
    push_expression_children(node, &mut children);
    children
}

pub fn push_expression_children(node: &Node, children: &mut Vec<NodeId>) {
    if !matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
        for (name, field) in node.fields() {
            // The aggregate list serializes its cached empty row for replay; visitors skip that cache.
            if !matches!(name, "type" | "originalStruct" | "cachedEmptySetResult") {
                field.collect_inner_edges(children);
            }
        }
    }
}

/// A movable expression may refer to its own declarations and an optional outer parameter.
pub fn closed_over(
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
