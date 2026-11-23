//! Lexical declarations and captures in a serialized closure.
use feldera_sql_ir::{Circuit, Error, ExpressionKind as Expr, InnerKind, Node, NodeId};
use std::collections::{HashMap, HashSet};

pub(crate) type References = HashMap<NodeId, NodeId>;

fn name<'a>(node: &'a Node, field: &str) -> Result<&'a str, Error> {
    node.data(field)
        .and_then(|value| value.as_str())
        .ok_or_else(|| Error::new(format!("{} needs a {field} name", node.class())))
}

enum Visit<'a> {
    Enter(NodeId),
    Leave(NodeId),
    Bind(&'a str, NodeId),
    Record(NodeId, NodeId),
    Restore(usize),
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
    resolve_variables(circuit, root, Vec::new(), allow_free)
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

    fn optional(self) -> Result<Option<References>, Error> {
        match self {
            Self::Free { .. } => Ok(None),
            other => other.required().map(Some),
        }
    }
}

/// Keep the identities of failed bindings so the caller can report their live source locations.
pub fn lexical_references(circuit: &Circuit, root: NodeId) -> Result<Resolution<'_>, Error> {
    Ok(inspect_references(circuit, root, false)?.resolution())
}

pub(crate) fn resolve(circuit: &Circuit, closure: NodeId) -> Result<References, Error> {
    lexical_references(circuit, closure)?.required()
}

pub(crate) fn has_free_variables(circuit: &Circuit, closure: NodeId) -> Result<bool, Error> {
    let inspection = inspect_references(circuit, closure, true)?;
    let found = inspection.has_free_variables;
    inspection.resolution().required()?;
    Ok(found)
}

/// Resolve a predicate detached from its closure, keeping its row parameter in scope.
pub(crate) fn predicate_references(
    circuit: &Circuit,
    parameter: NodeId,
    body: NodeId,
) -> Result<Option<References>, Error> {
    resolve_body(circuit, [parameter], body)?.optional()
}

/// A detached closure body may have separate key, left and right parameters.
pub(crate) fn resolve_body(
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
    Ok(resolve_variables(circuit, body, bindings, false)?.resolution())
}

/// An unbound name is a normal outcome when testing a partially projected closure.
fn resolve_variables<'a>(
    circuit: &'a Circuit,
    closure: NodeId,
    mut bindings: Vec<(&'a str, NodeId)>,
    allow_free: bool,
) -> Result<ReferenceInspection<'a>, Error> {
    let mut result = ReferenceInspection::default();
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
            Visit::Record(variable, current) => {
                let previous = *result.bindings.entry(variable).or_insert(current);
                if previous != current {
                    result.diagnostic = Some(BindingDiagnostic::Conflict { variable, previous, current });
                    break;
                }
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
                pending.push(Visit::Restore(bindings.len()));
                pending.push(Visit::Enter(node.edge("body")?));
                pending.extend(
                    node.edges("parameters")?
                        .into_iter()
                        .rev()
                        .map(Visit::Enter),
                );
            }
            Some(InnerKind::Parameter) => {
                bindings.push((name(node, "name")?, id));
            }
            Some(InnerKind::Expression(Expr::Let)) => {
                let variable = node.edge("variable")?;
                let path = circuit.node(variable).expect("validated let variable");
                // Its initializer sees the outer binding of the same name.
                pending.push(Visit::Restore(bindings.len()));
                pending.push(Visit::Enter(node.edge("consumer")?));
                pending.push(Visit::Record(variable, id));
                pending.push(Visit::Bind(name(path, "variable")?, id));
                pending.push(Visit::Enter(node.edge("initializer")?));
            }
            Some(InnerKind::Expression(Expr::Block)) => {
                pending.push(Visit::Restore(bindings.len()));
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

pub(crate) fn expression_children(node: &Node) -> Vec<NodeId> {
    let mut children = Vec::new();
    if !matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
        for (name, field) in node.fields() {
            // The aggregate list serializes its cached empty row for replay; visitors skip that cache.
            if !matches!(name, "type" | "originalStruct" | "cachedEmptySetResult") {
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
