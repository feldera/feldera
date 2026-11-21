//! Normalize references to recursive views before building nested circuits.
use crate::rebuild::rebuild_input_closures;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, Field, Node, NodeId, OperatorKind as Op};
use std::collections::{BTreeMap, HashMap};

/// The JVM applies this schedule to the live operators, preserving expression
/// identity and the declaration objects owned by its front end.
#[derive(Default)]
pub struct ViewReferences {
    pub order: Vec<NodeId>,
    pub redirects: BTreeMap<NodeId, BTreeMap<usize, NodeId>>,
}

pub enum ViewResolution {
    Valid(ViewReferences),
    UndeclaredRecursion { operator: NodeId, message: String },
}

pub(super) fn metadata_field<'a>(
    view: &'a Node,
    key: &str,
) -> Result<&'a serde_json::Value, Error> {
    if let Some(Field::Object(metadata)) = view.field("metadata") {
        if let Some(Field::Data(value)) = metadata.get(key) {
            return Ok(value);
        }
    }
    Err(Error::new(format!("view metadata requires {key}")))
}

fn is_recursive(view: &Node) -> Result<bool, Error> {
    metadata_field(view, "recursive")?
        .as_bool()
        .ok_or_else(|| Error::new("invalid recursive view flag"))
}

/// Kosaraju's algorithm with Java's DFS root and input order. Within each
/// component Java subsequently emits operators in their original body order.
fn ordered_components(predecessors: &[Vec<usize>]) -> Vec<Vec<usize>> {
    let mut successors = vec![Vec::new(); predecessors.len()];
    for (to, inputs) in predecessors.iter().enumerate() {
        for &from in inputs {
            successors[from].push(to);
        }
    }
    let mut visited = vec![false; predecessors.len()];
    let mut finish = Vec::with_capacity(predecessors.len());
    let mut stack = Vec::new();
    for root in 0..predecessors.len() {
        if visited[root] {
            continue;
        }
        stack.push((root, false));
        while let Some((vertex, exiting)) = stack.pop() {
            if exiting {
                finish.push(vertex);
            } else if !std::mem::replace(&mut visited[vertex], true) {
                stack.push((vertex, true));
                stack.extend(predecessors[vertex].iter().rev().map(|&v| (v, false)));
            }
        }
    }
    let mut component = vec![usize::MAX; predecessors.len()];
    let mut count = 0;
    let mut pending = Vec::new();
    for root in finish.into_iter().rev() {
        if component[root] != usize::MAX {
            continue;
        }
        component[root] = count;
        pending.push(root);
        while let Some(vertex) = pending.pop() {
            for &reader in &successors[vertex] {
                if component[reader] == usize::MAX {
                    component[reader] = count;
                    pending.push(reader);
                }
            }
        }
        count += 1;
    }
    let mut groups = vec![Vec::new(); count];
    for (index, group) in component.into_iter().enumerate() {
        groups[count - 1 - group].push(index);
    }
    groups
}

/// The flat circuit includes an implicit edge from each recursive view to
/// its declaration. Both recursion passes must use the same dependencies.
pub(super) struct Components<'a> {
    pub ids: Vec<NodeId>,
    pub index: HashMap<NodeId, usize>,
    pub views: HashMap<&'a str, usize>,
    pub declaration_names: HashMap<usize, String>,
    pub groups: Vec<Vec<usize>>,
    pub group_of: Vec<usize>,
}

pub(super) fn components(circuit: &Circuit) -> Result<Option<Components<'_>>, Error> {
    let ids = circuit.root().edges("allOperators")?;
    let node = |i: usize| circuit.node(ids[i]).expect("validated circuit member");
    if !ids
        .iter()
        .any(|&id| circuit.node(id).and_then(Node::operator_kind) == Some(Op::ViewDeclaration))
    {
        return Ok(None);
    }

    let index: HashMap<_, _> = ids.iter().copied().zip(0..).collect();
    let mut views = HashMap::new();
    let mut declaration_names = HashMap::new();
    for i in 0..ids.len() {
        match node(i).operator_kind() {
            Some(Op::View) => {
                views.insert(node(i).relation_name("viewName")?, i);
            }
            Some(Op::ViewDeclaration) => {
                declaration_names
                    .insert(i, node(i).relation_name("tableName")?.replace("-decl", ""));
            }
            _ => (),
        }
    }
    // A declaration has an implicit dependency on its definition, including
    // when that definition follows it in the serialized circuit.
    let mut predecessors = Vec::with_capacity(ids.len());
    for i in 0..ids.len() {
        let inputs = if let Some(name) = declaration_names.get(&i) {
            views.get(name.as_str()).copied().into_iter().collect()
        } else {
            node(i)
                .inputs()
                .map(|p| {
                    index.get(&p.operator).copied().ok_or_else(|| {
                        Error::new("expected a flat circuit before recursion analysis")
                    })
                })
                .collect::<Result<_, _>>()?
        };
        predecessors.push(inputs);
    }
    let groups = ordered_components(&predecessors);
    let mut group_of = vec![0; ids.len()];
    for (group, members) in groups.iter().enumerate() {
        for &i in members {
            group_of[i] = group;
        }
    }
    Ok(Some(Components {
        ids,
        index,
        views,
        declaration_names,
        groups,
        group_of,
    }))
}

pub fn resolve_view_references(circuit: &Circuit) -> Result<ViewResolution, Error> {
    let mut edits = ViewReferences::default();
    let Some(Components {
        ids,
        index,
        views,
        declaration_names,
        groups,
        group_of,
    }) = components(circuit)?
    else {
        return Ok(ViewResolution::Valid(edits));
    };
    let node = |i: usize| circuit.node(ids[i]).expect("validated circuit member");
    let mut declared = HashMap::new();
    for (group, members) in groups.iter().enumerate() {
        for &i in members {
            let operator = node(i);
            edits.order.push(ids[i]);
            for (slot, port) in operator.inputs().enumerate() {
                let source = index[&port.operator];
                let target = match node(source).operator_kind() {
                    Some(Op::ViewDeclaration) if group_of[source] != group => {
                        let name = &declaration_names[&source];
                        Some(
                            *views.get(name.as_str()).ok_or_else(|| {
                                Error::new(format!("missing definition of {name}"))
                            })?,
                        )
                    }
                    Some(Op::View) if group_of[source] == group && is_recursive(node(source))? => {
                        declared
                            .get(node(source).relation_name("viewName")?)
                            .copied()
                    }
                    _ => None,
                };
                if let Some(target) = target {
                    edits
                        .redirects
                        .entry(ids[i])
                        .or_default()
                        .insert(slot, ids[target]);
                }
            }
            match operator.operator_kind() {
                Some(Op::ViewDeclaration) => {
                    declared.insert(declaration_names[&i].as_str(), i);
                }
                Some(Op::View)
                    if members.len() > 1
                        && !is_recursive(operator)?
                        && metadata_field(operator, "viewKind")? != "LOCAL" =>
                {
                    let first = members
                        .iter()
                        .copied()
                        .find(|&j| i != j && node(j).operator_kind() == Some(Op::View))
                        .ok_or_else(|| Error::new("recursive component has no other view"))?;
                    let name = operator.relation_name("viewName")?;
                    let other = node(first).relation_name("viewName")?;
                    let message = format!("View '{name}' must be declared either as LOCAL or as RECURSIVE\nsince is is used in the computation of recursive view '{other}'");
                    return Ok(ViewResolution::UndeclaredRecursion {
                        operator: ids[i],
                        message,
                    });
                }
                _ => (),
            }
        }
    }
    if edits.redirects.is_empty() && edits.order == ids {
        edits.order.clear();
    }
    Ok(ViewResolution::Valid(edits))
}

impl ViewReferences {
    pub fn apply(self, circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
        if self.order.is_empty() {
            return Ok(circuit);
        }
        let mut edit = CircuitEditor::new(circuit);
        edit.order_root_operators(&self.order)?;
        for (operator, slots) in self.redirects {
            for (slot, view) in slots {
                edit.redirect_view_reference(operator, slot, view)?;
            }
        }
        rebuild_input_closures(edit.finish_with_rewrites()?, next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cycle(local: bool) -> Circuit {
        let port = |id| json!({"operator":{"node":id},"outputNumber":0});
        Circuit::from_json(json!({"class":"DBSPCircuit","id":0,"declarations":[], "allOperators":[
            {"class":"DBSPViewDeclarationOperator","id":1,"tableName":{"name":"R-decl"},"inputs":[]},
            {"class":"DBSPMapOperator","id":2,"inputs":[port(1)]},
            {"class":"DBSPViewOperator","id":3,"viewName":{"name":"Step"},
             "metadata":{"recursive":false,"viewKind":if local {"LOCAL"} else {"STANDARD"}},"inputs":[port(1)]},
            {"class":"DBSPViewOperator","id":4,"viewName":{"name":"R"},"metadata":{"recursive":true,"viewKind":"STANDARD"},"inputs":[port(3)]},
        ]})).unwrap()
    }

    #[test]
    fn readers_outside_a_cycle_use_the_definition_even_when_it_appears_later() {
        let input = cycle(true);
        let ViewResolution::Valid(plan) = resolve_view_references(&input).unwrap() else {
            panic!("legal recursive view")
        };
        assert_eq!(plan.order, [1, 3, 4, 2].map(NodeId::Outer));
        assert_eq!(plan.redirects.len(), 1);
        assert_eq!(plan.redirects[&NodeId::Outer(2)][&0], NodeId::Outer(4));
    }

    #[test]
    fn illegal_view_identifies_the_other_member_in_source_order() {
        let ViewResolution::UndeclaredRecursion { operator, message } =
            resolve_view_references(&cycle(false)).unwrap()
        else {
            panic!("expected recursive view error")
        };
        assert_eq!(operator, NodeId::Outer(3));
        assert_eq!(message, "View 'Step' must be declared either as LOCAL or as RECURSIVE\nsince is is used in the computation of recursive view 'R'");
    }
}
