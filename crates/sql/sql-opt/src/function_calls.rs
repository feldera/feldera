//! Inspect named function calls in the executable part of a circuit. Table
//! column defaults live in source metadata and have separate validation rules.
use feldera_sql_ir::{
    BodyOrder, Circuit, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId,
    OperatorKind, provenance::SourceRange,
};
use serde::Serialize;
use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
};

pub struct ConnectorMetadataCall<'a> {
    pub name: Cow<'a, str>,
    pub position: SourceRange,
}

/// Match DBSPPath.asString(), which joins identifiers and skips generic arguments.
pub(super) fn function_name<'a>(
    circuit: &'a Circuit,
    node: &Node,
) -> Result<Option<Cow<'a, str>>, Error> {
    if !matches!(node.inner_kind(), Some(InnerKind::Expression(Expr::Apply))) {
        return Ok(None);
    }
    let function = circuit
        .node(node.edge("function")?)
        .expect("resolved function edge");
    if !matches!(
        function.inner_kind(),
        Some(InnerKind::Expression(Expr::Path))
    ) {
        return Ok(None);
    }
    let path = circuit
        .node(function.edge("path")?)
        .expect("resolved path edge");
    let components = path.edges("components")?;
    let identifier = |segment| {
        circuit
            .node(segment)
            .expect("resolved path segment")
            .data("identifier")
            .and_then(|n| n.as_str())
            .ok_or_else(|| Error::new("function path requires a segment identifier"))
    };
    if let [segment] = components.as_slice() {
        return Ok(Some(Cow::Borrowed(identifier(*segment)?)));
    }
    let names = components
        .into_iter()
        .map(identifier)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(Cow::Owned(names.join("::"))))
}

pub fn is_clock_call(name: &str, arity: usize) -> bool {
    arity == 0 && name.eq_ignore_ascii_case("now")
}

/// Find the last zero-argument NOW call in each root's visitor order. A shared
/// subtree contributes its last call at every occurrence, including across roots.
pub fn temporal_clock_sites(
    circuit: &Circuit,
    roots: &[NodeId],
) -> Result<Vec<(NodeId, NodeId)>, Error> {
    let mut last = HashMap::new();
    let mut active = HashSet::new();
    let mut pending: Vec<_> = roots.iter().rev().map(|&root| (root, false)).collect();
    while let Some((id, leaving)) = pending.pop() {
        if last.contains_key(&id) {
            continue;
        }
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing clock expression"))?;
        let children = crate::lexical_scope::expression_children(node);
        if leaving {
            let site = children.iter().rev().find_map(|child| last[child]);
            last.insert(id, site);
            active.remove(&id);
        } else if matches!(function_name(circuit, node)?, Some(name) if is_clock_call(&name, node.edges("arguments")?.len()))
        {
            last.insert(id, Some(id));
        } else {
            if !active.insert(id) {
                return Err(Error::new("cycle in clock expression"));
            }
            pending.push((id, true));
            pending.extend(children.into_iter().rev().map(|child| (child, false)));
        }
    }
    Ok(roots
        .iter()
        .filter_map(|root| Some((*root, last[root]?)))
        .collect())
}

pub(super) fn contains_now(circuit: &Circuit, roots: &[NodeId]) -> Result<bool, Error> {
    let mut pending = roots.to_vec();
    let mut seen = HashSet::new();
    while let Some(id) = pending.pop() {
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing executable node"))?;
        if !seen.insert(id) || matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
            continue;
        }
        if let Some(name) = function_name(circuit, node)? {
            if is_clock_call(&name, node.edges("arguments")?.len()) {
                return Ok(true);
            }
        }
        crate::lexical_scope::push_expression_children(node, &mut pending);
    }
    Ok(false)
}

/// The clock source itself and uncalled declarations do not activate temporal rewriting.
pub fn circuit_contains_now(circuit: &Circuit) -> Result<bool, Error> {
    let mut roots = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("ordered operator");
        if matches!(
            node.operator_kind(),
            Some(OperatorKind::Now | OperatorKind::Nested)
        ) {
            continue;
        }
        for (key, field) in node.fields() {
            if !matches!(key, "inputs" | "metadata" | "annotations" | "provenance") {
                field.collect_inner_edges(&mut roots);
            }
        }
    }
    contains_now(circuit, &roots)
}

pub fn is_connector_metadata(name: &str) -> bool {
    name.eq_ignore_ascii_case("connector_metadata")
}

pub fn connector_metadata_call(
    circuit: &Circuit,
) -> Result<Option<ConnectorMetadataCall<'_>>, Error> {
    let mut roots = Vec::new();
    for declaration in circuit.root().edges("declarations")? {
        roots.push(
            circuit
                .node(declaration)
                .expect("declared item")
                .edge("item")?,
        );
    }
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        if node.operator_kind() == Some(OperatorKind::Nested) {
            continue;
        }
        for (key, field) in node.fields() {
            if !matches!(key, "inputs" | "metadata" | "annotations" | "provenance") {
                field.collect_inner_edges(&mut roots);
            }
        }
    }

    let found = find_call(circuit, roots, false, is_connector_metadata)?;
    found
        .map(|(node, name)| {
            let position = node
                .field("position")
                .map(Field::source_range)
                .transpose()?
                .unwrap_or_default();
            Ok(ConnectorMetadataCall { name, position })
        })
        .transpose()
}

fn find_call<'a>(
    circuit: &'a Circuit,
    roots: Vec<NodeId>,
    skip_types: bool,
    matches: impl Fn(&str) -> bool,
) -> Result<Option<(&'a Node, Cow<'a, str>)>, Error> {
    let mut visited = HashSet::new();
    let mut pending: Vec<_> = roots.into_iter().rev().map(|id| (id, false)).collect();
    let mut children = Vec::new();
    while let Some((id, postorder)) = pending.pop() {
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing executable node"))?;
        if postorder {
            if let Some(name) = function_name(circuit, node)?.filter(|n| matches(n.as_ref())) {
                return Ok(Some((node, name)));
            }
        } else if visited.insert(id)
            && !(skip_types && matches!(node.inner_kind(), Some(InnerKind::Type(_))))
        {
            pending.push((id, true));
            for (_, field) in node.fields() {
                field.collect_inner_edges(&mut children);
            }
            pending.extend(children.drain(..).rev().map(|child| (child, false)));
        }
    }
    Ok(None)
}

/// Indexes refer to the live declarations or to columns in the source's original
/// declaration order. The caller uses them to attach diagnostics to the SQL text.
#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(untagged)]
pub enum NondeterministicInitializer {
    Function { declaration: u64 },
    Default { source: u64, column: usize },
}

pub fn nondeterministic_initializers(
    circuit: &Circuit,
) -> Result<Vec<NondeterministicInitializer>, Error> {
    let mut names = HashSet::from([String::from("now")]);
    let mut findings = Vec::new();
    for id in circuit.root().edges("declarations")? {
        let decl = circuit.node(id).expect("declared circuit item");
        let item = circuit
            .node(decl.edge("item")?)
            .expect("resolved declaration");
        if item.class() != "DBSPFunctionItem" {
            continue;
        }
        let function = circuit
            .node(item.edge("function")?)
            .expect("declared function");
        let Some(body) = function.field("body").and_then(Field::node_id) else {
            continue;
        };
        if find_call(circuit, vec![body], true, |n| {
            names.contains(&n.to_lowercase())
        })?
        .is_some()
        {
            let name = function
                .data("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("function declaration requires a name"))?;
            names.insert(name.to_lowercase());
            let NodeId::Outer(declaration) = id else {
                unreachable!("declaration is an outer node")
            };
            findings.push(NondeterministicInitializer::Function { declaration });
        }
    }
    for id in circuit.operator_order(BodyOrder::Preorder)? {
        let op = circuit.node(id).expect("validated circuit member");
        if op.operator_kind() != Some(OperatorKind::SourceMultiset) {
            continue;
        }
        let Some(Field::Object(metadata)) = op.field("metadata") else {
            return Err(Error::new("table source requires metadata"));
        };
        let Some(Field::Array(columns)) = metadata.get("columnMetadata") else {
            return Err(Error::new("table source requires column metadata"));
        };
        for (column, field) in columns.iter().enumerate() {
            let Field::Object(column_metadata) = field else {
                return Err(Error::new("invalid source column metadata"));
            };
            if let Some(default) = column_metadata.get("defaultValue").and_then(Field::node_id) {
                if find_call(circuit, vec![default], true, |n| {
                    names.contains(&n.to_lowercase())
                })?
                .is_some()
                {
                    let NodeId::Outer(source) = id else {
                        unreachable!("source is an outer node")
                    };
                    findings.push(NondeterministicInitializer::Default { source, column });
                }
            }
        }
    }
    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};

    #[test]
    fn the_last_zero_argument_clock_call_supplies_each_rewrite_site() -> Result<(), Error> {
        let circuit = Circuit::from_json(
            json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]}),
        )?;
        let shared = call(10, "NoW");
        let distinct = call(20, "now");
        let mut with_argument = call(30, "now");
        with_argument["arguments"] = json!([{"node":10}]);
        let tuple =
            json!({"class":"DBSPTupleExpression","id":1,"fields":[shared,distinct,with_argument]});
        let (circuit, roots) =
            circuit.with_inner_roots([tuple, json!({"node":20}), call(40, "other")])?;
        assert_eq!(
            temporal_clock_sites(&circuit, &roots)?,
            [
                (NodeId::Inner(1), NodeId::Inner(10)),
                (NodeId::Inner(20), NodeId::Inner(20)),
            ]
        );
        Ok(())
    }

    fn call(id: u64, name: &str) -> Value {
        json!({"class":"DBSPApplyExpression","id":id,"arguments":[],
        "function":{"class":"DBSPPathExpression","id":id+1,
            "path":{"class":"DBSPPath","id":id+2,"components":[
                {"class":"DBSPSimplePathSegment","id":id+3,"identifier":name,"genericArgs":[]}
            ]}}})
    }

    #[test]
    fn a_shared_connector_call_is_legal_in_defaults_and_illegal_in_executable_code() {
        let mut expr = call(7, "CoNnEcToR_MeTaDaTa");
        expr["position"] =
            json!({"start_line_number":3,"start_column":5,"end_line_number":3,"end_column":24});
        let mut wire = json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
            {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[],
             "metadata":{"columnMetadata":[{"defaultValue":expr}]}}
        ]});
        assert!(
            connector_metadata_call(&Circuit::from_json(wire.clone()).unwrap())
                .unwrap()
                .is_none()
        );
        wire["allOperators"].as_array_mut().unwrap().push(json!({
            "class":"DBSPMapOperator","id":2,"inputs":[{"operator":{"node":1},"outputNumber":0}],
            "function":{"node":7}
        }));
        let circuit = Circuit::from_json(wire).unwrap();
        let call = connector_metadata_call(&circuit).unwrap().unwrap();
        assert_eq!(call.name, "CoNnEcToR_MeTaDaTa");
        assert_eq!(
            (call.position.start_line_number, call.position.start_column),
            (3, 5)
        );
    }

    #[test]
    fn nondeterminism_propagates_through_earlier_functions_to_column_defaults() {
        let decl = |id: u64, name: &str, callee| {
            json!({"class":"DBSPDeclaration","id":id,
            "item":{"class":"DBSPFunctionItem","id":id*10,
                "function":{"class":"DBSPFunction","id":id*10+1,"name":name,"body":call(id*10+2,callee)}}})
        };
        let circuit=Circuit::from_json(json!({"class":"DBSPCircuit","id":0,
            "declarations":[decl(1,"clock","NoW"),decl(2,"alias","CLOCK"),decl(3,"pure","abs")],
            "allOperators":[{"class":"DBSPSourceMultisetOperator","id":4,"inputs":[],
                "metadata":{"columnMetadata":[{"defaultValue":call(45,"pure")},{"defaultValue":call(50,"alias")}]}}]})).unwrap();
        use NondeterministicInitializer::*;
        assert_eq!(
            nondeterministic_initializers(&circuit).unwrap(),
            [
                Function { declaration: 1 },
                Function { declaration: 2 },
                Default {
                    source: 4,
                    column: 1
                }
            ]
        );
    }
}
