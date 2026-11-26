//! Structural comparisons for Java circuit dumps, independent of either compiler.
//!
//! Allocation ids and definition placement may differ. Node sharing, ordered edges,
//! output ports, metadata, source positions, and annotations must remain identical.

use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;

mod dump;
pub use dump::{canonical_dump_bytes, compare_dumps, compare_final_s3};
pub mod corpus;

pub use feldera_sql_ir::parse_json;

/// A malformed circuit or the location of its first structural difference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComparisonError {
    /// JSON pointer into the input or canonical graph (see the crate README).
    pub path: String,
    /// Explanation without SQL text or literal values from the dump.
    pub message: String,
}

impl ComparisonError {
    fn new(path: &str, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
        }
    }
}

impl fmt::Display for ComparisonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}: {}",
            if self.path.is_empty() {
                "/"
            } else {
                &self.path
            },
            self.message
        )
    }
}

impl std::error::Error for ComparisonError {}

type Result<T> = std::result::Result<T, ComparisonError>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
enum Namespace {
    Inner,
    Outer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct NodeId {
    namespace: Namespace,
    id: u64,
}

/// The Java serializers use independent inner and outer id counters. All fields
/// contain inner nodes except these outer-node collections and OutputPort links.
fn child_namespace(parent: &Map<String, Value>, field: &str) -> Namespace {
    if matches!(field, "allOperators" | "declarations")
        || (field == "operator" && parent.contains_key("outputNumber"))
    {
        Namespace::Outer
    } else {
        Namespace::Inner
    }
}

fn pointer(path: &str, field: &str) -> String {
    format!("{path}/{}", field.replace('~', "~0").replace('/', "~1"))
}

fn node_id(value: &Value, namespace: Namespace, path: &str) -> Result<Option<NodeId>> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let field = if object.contains_key("class") && object.contains_key("id") {
        if !object["class"].is_string() {
            return Err(ComparisonError::new(path, "node class must be a string"));
        }
        "id"
    } else if object.len() == 1 && object.contains_key("node") {
        "node"
    } else {
        return Ok(None);
    };
    let id = object[field].as_u64().ok_or_else(|| {
        ComparisonError::new(
            &pointer(path, field),
            "node id must be a nonnegative integer",
        )
    })?;
    Ok(Some(NodeId { namespace, id }))
}

/// Annotations contain their own ids and class tags, not circuit references.
/// Root metadata is the schema JSON; operator metadata can contain IR expressions.
fn is_plain_data(field: &str, root: bool) -> bool {
    field == "annotations" || (root && field == "metadata")
}

struct Graph<'a> {
    definitions: HashMap<NodeId, &'a Value>,
}

struct PortReference {
    source: NodeId,
    number: usize,
    path: String,
}

fn read_outer_ports(
    object: &Map<String, Value>,
    path: &str,
    ports: &mut Vec<PortReference>,
) -> Result<()> {
    if let Some(operators) = object.get("allOperators") {
        let list_path = pointer(path, "allOperators");
        let operators = operators
            .as_array()
            .ok_or_else(|| ComparisonError::new(&list_path, "operator list must be an array"))?;
        for (index, operator) in operators.iter().enumerate() {
            let operator_path = pointer(&list_path, &index.to_string());
            if node_id(operator, Namespace::Outer, &operator_path)?.is_none() {
                return Err(ComparisonError::new(
                    &operator_path,
                    "expected an operator definition or reference",
                ));
            }
        }
    }
    for field in ["inputs", "internalOutputs"] {
        let Some(links) = object.get(field) else {
            continue;
        };
        let links_path = pointer(path, field);
        let links = links
            .as_array()
            .ok_or_else(|| ComparisonError::new(&links_path, "port list must be an array"))?;
        for (index, link) in links.iter().enumerate() {
            if field == "internalOutputs" && link.is_null() {
                continue;
            }
            let path = pointer(&links_path, &index.to_string());
            let source = node_id(
                &link["operator"],
                Namespace::Outer,
                &pointer(&path, "operator"),
            )?
            .ok_or_else(|| {
                ComparisonError::new(
                    &pointer(&path, "operator"),
                    "expected a port source operator",
                )
            })?;
            let path = pointer(&path, "outputNumber");
            let number = link["outputNumber"]
                .as_u64()
                .and_then(|value| usize::try_from(value).ok())
                .ok_or_else(|| {
                    ComparisonError::new(&path, "output port must be a nonnegative integer")
                })?;
            ports.push(PortReference {
                source,
                number,
                path,
            });
        }
    }
    Ok(())
}

fn has_output(operator: &Value, port: usize) -> bool {
    match operator["class"].as_str() {
        Some("DBSPNestedOperator") => operator["internalOutputs"]
            .as_array()
            .and_then(|outputs| outputs.get(port))
            .is_some_and(|output| !output.is_null()),
        Some("DBSPInputMapWithWaterlineOperator") => port < 3,
        Some("DBSPControlledKeyFilterOperator") => port < 2,
        Some("DBSPCircuit" | "DBSPDeclaration") => false,
        _ => port == 0,
    }
}

impl<'a> Graph<'a> {
    fn read(root: &'a Value, namespace: Namespace) -> Result<Self> {
        if namespace == Namespace::Outer
            && (root.get("class").and_then(Value::as_str) != Some("DBSPCircuit")
                || !root.get("allOperators").is_some_and(Value::is_array)
                || !root.get("declarations").is_some_and(Value::is_array)
                || root.get("id").is_none())
        {
            return Err(ComparisonError::new(
                "",
                "expected a DBSPCircuit with id, allOperators, and declarations; extract the circuit from any envelope first",
            ));
        }
        if !root.get("class").is_some_and(Value::is_string)
            || root.get("id").and_then(Value::as_u64).is_none()
        {
            return Err(ComparisonError::new("", "expected an IR node definition"));
        }
        let mut graph = Self {
            definitions: HashMap::new(),
        };
        let mut references = Vec::new();
        let mut ports = Vec::new();
        let mut pending = vec![(root, namespace, String::new(), true)];
        while let Some((value, namespace, path, is_root)) = pending.pop() {
            if let Some(id) = node_id(value, namespace, &path)? {
                if value.get("class").is_some() {
                    if graph.definitions.insert(id, value).is_some() {
                        return Err(ComparisonError::new(
                            &path,
                            "duplicate node definition in the same namespace",
                        ));
                    }
                    if namespace == Namespace::Outer {
                        read_outer_ports(
                            value.as_object().expect("node definition"),
                            &path,
                            &mut ports,
                        )?;
                    }
                } else {
                    references.push((id, path));
                    continue;
                }
            }
            match value {
                Value::Object(object) => {
                    // Reverse sorted insertion makes diagnostics independent of map features.
                    let names: BTreeSet<_> = object.keys().collect();
                    for name in names.into_iter().rev() {
                        if !is_plain_data(name, is_root) {
                            pending.push((
                                &object[name],
                                child_namespace(object, name),
                                pointer(&path, name),
                                false,
                            ));
                        }
                    }
                }
                Value::Array(values) => {
                    for (index, value) in values.iter().enumerate().rev() {
                        pending.push((value, namespace, pointer(&path, &index.to_string()), false));
                    }
                }
                _ => {}
            }
        }
        for (id, path) in references {
            if !graph.definitions.contains_key(&id) {
                return Err(ComparisonError::new(
                    &path,
                    "reference to an undefined node in this namespace",
                ));
            }
        }
        for port in ports {
            if !graph
                .definitions
                .get(&port.source)
                .is_some_and(|operator| has_output(operator, port.number))
            {
                return Err(ComparisonError::new(
                    &port.path,
                    "source operator has no such output port",
                ));
            }
        }
        if namespace == Namespace::Outer {
            graph.check_body_tree(root)?;
        }
        Ok(graph)
    }

    fn check_body_tree(&self, root: &Value) -> Result<()> {
        let mut stack = vec![root];
        let mut members = HashSet::new();
        while let Some(body) = stack.pop() {
            let slots = body["allOperators"].as_array().ok_or_else(|| {
                ComparisonError::new(
                    "/allOperators",
                    "missing circuit body. Supply an operator list.",
                )
            })?;
            for slot in slots {
                let id = node_id(slot, Namespace::Outer, "")?.expect("validated body slot");
                if !members.insert(id) {
                    return Err(ComparisonError::new("/allOperators", format!(
                        "operator {} occurs in multiple body slots. Keep one slot per operator under the circuit root.",
                        id.id
                    )));
                }
                let member = self.definitions[&id];
                if member["class"] == "DBSPNestedOperator" {
                    stack.push(member);
                }
            }
        }
        let missing = self
            .definitions
            .iter()
            .filter(|(id, node)| {
                id.namespace == Namespace::Outer
                    && node["class"]
                        .as_str()
                        .is_some_and(|class| class.ends_with("Operator"))
                    && !members.contains(*id)
            })
            .map(|(id, _)| id.id)
            .min();
        if let Some(id) = missing {
            return Err(ComparisonError::new(
                "/allOperators",
                format!("operator {id} has no body reachable from the circuit root. Add it to a reachable body."),
            ));
        }
        Ok(())
    }
}

// References have their own variant so ordinary JSON objects cannot masquerade
// as normalized references and cause a false equality.
#[derive(PartialEq, Serialize)]
enum CanonicalValue {
    Reference(Namespace, usize),
    Object(BTreeMap<String, CanonicalValue>),
    Array(Vec<CanonicalValue>),
    Scalar(Value),
}

// Plain metadata must retain class/id fields as data, but JSON key order is
// incidental both when comparing graphs and when encoding their fingerprints.
fn canonical_plain(value: &Value, depth: usize) -> Result<CanonicalValue> {
    if depth > 128 {
        return Err(ComparisonError::new(
            "",
            "JSON nesting exceeds the comparator limit of 128",
        ));
    }
    match value {
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), canonical_plain(value, depth + 1)?)))
            .collect::<Result<BTreeMap<_, _>>>()
            .map(CanonicalValue::Object),
        Value::Array(array) => array
            .iter()
            .map(|value| canonical_plain(value, depth + 1))
            .collect::<Result<Vec<_>>>()
            .map(CanonicalValue::Array),
        _ => Ok(CanonicalValue::Scalar(value.clone())),
    }
}

struct Canonicalizer<'g, 'a> {
    graph: &'g Graph<'a>,
    ids: HashMap<NodeId, usize>,
    pending: VecDeque<NodeId>,
    inner_count: usize,
    outer_count: usize,
}

impl Canonicalizer<'_, '_> {
    fn reference(&mut self, id: NodeId) -> CanonicalValue {
        let next = match id.namespace {
            Namespace::Inner => &mut self.inner_count,
            Namespace::Outer => &mut self.outer_count,
        };
        let index = *self.ids.entry(id).or_insert_with(|| {
            let index = *next;
            *next += 1;
            self.pending.push_back(id);
            index
        });
        CanonicalValue::Reference(id.namespace, index)
    }

    // Only JSON nesting recurses. Graph edges enqueue definitions instead, so a
    // long chain or recursive circuit cannot overflow the stack or expand a DAG.
    fn value(
        &mut self,
        value: &Value,
        namespace: Namespace,
        path: &str,
        depth: usize,
    ) -> Result<CanonicalValue> {
        if depth > 128 {
            return Err(ComparisonError::new(
                path,
                "JSON nesting exceeds the comparator limit of 128",
            ));
        }
        if let Some(id) = node_id(value, namespace, path)? {
            return Ok(self.reference(id));
        }
        match value {
            Value::Object(object) => self.object(object, path, depth, false, false),
            Value::Array(values) => values
                .iter()
                .enumerate()
                .map(|(i, v)| self.value(v, namespace, &pointer(path, &i.to_string()), depth + 1))
                .collect::<Result<Vec<_>>>()
                .map(CanonicalValue::Array),
            _ => Ok(CanonicalValue::Scalar(value.clone())),
        }
    }

    fn object(
        &mut self,
        object: &Map<String, Value>,
        path: &str,
        depth: usize,
        definition: bool,
        root: bool,
    ) -> Result<CanonicalValue> {
        let mut result = BTreeMap::new();
        for name in object.keys().collect::<BTreeSet<_>>() {
            if definition && name == "id" {
                continue;
            }
            let value = if is_plain_data(name, root) {
                canonical_plain(&object[name], depth + 1)?
            } else {
                self.value(
                    &object[name],
                    child_namespace(object, name),
                    &pointer(path, name),
                    depth + 1,
                )?
            };
            result.insert(name.clone(), value);
        }
        Ok(CanonicalValue::Object(result))
    }

    fn run(root: &Value, namespace: Namespace) -> Result<CanonicalValue> {
        let graph = Graph::read(root, namespace)?;
        let root_id = node_id(root, namespace, "")?.expect("validated root id");
        graph.canonicalize(root_id)
    }
}

impl Graph<'_> {
    fn canonicalize(&self, root_id: NodeId) -> Result<CanonicalValue> {
        let mut canonical = Canonicalizer {
            graph: self,
            ids: HashMap::new(),
            pending: VecDeque::new(),
            inner_count: 0,
            outer_count: 0,
        };
        canonical.reference(root_id);
        let mut inner = Vec::new();
        let mut outer = Vec::new();
        while let Some(id) = canonical.pending.pop_front() {
            let node = canonical.graph.definitions[&id];
            let path = format!(
                "/{}/{}",
                match id.namespace {
                    Namespace::Inner => "inner",
                    Namespace::Outer => "outer",
                },
                canonical.ids[&id]
            );
            let normalized = canonical.object(
                node.as_object().expect("node definition"),
                &path,
                0,
                true,
                id == root_id,
            )?;
            match id.namespace {
                Namespace::Inner => inner.push(normalized),
                Namespace::Outer => outer.push(normalized),
            }
        }
        Ok(CanonicalValue::Object(BTreeMap::from([
            ("inner".into(), CanonicalValue::Array(inner)),
            ("outer".into(), CanonicalValue::Array(outer)),
        ])))
    }
}

/// Compare raw Java DBSPCircuit JSON, preserving sharing in both namespaces.
///
/// Both inputs are validated before comparing. Malformed references, nonexistent
/// ports, and duplicate definitions fail even when the inputs are otherwise identical. This is not an
/// IR type checker or a validator of versioned seam envelopes.
pub fn compare_circuits(expected: &Value, actual: &Value) -> Result<()> {
    compare_graphs(expected, actual, Namespace::Outer)
}

/// Compare independently serialized expressions, including typed corpus Z-sets.
/// Each expression owns its own inner-node id namespace.
pub fn compare_expressions(expected: &Value, actual: &Value) -> Result<()> {
    compare_graphs(expected, actual, Namespace::Inner)
}

fn compare_graphs(expected: &Value, actual: &Value, namespace: Namespace) -> Result<()> {
    let normalize = |value, side| {
        Canonicalizer::run(value, namespace).map_err(|mut error| {
            error.message = format!("{side} circuit: {}", error.message);
            error
        })
    };
    let expected = normalize(expected, "expected")?;
    let actual = normalize(actual, "actual")?;
    let mut pending = vec![(&expected, &actual, String::new())];
    while let Some((expected, actual, path)) = pending.pop() {
        match (expected, actual) {
            (CanonicalValue::Object(left), CanonicalValue::Object(right)) => {
                let names: BTreeSet<_> = left.keys().chain(right.keys()).collect();
                for name in names.into_iter().rev() {
                    let child_path = pointer(&path, name);
                    match (left.get(name), right.get(name)) {
                        (Some(a), Some(b)) => pending.push((a, b, child_path)),
                        _ => {
                            return Err(ComparisonError::new(
                                &child_path,
                                "field exists in only one circuit",
                            ));
                        }
                    }
                }
            }
            (CanonicalValue::Array(left), CanonicalValue::Array(right)) => {
                if left.len() != right.len() {
                    return Err(ComparisonError::new(&path, "array lengths differ"));
                }
                for (i, (a, b)) in left.iter().zip(right).enumerate().rev() {
                    pending.push((a, b, pointer(&path, &i.to_string())));
                }
            }
            _ if expected == actual => {}
            _ => return Err(ComparisonError::new(&path, "values or JSON types differ")),
        }
    }
    Ok(())
}
