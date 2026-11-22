//! Circuit graph exchanged by the SQL compiler and its consumers.
//!
//! Outer nodes (circuits, operators and declarations) and inner nodes (types
//! and expressions) have independent identities. An edge always names a node
//! in one of these arenas, even when its definition occurs at the edge site.

pub mod build;
mod coalesce;
mod prune;
pub use coalesce::CircuitRewrite;
mod edit;
pub use edit::CircuitEditor;
mod expression;
pub use expression::{BinaryOp, ExpressionId, OperatorSite, RefParameter, TypeId};
mod gen2;
mod inner;
mod json;
mod live;
pub use live::FullSnapshotRequired;
mod mir;
mod operator;
pub mod provenance;
mod wire;
pub use wire::LiveCircuitDecoder;

pub use inner::{ExpressionKind, InnerKind, TypeKind};
pub use json::parse_json;
pub use operator::OperatorKind;
mod stream;
pub use stream::{OutputClocks, StreamKind, StreamViolation};

use indexmap::IndexMap;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fmt,
    ops::{Deref, DerefMut},
    sync::Arc,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
pub enum NodeId {
    Outer(u64),
    Inner(u64),
}

impl NodeId {
    fn number(self) -> u64 {
        match self {
            Self::Inner(id) | Self::Outer(id) => id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error(String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// A field containing a graph edge or data. The wire codec remembers the
/// definition site so replay consumers can resolve references in source order.
#[derive(Clone, Debug, PartialEq)]
pub enum Field {
    Definition(NodeId),
    Reference(NodeId),
    Object(IndexMap<String, Field>),
    Array(Vec<Field>),
    Data(Value),
}

impl Field {
    /// Append inner dependencies in their serialized field order.
    pub fn collect_inner_edges(&self, edges: &mut Vec<NodeId>) {
        match self {
            Self::Definition(id @ NodeId::Inner(_)) | Self::Reference(id @ NodeId::Inner(_)) => {
                edges.push(*id)
            }
            Self::Object(fields) => fields.values().for_each(|f| f.collect_inner_edges(edges)),
            Self::Array(fields) => fields.iter().for_each(|f| f.collect_inner_edges(edges)),
            _ => {}
        }
    }

    /// Substitute inner edges in a property, preserving its array or object structure.
    /// Return whether any identity changed. Definitions are relocated at edit completion.
    pub fn map_inner_edges(
        &mut self,
        replace: &mut impl FnMut(NodeId) -> Result<NodeId, Error>,
    ) -> Result<bool, Error> {
        match self {
            Self::Definition(old @ NodeId::Inner(_)) | Self::Reference(old @ NodeId::Inner(_)) => {
                let new = replace(*old)?;
                if !matches!(new, NodeId::Inner(_)) {
                    return Err(Error::new("an inner edge requires an inner node"));
                }
                if new == *old {
                    return Ok(false);
                }
                *self = Field::Reference(new);
                Ok(true)
            }
            Self::Array(fields) => fields.iter_mut().try_fold(false, |changed, field| {
                Ok(field.map_inner_edges(replace)? || changed)
            }),
            Self::Object(fields) => fields.values_mut().try_fold(false, |changed, field| {
                Ok(field.map_inner_edges(replace)? || changed)
            }),
            _ => Ok(false),
        }
    }

    pub fn node_id(&self) -> Option<NodeId> {
        match self {
            Self::Definition(id) | Self::Reference(id) => Some(*id),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    class: NodeClass,
    fields: Shared<IndexMap<String, Field>>,
}

#[derive(Clone, Debug)]
enum NodeClass {
    Circuit,
    Declaration,
    Operator(OperatorKind),
    Inner(InnerKind),
}

impl NodeClass {
    fn decode(id: NodeId, class: std::borrow::Cow<'_, str>) -> Result<Self, Error> {
        Ok(match id {
            NodeId::Inner(_) => Self::Inner(InnerKind::decode(class)),
            NodeId::Outer(_) => match class.as_ref() {
                "DBSPCircuit" => Self::Circuit,
                "DBSPDeclaration" => Self::Declaration,
                name => Self::Operator(
                    OperatorKind::from_wire(name)
                        .ok_or_else(|| Error(format!("unknown operator class {name} at {id:?}")))?,
                ),
            },
        })
    }
}

impl Node {
    fn new(class: NodeClass, fields: IndexMap<String, Field>) -> Self {
        Self {
            class,
            fields: Shared(Arc::new(fields)),
        }
    }

    pub fn class(&self) -> &str {
        match &self.class {
            NodeClass::Circuit => "DBSPCircuit",
            NodeClass::Declaration => "DBSPDeclaration",
            NodeClass::Operator(kind) => kind.wire_name(),
            NodeClass::Inner(kind) => kind.wire_name(),
        }
    }

    pub fn inner_kind(&self) -> Option<&InnerKind> {
        match &self.class {
            NodeClass::Inner(kind) => Some(kind),
            _ => None,
        }
    }

    pub fn operator_kind(&self) -> Option<OperatorKind> {
        match self.class {
            NodeClass::Operator(kind) => Some(kind),
            _ => None,
        }
    }

    /// Global and local Merkle hashes coexist in the ordered annotation list.
    pub fn operator_hash(&self, global: bool) -> Result<Option<&str>, Error> {
        mir::annotations(self)?
            .iter()
            .find(|annotation| {
                annotation["class"] == "OperatorHash" && annotation["global"] == global
            })
            .map(|annotation| {
                annotation["hash"]
                    .as_str()
                    .ok_or_else(|| Error::new("missing operator hash"))
            })
            .transpose()
    }

    /// SQL spelling of a table or view name, including the case of quoted identifiers.
    pub fn relation_name(&self, field: &str) -> Result<&str, Error> {
        mir::identifier(self.field(field))
    }

    /// Input order is significant, including repeated uses of the same port.
    pub fn inputs(&self) -> impl Iterator<Item = OutputPort> + '_ {
        let ports = match self.field("inputs") {
            Some(Field::Array(ports)) if self.operator_kind().is_some() => ports.as_slice(),
            _ => &[],
        };
        ports
            .iter()
            .map(|port| wire::output(port).expect("validated operator input"))
    }

    pub fn edge(&self, name: &str) -> Result<NodeId, Error> {
        self.field(name)
            .and_then(Field::node_id)
            .ok_or_else(|| Error(format!("{}.{} requires a node", self.class(), name)))
    }

    pub fn edges(&self, name: &str) -> Result<Vec<NodeId>, Error> {
        let Some(Field::Array(fields)) = self.field(name) else {
            return Err(Error(format!(
                "{}.{} requires a node list",
                self.class(),
                name
            )));
        };
        fields
            .iter()
            .map(|f| {
                f.node_id()
                    .ok_or_else(|| Error(format!("invalid edge in {}.{}", self.class(), name)))
            })
            .collect()
    }

    pub fn data(&self, name: &str) -> Option<&Value> {
        let Field::Data(value) = self.field(name)? else {
            return None;
        };
        Some(value)
    }

    pub fn fields(&self) -> impl Iterator<Item = (&str, &Field)> {
        self.fields
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OutputPort {
    pub operator: NodeId,
    pub number: usize,
}

impl OutputPort {
    pub(crate) fn as_field(self) -> Field {
        Field::Object(
            [
                ("operator".into(), Field::Reference(self.operator)),
                ("outputNumber".into(), Field::Data(self.number.into())),
            ]
            .into(),
        )
    }

    pub fn from_field(field: &Field) -> Option<Self> {
        wire::output(field)
    }
}

/// When to visit a nested operator relative to the operators in its body.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum BodyOrder {
    Preorder,
    Postorder,
}

/// Passes fork the arena when they edit their first node. Unmodified nodes
/// continue to share the ordered fields of the previous snapshot.
#[derive(Clone, Debug, PartialEq)]
struct Shared<T>(Arc<T>);

impl<T> Deref for Shared<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: Clone> DerefMut for Shared<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        Arc::make_mut(&mut self.0)
    }
}

#[derive(Clone, Debug)]
pub struct Circuit {
    // A live optimizer request can refer to relation snapshots held by its JVM caller.
    live_relations: bool,
    root: NodeId,
    nodes: Shared<BTreeMap<NodeId, Node>>,
}

impl Circuit {
    /// Decode a raw DBSPCircuit. Observation and checkpoint envelopes carry
    /// additional compiler state that belongs to their respective callers.
    pub fn from_json(value: Value) -> Result<Self, Error> {
        wire::decode(value)
    }

    /// Decode additional expression roots written in this circuit's identity space.
    /// A root can refer to the snapshot or to another expression in the same batch.
    pub fn with_inner_roots(
        self,
        expressions: impl IntoIterator<Item = Value>,
    ) -> Result<(Self, Vec<NodeId>), Error> {
        let mut extra = BTreeMap::new();
        let mut roots = Vec::new();
        for expression in expressions {
            let root = wire::read(expression, false, &mut extra, 0)?
                .node_id()
                .ok_or_else(|| Error::new("expected a serialized inner node"))?;
            roots.push(root);
        }
        self.attach_inner_roots(extra, roots)
    }

    pub fn to_json(&self) -> Value {
        wire::encode_node(self.root, &self.nodes)
    }

    /// Compare node identities, definition sites and all properties to an input
    /// graph without allocating a second JSON tree for an unchanged pass.
    pub fn matches_json(&self, wire: &Value) -> bool {
        wire::matches_node(self.root, wire, &self.nodes)
    }

    /// The wire graph to publish after an optimization. Retain live operator
    /// identities when the pass returned its input or rebuilt identical fields.
    pub fn changed_graph(&self, previous: &Self) -> Option<Value> {
        if self.root == previous.root && Arc::ptr_eq(&self.nodes.0, &previous.nodes.0) {
            return None;
        }
        let wire = self.to_json();
        (!previous.matches_json(&wire)).then_some(wire)
    }

    pub fn root_id(&self) -> NodeId {
        self.root
    }

    pub fn root(&self) -> &Node {
        &self.nodes[&self.root]
    }

    pub fn node(&self, id: NodeId) -> Option<&Node> {
        self.nodes.get(&id)
    }

    /// Operators in storage order within each circuit. Only body containment
    /// determines the traversal, independent of input edges and allocation IDs.
    pub fn operator_order(&self, order: BodyOrder) -> Result<Vec<NodeId>, Error> {
        let mut stack = vec![(self.root, false)];
        let mut seen = std::collections::BTreeSet::new();
        let mut result = Vec::new();
        while let Some((id, finished)) = stack.pop() {
            if finished {
                result.push(id);
                continue;
            }
            if !seen.insert(id) {
                return Err(Error(format!(
                    "operator {id:?} occurs in multiple body slots"
                )));
            }
            if id != self.root {
                match order {
                    BodyOrder::Preorder => result.push(id),
                    BodyOrder::Postorder => stack.push((id, true)),
                }
            }
            if let Some(Field::Array(body)) = self.nodes[&id].field("allOperators") {
                stack.extend(
                    body.iter()
                        .rev()
                        .map(|op| (op.node_id().expect("validated body"), false)),
                );
            }
        }
        Ok(result)
    }

    /// Mutate only the annotations of an existing operator.
    pub fn annotations_mut(&mut self, id: NodeId) -> Result<&mut Vec<Value>, Error> {
        let node = self
            .nodes
            .get_mut(&id)
            .filter(|n| n.operator_kind().is_some())
            .ok_or_else(|| Error(format!("expected an operator at {id:?}")))?;
        match node.fields.get_mut("annotations") {
            Some(Field::Data(Value::Array(annotations))) => Ok(annotations),
            _ => Err(Error(format!("expected an annotation array on {id:?}"))),
        }
    }

    /// Top-level operators in circuit order. Nested bodies have their own order.
    pub fn operators(&self) -> impl Iterator<Item = (NodeId, &Node)> {
        let Some(Field::Array(operators)) = self.root().field("allOperators") else {
            unreachable!("decoder validated the root operator list")
        };
        operators.iter().map(|operator| {
            let id = operator.node_id().expect("validated operator id");
            (id, &self.nodes[&id])
        })
    }
}
