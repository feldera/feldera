//! Intern expression shapes so comparisons of shared subexpressions visit a
//! node once per lexical scope. Bound names compare by parameter position.

use feldera_sql_ir::{Circuit, ExpressionKind as Expr, Field, InnerKind, Node, NodeId};
use std::collections::{HashMap, HashSet};

type ShapeId = usize;

#[derive(Hash, Eq, PartialEq)]
enum Shape {
    Identity(NodeId),
    Shuffle(u64),
    Bound(usize),
    Scalar(String),
    Array(Vec<ShapeId>),
    Object(Vec<(String, ShapeId)>),
    Node(String, Vec<(String, ShapeId)>),
}

#[derive(Default)]
pub(crate) struct Terms {
    shuffles: std::collections::BTreeMap<NodeId, u64>,
    visiting: HashSet<NodeId>,
    shapes: HashMap<Shape, ShapeId>,
    cache: HashMap<(NodeId, Vec<String>), ShapeId>,
}

impl Terms {
    pub(crate) fn with_shuffles(shuffles: &std::collections::BTreeMap<NodeId, u64>) -> Self {
        Self {
            shuffles: shuffles.clone(),
            ..Self::default()
        }
    }

    fn intern(&mut self, shape: Shape) -> ShapeId {
        let next = self.shapes.len();
        *self.shapes.entry(shape).or_insert(next)
    }

    pub(crate) fn field(&mut self, circuit: &Circuit, field: &Field, scope: &[String]) -> ShapeId {
        let shape = match field {
            Field::Definition(id) | Field::Reference(id) => return self.node(circuit, *id, scope),
            Field::Data(value) => Shape::Scalar(value.to_string()),
            Field::Array(fields) => Shape::Array(
                fields
                    .iter()
                    .map(|f| self.field(circuit, f, scope))
                    .collect(),
            ),
            Field::Object(fields) => {
                let mut items: Vec<_> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), self.field(circuit, v, scope)))
                    .collect();
                items.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                Shape::Object(items)
            }
        };
        self.intern(shape)
    }

    fn node(&mut self, circuit: &Circuit, id: NodeId, scope: &[String]) -> ShapeId {
        let key = (id, scope.to_vec());
        if let Some(&shape) = self.cache.get(&key) {
            return shape;
        }
        let identity = self.intern(Shape::Identity(id));
        if !self.visiting.insert(id) {
            return identity;
        }
        let node = circuit.node(id).expect("validated edge");
        let shape = match node.inner_kind() {
            Some(InnerKind::Expression(Expr::Flatmap)) => {
                self.shuffles.get(&id).copied().map(|shuffle| {
                    let mut fields =
                        vec![("shuffle".to_owned(), self.intern(Shape::Shuffle(shuffle)))];
                    for name in [
                        "collectionExpression",
                        "passthroughIndexes",
                        "type",
                        "postProcess",
                    ] {
                        if let Some(field) = node.field(name) {
                            fields.push((
                                name.to_owned(),
                                self.field(
                                    circuit,
                                    field,
                                    if name == "postProcess" { &[] } else { scope },
                                ),
                            ));
                        }
                    }
                    Shape::Node(node.class().into(), fields)
                })
            }
            Some(InnerKind::Expression(Expr::Closure)) => self.closure(circuit, node, scope),
            Some(InnerKind::Expression(Expr::VariablePath)) => string(node, "variable")
                .and_then(|name| scope.iter().rposition(|s| s == name))
                .map(Shape::Bound),
            Some(InnerKind::AggregateList) => {
                let row = node
                    .field("rowVar")
                    .and_then(Field::node_id)
                    .and_then(|id| circuit.node(id));
                row.and_then(|row| {
                    let name = string(row, "variable")?;
                    let fields = self.field(circuit, node.field("aggregates")?, &[name.into()]);
                    Some(Shape::Node(
                        node.class().into(),
                        vec![("aggregates".into(), fields)],
                    ))
                })
            }
            Some(InnerKind::Expression(Expr::Block)) => self.block(circuit, node, scope),
            Some(InnerKind::Expression(Expr::Let | Expr::For)) => {
                self.binding(circuit, node, scope)
            }
            Some(InnerKind::Expression(Expr::Static)) => None,
            _ => {
                let mut fields: Vec<_> = node
                    .fields()
                    .filter(|(name, _)| {
                        !matches!(
                            *name,
                            "position" | "sourceEmpty" | "singleton" | "originalStruct"
                        )
                    })
                    .map(|(k, v)| (k.to_owned(), self.field(circuit, v, scope)))
                    .collect();
                fields.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                Some(Shape::Node(node.class().into(), fields))
            }
        };
        let result = shape.map_or(identity, |shape| self.intern(shape));
        self.visiting.remove(&id);
        self.cache.insert(key, result);
        result
    }

    fn block(&mut self, circuit: &Circuit, node: &Node, scope: &[String]) -> Option<Shape> {
        let mut names = scope.to_vec();
        let mut fields = Vec::new();
        for id in node.edges("contents").ok()? {
            let statement = circuit.node(id)?;
            match statement.class() {
                "DBSPComment" => continue,
                "DBSPLetStatement" => {
                    for key in ["initializer", "type", "mutable"] {
                        if let Some(part) = statement.field(key) {
                            fields.push((key.into(), self.field(circuit, part, &names)));
                        }
                    }
                    names.push(string(statement, "variable")?.into());
                }
                _ => fields.push(("statement".into(), self.node(circuit, id, &names))),
            }
        }
        if let Some(last) = node.field("lastExpression") {
            fields.push(("result".into(), self.field(circuit, last, &names)));
        }
        Some(Shape::Node(node.class().into(), fields))
    }

    fn binding(&mut self, circuit: &Circuit, node: &Node, scope: &[String]) -> Option<Shape> {
        let (name, init, body) = if node.class() == "DBSPForExpression" {
            (string(node, "variable")?, "iterated", "block")
        } else {
            (
                string(circuit.node(node.edge("variable").ok()?)?, "variable")?,
                "initializer",
                "consumer",
            )
        };
        let mut names = scope.to_vec();
        names.push(name.into());
        Some(Shape::Node(
            node.class().into(),
            vec![
                (init.into(), self.field(circuit, node.field(init)?, scope)),
                (body.into(), self.field(circuit, node.field(body)?, &names)),
            ],
        ))
    }

    fn closure(&mut self, circuit: &Circuit, node: &Node, scope: &[String]) -> Option<Shape> {
        let Field::Array(parameters) = node.field("parameters")? else {
            return None;
        };
        let mut names = scope.to_vec();
        let mut fields = Vec::new();
        for parameter in parameters {
            let parameter = circuit.node(parameter.node_id()?)?;
            names.push(string(parameter, "name")?.to_owned());
            fields.push((
                "parameter".to_owned(),
                self.field(circuit, parameter.field("type")?, scope),
            ));
        }
        fields.push((
            "body".into(),
            self.field(circuit, node.field("body")?, &names),
        ));
        Some(Shape::Node(node.class().into(), fields))
    }
}

fn string<'a>(node: &'a Node, field: &str) -> Option<&'a str> {
    let Field::Data(data) = node.field(field)? else {
        return None;
    };
    data.as_str()
}
