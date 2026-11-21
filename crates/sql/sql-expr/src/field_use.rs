//! Follow values through a projection to identify the fields of its input row.
//! A tuple preserves the dependency of each component. Computing a scalar
//! consumes the dependencies of every operand, even if the result is projected.
use feldera_sql_ir::{
    Circuit, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use std::collections::{BTreeSet, HashMap};

type Paths = BTreeSet<Vec<usize>>;

#[derive(Clone)]
enum Use {
    Field(Vec<usize>),
    Borrowed(Vec<usize>),
    Tuple(Vec<Use>),
    Computed(Paths),
}

impl Use {
    fn collect(self, paths: &mut Paths) {
        match self {
            Self::Field(path) | Self::Borrowed(path) => {
                paths.insert(path);
            }
            Self::Tuple(fields) => fields.into_iter().for_each(|field| field.collect(paths)),
            Self::Computed(dependencies) => paths.extend(dependencies),
        }
    }
}

#[derive(Clone, Copy)]
enum ParameterShape {
    Borrowed { tuple: bool },
    IndexedPair,
}

impl ParameterShape {
    fn binding(self) -> Use {
        match self {
            Self::IndexedPair => Use::Tuple(vec![Use::Borrowed(vec![0]), Use::Borrowed(vec![1])]),
            Self::Borrowed { .. } => Use::Borrowed(vec![]),
        }
    }

    fn is_row_null_test(self, path: &[usize]) -> bool {
        match self {
            Self::IndexedPair => path.len() == 1,
            Self::Borrowed { tuple } => tuple && path.is_empty(),
        }
    }
}

struct RowProjection<'a> {
    circuit: &'a Circuit,
    parameter: &'a str,
    shape: ParameterShape,
    known: HashMap<NodeId, Use>,
}

impl RowProjection<'_> {
    fn value(&mut self, id: NodeId, depth: usize) -> Result<Use, Error> {
        if depth > 128 {
            return Err(Error::new(
                "projection exceeds the expression nesting limit",
            ));
        }
        if let Some(value) = self.known.get(&id) {
            return Ok(value.clone());
        }
        let node = self
            .circuit
            .node(id)
            .ok_or_else(|| Error::new("missing projection expression"))?;
        let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
            return Err(Error::new("expected a projection expression"));
        };
        let mut operand = |field| self.value(node.edge(field)?, depth + 1);
        let result = match kind {
            Expr::VariablePath => {
                if node.data("variable").and_then(|name| name.as_str()) != Some(self.parameter) {
                    return Err(Error::new("unbound variable in an input projection"));
                }
                self.shape.binding()
            }
            Expr::Borrow => match operand("expression")? {
                Use::Field(path) => Use::Borrowed(path),
                other => other,
            },
            Expr::Deref => match operand("expression")? {
                Use::Borrowed(path) => Use::Field(path),
                other => other,
            },
            Expr::Field | Expr::CustomOrdField => {
                let index = node
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("invalid projection field index"))?;
                match operand("expression")? {
                    Use::Field(mut path) => {
                        path.push(index);
                        Use::Field(path)
                    }
                    Use::Tuple(fields) => fields
                        .into_iter()
                        .nth(index)
                        .ok_or_else(|| Error::new("projection exceeds tuple width"))?,
                    other => other,
                }
            }
            Expr::Tuple | Expr::RawTuple if node.field("fields").is_some() => {
                let fields = node
                    .edges("fields")?
                    .into_iter()
                    .map(|id| self.value(id, depth + 1))
                    .collect::<Result<_, _>>()?;
                Use::Tuple(fields)
            }
            Expr::IsNull => match operand("expression")? {
                // Checking the presence of the entire row does not read its columns.
                Use::Field(path) if self.shape.is_row_null_test(&path) => {
                    Use::Computed(Paths::new())
                }
                other => other,
            },
            Expr::Cast | Expr::Unary | Expr::UnsignedUnwrap | Expr::UnsignedWrap => {
                operand("source")?
            }
            Expr::Unwrap | Expr::Some | Expr::Clone => operand("expression")?,
            Expr::Variant if node.field("value").and_then(Field::node_id).is_some() => {
                operand("value")?
            }
            Expr::WindowBound => operand("representation")?,
            Expr::Block if node.field("lastExpression").is_some() => operand("lastExpression")?,
            Expr::Apply => self.dependencies(node, &["arguments"], depth)?,
            Expr::ApplyMethod => self.dependencies(node, &["self", "arguments"], depth)?,
            Expr::Constructor => self.dependencies(node, &["function", "arguments"], depth)?,
            Expr::Binary | Expr::TimeAddSub | Expr::GeoPointConstructor => {
                self.dependencies(node, &["left", "right"], depth)?
            }
            Expr::ConditionalIncrement => {
                self.dependencies(node, &["condition", "left", "right"], depth)?
            }
            Expr::If => self.dependencies(node, &["condition", "positive", "negative"], depth)?,
            Expr::Map => self.dependencies(node, &["keys", "values"], depth)?,
            Expr::Array => self.dependencies(node, &["data"], depth)?,
            // A nested closure is a pure function value. Its parameters have their own scope.
            Expr::Closure
            | Expr::Static
            | Expr::Path
            | Expr::Fail
            | Expr::Variant
            | Expr::Tuple
            | Expr::RawTuple
            | Expr::Block
            | Expr::Assignment
            | Expr::AsymmetricFieldComparator
            | Expr::DirectComparator
            | Expr::EqualityComparator
            | Expr::FieldComparator
            | Expr::NoComparator => Use::Computed(Paths::new()),
            kind if kind.wire_name().ends_with("Literal") => Use::Computed(Paths::new()),
            _ => {
                return Err(Error::new(format!(
                    "unsupported input projection expression {}",
                    node.class()
                )))
            }
        };
        self.known.insert(id, result.clone());
        Ok(result)
    }

    fn dependencies(&mut self, node: &Node, fields: &[&str], depth: usize) -> Result<Use, Error> {
        let mut children = Vec::new();
        for &name in fields {
            if let Some(field) = node.field(name) {
                field.collect_inner_edges(&mut children);
            }
        }
        let mut paths = Paths::new();
        for child in children {
            self.value(child, depth + 1)?.collect(&mut paths);
        }
        Ok(Use::Computed(paths))
    }
}

/// Dependencies of a single-parameter closure over a borrowed row or key/value pair.
/// Each ordered path starts at the row. An empty path denotes the whole row.
pub fn input_field_paths(
    circuit: &Circuit,
    closure: NodeId,
) -> Result<BTreeSet<Vec<usize>>, Error> {
    let node = circuit
        .node(closure)
        .ok_or_else(|| Error::new("missing input projection"))?;
    let [parameter] = node
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("expected one input row parameter"))?;
    let param = circuit.node(parameter).expect("validated parameter");
    let name = param
        .data("name")
        .and_then(|value| value.as_str())
        .ok_or_else(|| Error::new("missing input parameter name"))?;
    let ty = circuit
        .node(param.edge("type")?)
        .expect("validated parameter type");
    let shape = match ty.inner_kind() {
        Some(InnerKind::Type(TypeKind::Ref)) => ParameterShape::Borrowed {
            tuple: matches!(
                circuit.node(ty.edge("type")?).and_then(Node::inner_kind),
                Some(InnerKind::Type(TypeKind::Tuple))
            ),
        },
        Some(InnerKind::Type(TypeKind::RawTuple)) => {
            let parts = ty.edges("tupFields")?;
            if parts.len() != 2
                || !parts.iter().all(|id| {
                    matches!(
                        circuit.node(*id).and_then(Node::inner_kind),
                        Some(InnerKind::Type(TypeKind::Ref))
                    )
                })
            {
                return Err(Error::new("expected a borrowed key and value"));
            }
            ParameterShape::IndexedPair
        }
        _ => return Err(Error::new("expected a borrowed row or indexed pair")),
    };
    let mut projection = RowProjection {
        circuit,
        parameter: name,
        shape,
        known: HashMap::new(),
    };
    let mut paths = Paths::new();
    projection.value(node.edge("body")?, 0)?.collect(&mut paths);
    Ok(paths)
}
