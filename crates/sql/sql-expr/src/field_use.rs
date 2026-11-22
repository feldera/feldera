//! Follow values through a projection to identify the fields of its input row.
//! A tuple preserves the dependency of each component. Computing a scalar
//! consumes the dependencies of every operand, even if the result is projected.
use feldera_sql_ir::{
    Circuit, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use std::collections::{BTreeSet, HashMap, HashSet};

/// A field belongs to the parameter that introduced it, including after
/// tuple construction, field selection and scalar computations.
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct ParameterField {
    parameter: usize,
    indices: Vec<usize>,
}

type Dependencies = BTreeSet<ParameterField>;

#[derive(Clone)]
enum Use {
    Field(ParameterField),
    Borrowed(ParameterField),
    Tuple(Vec<Use>),
    Computed(Dependencies),
}

impl Use {
    fn collect(self, paths: &mut Dependencies) {
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
    Pair,
}

impl ParameterShape {
    fn binding(self, parameter: usize) -> Use {
        let borrowed = |indices| Use::Borrowed(ParameterField { parameter, indices });
        match self {
            Self::Pair => Use::Tuple(vec![borrowed(vec![0]), borrowed(vec![1])]),
            Self::Borrowed { .. } => borrowed(vec![]),
        }
    }

    fn is_row_null_test(self, path: &[usize]) -> bool {
        match self {
            Self::Pair => path.len() == 1,
            Self::Borrowed { tuple } => tuple && path.is_empty(),
        }
    }
}

struct RowProjection<'a> {
    circuit: &'a Circuit,
    parameters: HashMap<&'a str, usize>,
    shapes: Vec<ParameterShape>,
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
                let parameter = node
                    .data("variable")
                    .and_then(|name| name.as_str())
                    .and_then(|name| self.parameters.get(name))
                    .copied()
                    .ok_or_else(|| Error::new("unbound variable in an input projection"))?;
                self.shapes[parameter].binding(parameter)
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
                        path.indices.push(index);
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
                Use::Field(path) if self.shapes[path.parameter].is_row_null_test(&path.indices) => {
                    Use::Computed(Dependencies::new())
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
            | Expr::NoComparator => Use::Computed(Dependencies::new()),
            kind if kind.wire_name().ends_with("Literal") => Use::Computed(Dependencies::new()),
            _ => {
                return Err(Error::new(format!(
                    "unsupported input projection expression {}",
                    node.class()
                )));
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
        let mut paths = Dependencies::new();
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
    let [paths] = parameter_field_paths(circuit, closure)?
        .try_into()
        .map_err(|_| Error::new("expected one input row parameter"))?;
    Ok(paths)
}

/// Analyze all parameters together so a join's key and each value keep their
/// own field numbering when reads meet in a tuple or scalar expression.
pub fn parameter_field_paths(
    circuit: &Circuit,
    closure: NodeId,
) -> Result<Vec<BTreeSet<Vec<usize>>>, Error> {
    let (mut analysis, body) = RowProjection::for_closure(circuit, closure)?;
    let value = analysis.value(body, 0)?;
    Ok(analysis.paths(value))
}

/// Check all operands of a live closure before accepting its dependencies. A
/// block with statements or an assignment needs the reference visitor's handling
/// of effects and diagnostics. A nested function has its own parameter scope.
pub fn live_parameter_field_paths(
    circuit: &Circuit,
    closure: NodeId,
) -> Result<Option<Vec<BTreeSet<Vec<usize>>>>, Error> {
    let Ok((mut analysis, body)) = RowProjection::for_closure(circuit, closure) else {
        // The caller can interpret parameters outside the supported row shapes.
        return Ok(None);
    };
    let mut pending = vec![(body, false)];
    let mut active = HashSet::new();
    while let Some((id, finish)) = pending.pop() {
        if analysis.known.contains_key(&id) {
            continue;
        }
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing field-use operand"))?;
        let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
            continue;
        };
        if finish {
            // Every operand has already been evaluated in this scope. A live
            // caller handles unsupported expressions at their original SQL site.
            if analysis.value(id, 0).is_err() {
                return Ok(None);
            }
            active.remove(&id);
            continue;
        }
        match kind {
            Expr::Block
                if !node.edges("contents")?.is_empty()
                    || node
                        .field("lastExpression")
                        .and_then(Field::node_id)
                        .is_none() =>
            {
                return Ok(None);
            }
            Expr::Let | Expr::Assignment | Expr::For => return Ok(None),
            _ => {}
        }
        if !active.insert(id) {
            return Err(Error::new("cycle in field-use expression"));
        }
        pending.push((id, true));
        if *kind != Expr::Closure {
            let mut children = Vec::new();
            for (_, field) in node.fields() {
                field.collect_inner_edges(&mut children);
            }
            pending.extend(children.into_iter().rev().map(|child| (child, false)));
        }
    }
    let value = analysis
        .known
        .remove(&body)
        .ok_or_else(|| Error::new("expected field-use expression"))?;
    Ok(Some(analysis.paths(value)))
}

impl<'a> RowProjection<'a> {
    fn for_closure(circuit: &'a Circuit, closure: NodeId) -> Result<(Self, NodeId), Error> {
        let node = circuit
            .node(closure)
            .ok_or_else(|| Error::new("missing input projection"))?;

        let params = node.edges("parameters")?;
        let mut parameters = HashMap::with_capacity(params.len());
        let mut shapes = Vec::with_capacity(params.len());
        for (i, id) in params.into_iter().enumerate() {
            let param = circuit.node(id).expect("validated parameter");
            let name = param
                .data("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("missing input parameter name"))?;
            if parameters.insert(name, i).is_some() {
                return Err(Error::new("duplicate map or join parameter name"));
            }
            let ty = circuit
                .node(param.edge("type")?)
                .expect("validated parameter type");
            let shape = match ty.inner_kind() {
                Some(InnerKind::Type(TypeKind::RawTuple)) => {
                    // Indexed closures receive (&key, &value). Aggregate updates can
                    // receive the accumulator pair by value. Both expose two fields.
                    if ty.edges("tupFields")?.len() != 2 {
                        return Err(Error::new("expected a two-field parameter"));
                    }
                    ParameterShape::Pair
                }
                Some(InnerKind::Type(TypeKind::Ref)) => ParameterShape::Borrowed {
                    tuple: matches!(
                        circuit.node(ty.edge("type")?).and_then(Node::inner_kind),
                        Some(InnerKind::Type(TypeKind::Tuple))
                    ),
                },
                // By-value parameters, including TopK ranks, are used as a whole.
                Some(InnerKind::Type(_)) => ParameterShape::Borrowed { tuple: false },
                _ => return Err(Error::new("missing field-use parameter type")),
            };
            shapes.push(shape);
        }

        Ok((
            Self {
                circuit,
                parameters,
                shapes,
                known: HashMap::new(),
            },
            node.edge("body")?,
        ))
    }

    fn paths(&self, value: Use) -> Vec<BTreeSet<Vec<usize>>> {
        let mut paths = vec![BTreeSet::new(); self.shapes.len()];
        let mut uses = Dependencies::new();
        value.collect(&mut uses);
        for field in uses {
            paths[field.parameter].insert(field.indices);
        }
        paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::CircuitEditor;
    use serde_json::json;

    #[test]
    fn live_field_reads_follow_deep_scalar_expressions() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let flag = edit.bool_type(site, false)?;
        let row = edit.tuple_type(site, false, [flag.into(), flag.into()])?;
        let reference = edit.reference_type(row)?;
        let input = edit.named_variable(reference, "input")?;
        let row = edit.dereference(input)?;
        let mut read = edit.tuple_field(row, 1)?;
        for _ in 0..160 {
            read = edit.make_expr(
                site,
                Expr::Unary,
                flag,
                [
                    ("opcode", Field::Data("NOT".into())),
                    ("source", Field::Reference(read.into())),
                ],
            )?;
        }
        let closure = edit.closure_from_variables(read, &[input])?;
        assert_eq!(
            live_parameter_field_paths(edit.circuit(), closure.into())?,
            Some(vec![BTreeSet::from([vec![1]])])
        );
        Ok(())
    }

    #[test]
    fn an_aggregate_can_read_its_accumulator_pair_by_value() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let flag = edit.bool_type(site, false)?;
        let pair = edit.tuple_type(site, true, [flag.into(), flag.into()])?;
        let accumulator = edit.named_variable(pair, "accumulator")?;
        let read = edit.tuple_field(accumulator, 1)?;
        let closure = edit.closure_from_variables(read, &[accumulator])?;
        assert_eq!(
            live_parameter_field_paths(edit.circuit(), closure.into())?,
            Some(vec![BTreeSet::from([vec![1]])])
        );
        Ok(())
    }

    #[test]
    fn live_analysis_leaves_unused_statements_to_the_reference_visitor() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let flag = edit.bool_type(site, false)?;
        let ty = edit.tuple_type(site, false, [flag.into(), flag.into()])?;
        let reference = edit.reference_type(ty)?;
        let row = edit.named_variable(reference, "row")?;
        let value = edit.dereference(row)?;
        let first = edit.tuple_field(value, 0)?;
        let second = edit.tuple_field(value, 1)?;
        let unused = edit.let_statement("unused", first)?;
        for (statements, expected) in [
            (vec![], Some(vec![BTreeSet::from([vec![1]])])),
            (vec![Field::Reference(unused)], None),
        ] {
            let block = edit.make_expr(
                site,
                Expr::Block,
                flag,
                [
                    ("contents", Field::Array(statements)),
                    ("lastExpression", Field::Reference(second.into())),
                ],
            )?;
            let closure = edit.closure_from_variables(block, &[row])?;
            assert_eq!(
                live_parameter_field_paths(edit.circuit(), closure.into())?,
                expected
            );
        }
        Ok(())
    }

    #[test]
    fn join_reads_distinguish_the_key_left_and_right_parameters() {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[],
            "type":{"class":"DBSPTypeInteger","id":1,"code":"INT32","mayBeNull":false}
        }))
        .unwrap();
        let mut edit = CircuitEditor::new(circuit);
        let integer = edit.type_id(NodeId::Inner(1)).unwrap();
        let row = edit
            .tuple_type(integer.into(), false, [integer.into(), integer.into()])
            .unwrap();
        let borrowed = edit.reference_type(row).unwrap();
        let key = edit.named_variable(borrowed, "key").unwrap();
        let left = edit.named_variable(borrowed, "left").unwrap();
        let right = edit.named_variable(borrowed, "right").unwrap();
        let lhs = edit.dereference(left).unwrap();
        let rhs = edit.dereference(right).unwrap();
        let a = edit.tuple_field(lhs, 1).unwrap();
        let b = edit.tuple_field(rhs, 0).unwrap();
        let joined = edit.tuple_expr(row.into(), false, &[a, b, a]).unwrap();
        let reader = edit
            .closure_from_variables(joined, &[key, left, right])
            .unwrap();
        assert_eq!(
            parameter_field_paths(edit.circuit(), reader.into()).unwrap(),
            [
                BTreeSet::new(),
                BTreeSet::from([vec![1]]),
                BTreeSet::from([vec![0]])
            ]
        );
    }
}
