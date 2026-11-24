//! Follow values through a projection to identify the fields of its input row.
//! A tuple preserves the dependency of each component. Computing a scalar
//! consumes the dependencies of every operand, even if the result is projected.
use feldera_sql_ir::{
    Circuit, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
};
use indexmap::IndexSet;
use std::collections::{BTreeSet, HashMap, HashSet};

/// A field belongs to the parameter that introduced it, including after
/// tuple construction, field selection and scalar computations.
#[derive(Clone, Eq, Hash, PartialEq)]
struct ParameterField {
    parameter: usize,
    indices: Vec<usize>,
}

/// Tuple elements and members of a union refer to distinct symbolic values in
/// the projection. Reusing a tuple therefore costs one index per occurrence.
#[derive(Clone, Eq, Hash, PartialEq)]
enum Use {
    Field(ParameterField),
    Borrowed(ParameterField),
    Tuple(Vec<usize>),
    Computed(BTreeSet<usize>),
}

#[derive(Clone, Copy)]
enum ParameterShape {
    Borrowed { tuple: bool },
    Pair,
}

impl ParameterShape {
    fn binding(self, parameter: usize, values: &mut IndexSet<Use>) -> Use {
        let borrowed = |indices| Use::Borrowed(ParameterField { parameter, indices });
        match self {
            Self::Pair => Use::Tuple(
                (0..2)
                    .map(|i| values.insert_full(borrowed(vec![i])).0)
                    .collect(),
            ),
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
    members: IndexSet<Use>,
    known: HashMap<NodeId, Use>,
}

/// Only these operands contribute to the result. For example, a block projects
/// its final expression and a nested closure introduces a separate scope.
enum ProjectionOp {
    Parameter(usize),
    Borrow(NodeId),
    Deref(NodeId),
    Select(NodeId, usize),
    Tuple(Vec<NodeId>),
    IsNull(NodeId),
    Forward(NodeId),
    Compute(Vec<NodeId>),
}

impl ProjectionOp {
    fn operands(&self) -> &[NodeId] {
        match self {
            Self::Parameter(_) => &[],
            Self::Borrow(id)
            | Self::Deref(id)
            | Self::Select(id, _)
            | Self::IsNull(id)
            | Self::Forward(id) => std::slice::from_ref(id),
            Self::Tuple(ids) | Self::Compute(ids) => ids,
        }
    }
}

impl RowProjection<'_> {
    fn operation(&self, id: NodeId) -> Result<ProjectionOp, Error> {
        use ProjectionOp as Op;
        let node = self
            .circuit
            .node(id)
            .ok_or_else(|| Error::new("missing projection expression"))?;
        let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
            return Err(Error::new("expected a projection expression"));
        };
        let compute = |names: &[&str]| {
            let mut children = Vec::new();
            for name in names {
                if let Some(field) = node.field(name) {
                    field.collect_inner_edges(&mut children);
                }
            }
            Op::Compute(children)
        };
        Ok(match kind {
            Expr::VariablePath => {
                let parameter = node
                    .data("variable")
                    .and_then(|name| name.as_str())
                    .and_then(|name| self.parameters.get(name))
                    .ok_or_else(|| Error::new("unbound variable in an input projection"))?;
                Op::Parameter(*parameter)
            }
            Expr::Borrow => Op::Borrow(node.edge("expression")?),
            Expr::Deref => Op::Deref(node.edge("expression")?),
            Expr::Field | Expr::CustomOrdField => {
                let index = node
                    .data("fieldNo")
                    .and_then(|n| n.as_u64())
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(|| Error::new("invalid projection field index"))?;
                Op::Select(node.edge("expression")?, index)
            }
            Expr::Tuple | Expr::RawTuple if node.field("fields").is_some() => {
                Op::Tuple(node.edges("fields")?)
            }
            Expr::IsNull => Op::IsNull(node.edge("expression")?),
            Expr::Cast | Expr::Unary | Expr::UnsignedUnwrap | Expr::UnsignedWrap => {
                Op::Forward(node.edge("source")?)
            }
            Expr::Unwrap | Expr::Some | Expr::Clone => Op::Forward(node.edge("expression")?),
            Expr::Variant if node.field("value").and_then(Field::node_id).is_some() => {
                Op::Forward(node.edge("value")?)
            }
            Expr::WindowBound => Op::Forward(node.edge("representation")?),
            Expr::Block if node.field("lastExpression").is_some() => {
                Op::Forward(node.edge("lastExpression")?)
            }
            Expr::Apply => compute(&["arguments"]),
            Expr::ApplyMethod => compute(&["self", "arguments"]),
            Expr::Constructor => compute(&["function", "arguments"]),
            Expr::Binary | Expr::TimeAddSub | Expr::GeoPointConstructor => {
                compute(&["left", "right"])
            }
            Expr::ConditionalIncrement => compute(&["condition", "left", "right"]),
            Expr::If => compute(&["condition", "positive", "negative"]),
            Expr::Map => compute(&["keys", "values"]),
            Expr::Array => compute(&["data"]),
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
            | Expr::NoComparator => Op::Compute(vec![]),
            kind if kind.wire_name().ends_with("Literal") => Op::Compute(vec![]),
            _ => {
                return Err(Error::new(format!(
                    "unsupported input projection expression {}",
                    node.class()
                )));
            }
        })
    }

    /// Evaluate shared operands once in dependency order, including expressions
    /// that span several batches of IR definitions.
    fn value(&mut self, root: NodeId) -> Result<Use, Error> {
        if let Some(value) = self.known.get(&root) {
            return Ok(value.clone());
        }
        let mut pending = vec![(root, self.operation(root)?, 0)];
        let mut active = HashSet::from([root]);
        while let Some((_, op, next)) = pending.last_mut() {
            if let Some(&child) = op.operands().get(*next) {
                *next += 1;
                if !self.known.contains_key(&child) {
                    if !active.insert(child) {
                        return Err(Error::new("cycle in field-use expression"));
                    }
                    pending.push((child, self.operation(child)?, 0));
                }
            } else {
                let (id, op, _) = pending.pop().expect("current operation");
                let value = self.apply(op)?;
                self.known.insert(id, value);
                active.remove(&id);
            }
        }
        Ok(self.known[&root].clone())
    }

    fn apply(&mut self, op: ProjectionOp) -> Result<Use, Error> {
        use ProjectionOp as Op;
        Ok(match op {
            Op::Parameter(parameter) => {
                self.shapes[parameter].binding(parameter, &mut self.members)
            }
            Op::Borrow(id) => match self.known[&id].clone() {
                Use::Field(path) => Use::Borrowed(path),
                other => other,
            },
            Op::Deref(id) => match self.known[&id].clone() {
                Use::Borrowed(path) => Use::Field(path),
                other => other,
            },
            Op::Select(id, index) => match self.known[&id].clone() {
                Use::Field(mut path) => {
                    path.indices.push(index);
                    Use::Field(path)
                }
                Use::Tuple(fields) => fields
                    .get(index)
                    .map(|&id| self.members[id].clone())
                    .ok_or_else(|| Error::new("projection exceeds tuple width"))?,
                other => other,
            },
            Op::IsNull(id) => match self.known[&id].clone() {
                Use::Field(path) if self.shapes[path.parameter].is_row_null_test(&path.indices) => {
                    Use::Computed(BTreeSet::new())
                }
                other => other,
            },
            Op::Forward(id) => self.known[&id].clone(),
            Op::Tuple(fields) => Use::Tuple(
                fields
                    .into_iter()
                    .map(|id| self.members.insert_full(self.known[&id].clone()).0)
                    .collect(),
            ),
            Op::Compute(children) => {
                // Assignment checks compare symbolic values. A tuple and its
                // fields can read the same paths but compare differently.
                let mut uses = BTreeSet::new();
                for child in children {
                    match &self.known[&child] {
                        Use::Computed(fields) => uses.extend(fields),
                        value => {
                            uses.insert(self.members.insert_full(value.clone()).0);
                        }
                    }
                }
                Use::Computed(uses)
            }
        })
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
    let value = analysis.value(body)?;
    Ok(analysis.paths(value))
}

/// Check every evaluated operand, including discarded statement results.
/// A nested function has its own parameter scope.
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
    let mut without_value = HashSet::new();
    while let Some((id, finish)) = pending.pop() {
        if analysis.known.contains_key(&id) || without_value.contains(&id) {
            continue;
        }
        let node = circuit
            .node(id)
            .ok_or_else(|| Error::new("missing field-use operand"))?;
        let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
            continue;
        };
        if finish {
            // The interpreter evaluates a loop or a statement-only block but
            // records no value for it. A discarded statement needs no value.
            if *kind == Expr::For
                || (*kind == Expr::Block
                    && node
                        .field("lastExpression")
                        .and_then(Field::node_id)
                        .is_none())
            {
                without_value.insert(id);
                active.remove(&id);
                continue;
            }
            // Every operand has already been evaluated in this scope. A live
            // caller handles unsupported expressions at their original SQL site.
            if *kind == Expr::Assignment {
                let left = analysis.known.get(&node.edge("left")?);
                let right = analysis.known.get(&node.edge("right")?);
                if left.is_none() || left != right {
                    // Java diagnoses an assignment that changes the symbolic value of its target.
                    return Ok(None);
                }
            }
            let op = match analysis.operation(id) {
                Ok(op)
                    if op
                        .operands()
                        .iter()
                        .all(|id| analysis.known.contains_key(id)) =>
                {
                    op
                }
                _ => return Ok(None),
            };
            let Ok(value) = analysis.apply(op) else {
                return Ok(None);
            };
            analysis.known.insert(id, value);
            active.remove(&id);
            continue;
        }
        if !active.insert(id) {
            return Err(Error::new("cycle in field-use expression"));
        }
        pending.push((id, true));
        let mut children = Vec::new();
        match kind {
            Expr::Closure => {}
            Expr::Let => return Ok(None),
            Expr::Block => {
                for statement in node.edges("contents")? {
                    let statement = circuit.node(statement).expect("validated block statement");
                    match statement.class() {
                        "DBSPExpressionStatement" => children.push(statement.edge("expression")?),
                        "DBSPComment" => {}
                        _ => return Ok(None),
                    }
                }
                children.extend(node.field("lastExpression").and_then(Field::node_id));
            }
            _ => {
                for (_, field) in node.fields() {
                    field.collect_inner_edges(&mut children);
                }
            }
        }
        pending.extend(children.into_iter().rev().map(|child| (child, false)));
    }
    Ok(analysis
        .known
        .remove(&body)
        .map(|value| analysis.paths(value)))
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
                members: IndexSet::new(),
                known: HashMap::new(),
            },
            node.edge("body")?,
        ))
    }

    fn paths(&self, value: Use) -> Vec<BTreeSet<Vec<usize>>> {
        let mut paths = vec![BTreeSet::new(); self.shapes.len()];
        let mut pending = vec![&value];
        let mut visited = HashSet::new();
        while let Some(next) = pending.pop() {
            let mut enqueue = |&id: &usize| {
                if visited.insert(id) {
                    pending.push(&self.members[id]);
                }
            };
            match next {
                Use::Field(path) | Use::Borrowed(path) => {
                    paths[path.parameter].insert(path.indices.clone());
                }
                Use::Tuple(fields) => fields.iter().for_each(&mut enqueue),
                Use::Computed(fields) => fields.iter().for_each(&mut enqueue),
            }
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
    fn repeated_tuple_members_share_their_symbolic_values() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let scalar = edit.bool_type(site, false)?;
        let row = edit.tuple_type(site, false, [scalar.into()])?;
        let reference = edit.reference_type(row)?;
        let input = edit.named_variable(reference, "input")?;
        let base = edit.dereference(input)?;
        let mut fields = edit.tuple_field(base, 0)?;
        for _ in 0..40 {
            fields = edit.tuple_expr(site, false, &[fields, fields])?;
        }
        let closure = edit.closure_from_variables(fields, &[input])?;
        assert_eq!(
            live_parameter_field_paths(edit.circuit(), closure.into())?,
            Some(vec![BTreeSet::from([vec![0]])])
        );
        Ok(())
    }

    #[test]
    fn field_reads_follow_deep_scalar_expressions() -> Result<(), Error> {
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
            input_field_paths(edit.circuit(), closure.into())?,
            BTreeSet::from([vec![1]])
        );
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
