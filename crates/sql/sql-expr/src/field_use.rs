//! Follow values through a projection to identify the fields of its input row.
//! A tuple preserves the dependency of each component. Computing a scalar
//! consumes the dependencies of every operand, even if the result is projected.
use crate::lexical_scope::{References, Resolution, lexical_references};
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionKind as Expr, Field, InnerKind, Node, NodeId, TypeKind,
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
    parameters: HashMap<NodeId, usize>,
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
    /// Some expressions visit their operands without publishing a symbolic
    /// value. A statement can discard them, but a parent cannot read them.
    fn operation(&self, id: NodeId, bindings: &References) -> Result<Option<ProjectionOp>, Error> {
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
        let op = match kind {
            Expr::VariablePath => {
                let parameter = bindings
                    .get(&id)
                    .and_then(|declaration| self.parameters.get(declaration))
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
            Expr::Block => {
                return Ok(node
                    .field("lastExpression")
                    .and_then(Field::node_id)
                    .map(Op::Forward));
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
            | Expr::Assignment
            | Expr::DirectComparator
            | Expr::FieldComparator
            | Expr::NoComparator => Op::Compute(vec![]),
            kind if kind.wire_name().ends_with("Literal") => Op::Compute(vec![]),
            // The field-use visitor assigns values only to the operations above.
            // Other expressions can evaluate operands without defining a result.
            _ => return Ok(None),
        };
        Ok(Some(op))
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
    let bindings = crate::lexical_scope::resolve(circuit, closure)?;
    analysis
        .analyze(body, &bindings)?
        .ok_or_else(|| Error::new("cannot determine the fields read by the projection"))
}

/// Nested closures contribute no field paths, but the compiler resolves their
/// variables before interpreting the outer reader. On a binding conflict the
/// live visitor first copies shared expressions and then resolves that tree.
pub fn live_field_dependencies(
    circuit: &Circuit,
    function: NodeId,
) -> Result<Option<Vec<BTreeSet<Vec<usize>>>>, Error> {
    match lexical_references(circuit, function)? {
        Resolution::Bound(bindings) => {
            return bound_parameter_field_paths(circuit, function, &bindings);
        }
        Resolution::Free { .. } => return Ok(None),
        Resolution::Conflict { .. } => {}
    }

    let mut edit = CircuitEditor::new(circuit.clone());
    let tree = crate::ensure_expression_tree(&mut edit, function)?;
    // Deep-copying RETURN, for example, retains its argument. Two uses can
    // still share a variable with different declarations after the copy.
    live_parameter_field_paths(edit.circuit(), tree)
}

/// Check every evaluated operand, including discarded statement results.
/// A fused rewriter needs unique declarations on the original reader.
pub fn live_parameter_field_paths(
    circuit: &Circuit,
    closure: NodeId,
) -> Result<Option<Vec<BTreeSet<Vec<usize>>>>, Error> {
    let Resolution::Bound(bindings) = lexical_references(circuit, closure)? else {
        return Ok(None);
    };
    bound_parameter_field_paths(circuit, closure, &bindings)
}

/// Analyze a reader whose variable occurrences already have unique declarations.
fn bound_parameter_field_paths(
    circuit: &Circuit,
    closure: NodeId,
    bindings: &References,
) -> Result<Option<Vec<BTreeSet<Vec<usize>>>>, Error> {
    let Ok((mut analysis, body)) = RowProjection::for_closure(circuit, closure) else {
        // The caller can interpret parameters outside the supported row shapes.
        return Ok(None);
    };
    analysis.analyze(body, bindings)
}

impl RowProjection<'_> {
    /// Evaluate every expression once. A block's value and the fields read by its
    /// statements stay separate so a later selection can still project its value.
    fn analyze(
        &mut self,
        body: NodeId,
        bindings: &References,
    ) -> Result<Option<Vec<BTreeSet<Vec<usize>>>>, Error> {
        let circuit = self.circuit;
        let mut pending = vec![(body, false)];
        let mut statements = Vec::new();
        let mut active = HashSet::new();
        let mut without_value = HashSet::new();
        while let Some((id, finish)) = pending.pop() {
            if self.known.contains_key(&id) || without_value.contains(&id) {
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
                if *kind == Expr::Assignment {
                    let left = self.known.get(&node.edge("left")?);
                    let right = self.known.get(&node.edge("right")?);
                    if left.is_none() || left != right {
                        // Java diagnoses an assignment that changes the symbolic value of its target.
                        return Ok(None);
                    }
                }
                active.remove(&id);
                let op = match self.operation(id, bindings) {
                    Ok(None) => {
                        without_value.insert(id);
                        continue;
                    }
                    Ok(Some(op)) if op.operands().iter().all(|id| self.known.contains_key(id)) => {
                        op
                    }
                    _ => return Ok(None),
                };
                let Ok(value) = self.apply(op) else {
                    return Ok(None);
                };
                self.known.insert(id, value);
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
                    if !visit_block_operands(circuit, node, |id, is_statement| {
                        children.push(id);
                        if is_statement {
                            statements.push(id);
                        }
                    })? {
                        return Ok(None);
                    }
                }
                _ => {
                    for (_, field) in node.fields() {
                        field.collect_inner_edges(&mut children);
                    }
                }
            }
            pending.extend(children.into_iter().rev().map(|child| (child, false)));
        }
        let Some(result) = self.known.get(&body) else {
            return Ok(None);
        };
        let mut uses = vec![result];
        self.collect_statement_reads(statements, &mut uses)?;
        Ok(Some(self.paths(uses)))
    }
}

/// Visit block operands in the same order as the field-use interpreter. Only expression statements contribute
/// independent reads when the block itself is used as a value.
fn visit_block_operands(
    circuit: &Circuit,
    block: &Node,
    mut visit: impl FnMut(NodeId, bool),
) -> Result<bool, Error> {
    for id in block.edges("contents")? {
        let statement = circuit.node(id).expect("validated block statement");
        let expression = match statement.class() {
            "DBSPExpressionStatement" | "DBSPStaticItem" | "DBSPComparatorItem" => {
                Some(statement.edge("expression")?)
            }
            "DBSPFunctionItem" => {
                let function = circuit
                    .node(statement.edge("function")?)
                    .expect("validated function item");
                function.field("body").and_then(Field::node_id)
            }
            "DBSPComment" | "DBSPStructItem" => None,
            _ => return Ok(false),
        };
        if let Some(expression) = expression {
            visit(expression, statement.class() == "DBSPExpressionStatement");
        }
    }
    if let Some(tail) = block.field("lastExpression").and_then(Field::node_id) {
        visit(tail, false);
    }
    Ok(true)
}

impl<'a> RowProjection<'a> {
    fn for_closure(circuit: &'a Circuit, closure: NodeId) -> Result<(Self, NodeId), Error> {
        let node = circuit
            .node(closure)
            .ok_or_else(|| Error::new("missing input projection"))?;

        let params = node.edges("parameters")?;
        let mut parameters = HashMap::with_capacity(params.len());
        let mut names = HashSet::with_capacity(params.len());
        let mut shapes = Vec::with_capacity(params.len());
        for (i, id) in params.into_iter().enumerate() {
            let param = circuit.node(id).expect("validated parameter");
            let name = param
                .data("name")
                .and_then(|v| v.as_str())
                .ok_or_else(|| Error::new("missing input parameter name"))?;
            if !names.insert(name) {
                return Err(Error::new("duplicate map or join parameter name"));
            }
            parameters.insert(id, i);
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

    /// Record each complete access chain in an evaluated statement. A shared
    /// expression can be both the receiver of a field access and a standalone read.
    fn collect_statement_reads<'v>(
        &'v self,
        statements: Vec<NodeId>,
        uses: &mut Vec<&'v Use>,
    ) -> Result<(), Error> {
        let mut visited = HashSet::new();
        let mut pending: Vec<_> = statements.into_iter().map(|id| (id, false)).collect();
        while let Some((id, receiver)) = pending.pop() {
            if !visited.insert((id, receiver)) {
                continue;
            }
            let node = self.circuit.node(id).expect("validated statement operand");
            let Some(InnerKind::Expression(kind)) = node.inner_kind() else {
                continue;
            };
            if *kind == Expr::Closure {
                continue;
            }
            let access = matches!(
                kind,
                Expr::VariablePath
                    | Expr::Field
                    | Expr::Deref
                    | Expr::Borrow
                    | Expr::Clone
                    | Expr::Unwrap
                    | Expr::CustomOrdField
                    | Expr::IsNull
            );
            if access && !receiver {
                uses.push(&self.known[&id]);
            }
            if *kind == Expr::Block {
                // An enclosing expression statement also covers the expressions
                // inside item declarations in its block.
                visit_block_operands(self.circuit, node, |id, _| pending.push((id, false)))?;
            } else {
                let mut children = Vec::new();
                for (name, field) in node.fields() {
                    if name != "type" {
                        field.collect_inner_edges(&mut children);
                    }
                }
                pending.extend(children.into_iter().map(|child| (child, access)));
            }
        }
        Ok(())
    }

    fn paths<'v>(&'v self, mut pending: Vec<&'v Use>) -> Vec<BTreeSet<Vec<usize>>> {
        let mut paths = vec![BTreeSet::new(); self.shapes.len()];
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
    fn shared_statement_reads_survive_projection_of_the_block_result() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit", "id":0, "declarations":[], "allOperators":[]
        }))?;
        let site = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let flag = edit.bool_type(site, false)?;
        let pair = edit.tuple_type(site, false, [flag.into(), flag.into()])?;
        let row = edit.tuple_type(site, false, [pair.into(), flag.into(), flag.into()])?;
        let reference = edit.reference_type(row)?;
        let input = edit.named_variable(reference, "input")?;
        let value = edit.dereference(input)?;
        let shared = edit.tuple_field(value, 0)?;
        let leaf = edit.tuple_field(shared, 1)?;
        // The same node occurs on its own and as the receiver of a narrower access.
        let reads = edit.tuple_expr(site, false, &[shared, leaf])?;
        let statement = edit.inner_at(
            site,
            InnerKind::Other("DBSPExpressionStatement".into()),
            [("expression", Field::Reference(reads.into()))],
        )?;
        let block = edit.make_expr(
            site,
            Expr::Block,
            row,
            [
                ("contents", Field::Array(vec![Field::Reference(statement)])),
                ("lastExpression", Field::Reference(value.into())),
            ],
        )?;
        let tail = edit.tuple_field(block, 2)?;
        let closure = edit.closure_from_variables(tail, &[input])?;
        let paths = BTreeSet::from([vec![0], vec![0, 1], vec![2]]);
        assert_eq!(input_field_paths(edit.circuit(), closure.into())?, paths);
        assert_eq!(
            live_parameter_field_paths(edit.circuit(), closure.into())?,
            Some(vec![paths])
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
