//! Recognize key-preserving join trees in circuit order. Each candidate keeps
//! its binary join closure as well as the closure over all leaves of its tree.
use super::StarJoinTree;
use crate::{
    collapse_shared_chains::compose, consumer_graph::successors, identity::identity_closure,
    rebuild::input_type,
};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, InnerKind, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::{BTreeSet, HashMap};

struct Tree {
    operator: Option<NodeId>,
    key: TypeId,
    binary: ExpressionId,
    closure: ExpressionId,
    leaves: Vec<OutputPort>,
    children: [Option<usize>; 2],
}

fn reference_variable(
    edit: &mut CircuitEditor,
    ty: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let ty = edit.reference_type(ty)?;
    edit.variable(ty, next)
}

fn apply(
    edit: &mut CircuitEditor,
    function: ExpressionId,
    args: &[ExpressionId],
) -> Result<ExpressionId, Error> {
    let ty = edit.expression_type(function)?;
    let result = edit.node(ty.into())?.edge("resultType")?;
    let call = edit.empty_origin_node(
        function.into(),
        InnerKind::Expression(Expr::Apply),
        [
            ("type", Field::Reference(result)),
            ("function", Field::Reference(function.into())),
            (
                "arguments",
                Field::Array(args.iter().map(|&a| Field::Reference(a.into())).collect()),
            ),
        ],
    )?;
    edit.expression(call)
}

/// A child index may compute its output key from the joined rows. Project that key,
/// substitute placeholders for both value arguments, and test for the identity.
fn preserves_key(
    edit: &mut CircuitEditor,
    binary: ExpressionId,
    next: &mut u64,
) -> Result<bool, Error> {
    let body = edit.node(binary.into())?.edge("body")?;
    let ty = edit.expression_type(edit.expression(body)?)?;
    let value = reference_variable(edit, ty, next)?;
    let pair = edit.dereference(value)?;
    let key = edit.tuple_field(pair, 0)?;
    let projection = edit.closure_from_variables(key, &[value])?;
    let projected = compose(edit, binary, false, projection.into())?;
    let body = edit.node(projected.into())?.edge("body")?;
    let params = edit.node(projected.into())?.edges("parameters")?;
    let [key, left, right] = params.as_slice() else {
        return Err(Error::new(
            "expected binary join key and two value parameters",
        ));
    };
    let param = edit.node(*key)?;
    let name = param
        .data("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::new("missing join key parameter name"))?
        .to_owned();
    let key = edit.named_variable(edit.type_id(param.edge("type")?)?, &name)?;
    let mut placeholder = |param| {
        let ty = edit.node(param)?.edge("type")?;
        edit.empty_origin_node(
            param,
            InnerKind::Expression(Expr::NoExpression),
            [("type", Field::Reference(ty))],
        )
    };
    let args = [key.into(), placeholder(*left)?, placeholder(*right)?];
    let projected_key = reduce_closure_body(edit, body, &params, &args)?;
    let closure = edit.closure_from_variables(edit.expression(projected_key)?, &[key])?;
    identity_closure(edit, closure.into(), next)
}

struct Candidates {
    trees: Vec<Tree>,
    by_operator: HashMap<NodeId, usize>,
    fanout: HashMap<NodeId, usize>,
    maximal: BTreeSet<NodeId>,
}

impl Candidates {
    fn child(
        &self,
        edit: &mut CircuitEditor,
        input: OutputPort,
        key: TypeId,
        next: &mut u64,
    ) -> Result<Option<usize>, Error> {
        if self.fanout.get(&input.operator) != Some(&1) {
            return Ok(None);
        }
        let Some(&index) = self.by_operator.get(&input.operator) else {
            return Ok(None);
        };
        let tree = &self.trees[index];
        Ok(
            (edit.same_type(key, tree.key)? && preserves_key(edit, tree.binary, next)?)
                .then_some(index),
        )
    }

    fn tree(
        &self,
        edit: &mut CircuitEditor,
        binary: ExpressionId,
        ports: [OutputPort; 2],
        children: [Option<usize>; 2],
        next: &mut u64,
    ) -> Result<Tree, Error> {
        let stream = input_type(edit, ports[0])?;
        let key_type = edit.type_id(edit.node(stream.into())?.edge("keyType")?)?;
        let key = reference_variable(edit, key_type, next)?;
        let mut parameters = vec![key];
        let mut leaves = Vec::new();
        let mut arguments = vec![key];
        for (port, child) in ports.into_iter().zip(children) {
            let child = child.map(|i| &self.trees[i]);
            let inputs = child.map_or(std::slice::from_ref(&port), |c| c.leaves.as_slice());
            let mut child_arguments = vec![key];
            for &input in inputs {
                let stream = input_type(edit, input)?;
                let row = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
                let value = reference_variable(edit, row, next)?;
                leaves.push(input);
                parameters.push(value);
                child_arguments.push(value);
            }
            let value = if let Some(child) = child {
                let result = apply(edit, child.closure, &child_arguments)?;
                let value = edit.tuple_field(result, 1)?;
                let borrowed = edit.borrow_value(value)?;
                let reduced = reduce_closure_body(edit, borrowed.into(), &[], &[])?;
                edit.expression(reduced)?
            } else {
                child_arguments[1]
            };
            arguments.push(value);
        }
        let call = apply(edit, binary, &arguments)?;
        let closure = edit.closure_from_variables(call, &parameters)?;
        Ok(Tree {
            operator: None,
            key: key_type,
            binary,
            closure,
            leaves,
            children,
        })
    }

    fn visit(&mut self, edit: &mut CircuitEditor, id: NodeId, next: &mut u64) -> Result<(), Error> {
        let node = edit.node(id)?;
        let kind = node.operator_kind();
        let mut tree = match kind {
            Some(Op::StreamJoin | Op::StreamJoinIndex) => {
                let binary = edit.expression(node.edge("function")?)?;
                let ports: [OutputPort; 2] = node
                    .inputs()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| Error::new("snapshot join requires two inputs"))?;
                let stream = input_type(edit, ports[0])?;
                let key = edit.type_id(edit.node(stream.into())?.edge("keyType")?)?;
                let children = [
                    self.child(edit, ports[0], key, next)?,
                    self.child(edit, ports[1], key, next)?,
                ];
                for child in children.into_iter().flatten() {
                    if let Some(id) = self.trees[child].operator {
                        self.maximal.remove(&id);
                    }
                }
                self.maximal.insert(id);
                self.tree(edit, binary, ports, children, next)?
            }
            Some(Op::MapIndex) => {
                let input = node
                    .inputs()
                    .next()
                    .ok_or_else(|| Error::new("index map requires an input"))?
                    .operator;
                if self.fanout.get(&input) != Some(&1)
                    || edit.node(input)?.operator_kind() != Some(Op::StreamJoin)
                {
                    return Ok(());
                }
                let child = &self.trees[self.by_operator[&input]];
                let ports = edit
                    .node(input)?
                    .inputs()
                    .collect::<Vec<_>>()
                    .try_into()
                    .map_err(|_| Error::new("snapshot join requires two inputs"))?;
                let indexed = compose(edit, child.binary, false, node.edge("function")?)?;
                self.tree(edit, indexed, ports, child.children, next)?
            }
            _ => return Ok(()),
        };
        if kind != Some(Op::MapIndex) {
            tree.operator = Some(id);
        }
        self.by_operator.insert(id, self.trees.len());
        self.trees.push(tree);
        Ok(())
    }
}

/// For independent binary joins the analysis creates a parameter for the key
/// and each of the two values, then discards the closure. The live compiler
/// can advance its name counter without serializing those closures or types.
pub fn names_after_isolated_joins(circuit: &Circuit, next: u64) -> Result<Option<u64>, Error> {
    let members = circuit.root().edges("allOperators")?;
    let kind = |position: usize| {
        circuit
            .node(members[position])
            .expect("validated circuit member")
            .operator_kind()
    };
    let is_join = |kind| matches!(kind, Some(Op::StreamJoin | Op::StreamJoinIndex));
    let mut joins = (0..members.len()).filter(|&i| is_join(kind(i))).peekable();
    if joins.peek().is_none() {
        return Ok(Some(next));
    }

    let consumers = successors(circuit, circuit.root_id())?;
    let mut names = next;
    for join in joins {
        if let [(reader, _)] = consumers[join].as_slice() {
            let reader = kind(*reader);
            // A join can grow a tree or become an indexed child through a map.
            if is_join(reader)
                || (kind(join) == Some(Op::StreamJoin) && reader == Some(Op::MapIndex))
            {
                return Ok(None);
            }
        }
        names = names
            .checked_add(3)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
    }
    Ok(Some(names))
}

/// Analyze only the outer circuit. Recursive operators form opaque leaves.
pub fn find_join_trees(circuit: &Circuit, next: &mut u64) -> Result<Vec<StarJoinTree>, Error> {
    let ids = circuit.root().edges("allOperators")?;
    if !circuit.operators().any(|(_, n)| {
        matches!(
            n.operator_kind(),
            Some(Op::StreamJoin | Op::StreamJoinIndex)
        )
    }) {
        return Ok(vec![]);
    }
    let edges = successors(circuit, circuit.root_id())?;
    let mut candidates = Candidates {
        trees: Vec::new(),
        by_operator: HashMap::new(),
        maximal: BTreeSet::new(),
        fanout: ids
            .iter()
            .copied()
            .zip(edges.iter().map(Vec::len))
            .collect(),
    };
    let mut edit = CircuitEditor::new(circuit.clone());
    let mut names = *next;
    for id in ids {
        candidates.visit(&mut edit, id, &mut names)?;
    }
    let roots = candidates
        .maximal
        .iter()
        .filter_map(|id| {
            let tree = &candidates.trees[candidates.by_operator[id]];
            (tree.leaves.len() > 2).then_some((*id, tree))
        })
        .collect::<Vec<_>>();
    let closures = roots
        .iter()
        .map(|(_, tree)| tree.closure)
        .collect::<Vec<_>>();
    let encoded = edit.circuit().additional_expressions(circuit, &closures)?;
    let result = roots
        .into_iter()
        .zip(encoded)
        .map(|((root, tree), function)| {
            let number = |id| match id {
                NodeId::Outer(n) => n,
                NodeId::Inner(_) => unreachable!("operator id"),
            };
            StarJoinTree {
                root: number(root),
                inputs: tree
                    .leaves
                    .iter()
                    .map(|p| (number(p.operator), p.number))
                    .collect(),
                function,
            }
        })
        .collect();
    *next = names;
    Ok(result)
}
