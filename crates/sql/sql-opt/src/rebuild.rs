//! Rebuild the projection closures synthesized by stream-operator constructors.
use feldera_sql_ir::{
    Circuit, CircuitEditor, CircuitRewrite, Error, ExpressionId, ExpressionKind as Expr, Field,
    InnerKind, Node, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind,
};
use std::collections::{HashMap, HashSet};

/// Java allocates each constructor's binding during traversal. Rust rewires
/// the graph once at the end, using the names reserved at those positions.
#[derive(Default)]
pub(super) struct ProjectionNames {
    rebuilt: HashSet<NodeId>,
    reserved: HashMap<NodeId, u64>,
}

impl ProjectionNames {
    pub(super) fn visit(&mut self, id: NodeId, node: &Node, next: &mut u64) -> Result<(), Error> {
        if node
            .inputs()
            .any(|port| self.rebuilt.contains(&port.operator))
        {
            self.rebuilt.insert(id);
            if matches!(node.operator_kind(), Some(Op::Deindex | Op::Noop)) {
                self.reserved.insert(id, *next);
                *next = next
                    .checked_add(1)
                    .ok_or_else(|| Error::new("generated names exhausted"))?;
            }
        }
        if node.operator_kind() == Some(Op::Nested)
            && node
                .edges("allOperators")?
                .iter()
                .any(|child| self.rebuilt.contains(child))
        {
            self.rebuilt.insert(id);
        }
        Ok(())
    }

    pub(super) fn replaced(&mut self, id: NodeId) {
        self.rebuilt.insert(id);
    }

    pub(super) fn finish(mut self, rewrite: CircuitRewrite) -> Result<Circuit, Error> {
        let result = rebuild_projections(rewrite, |id| {
            self.reserved
                .remove(&id)
                .ok_or_else(|| Error::new("missing name for rebuilt projection"))
        })?;
        if !self.reserved.is_empty() {
            return Err(Error::new("reserved a name for an unchanged projection"));
        }
        Ok(result)
    }
}

fn link(id: impl Into<NodeId>) -> Field {
    Field::Reference(id.into())
}

pub(super) fn deindex_function(
    edit: &mut CircuitEditor,
    site: NodeId,
    key: TypeId,
    row: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let k = edit.reference_type(key)?;
    let v = edit.reference_type(row)?;
    let pair = edit.tuple_type(site, true, [k.into(), v.into()])?;
    let name = format!("t_{}", *next);
    *next = next
        .checked_add(1)
        .ok_or_else(|| Error::new("generated names exhausted"))?;
    let path = edit.make_expr(
        site,
        Expr::VariablePath,
        pair,
        [("variable", Field::Data(name.into()))],
    )?;
    let field = edit.empty_origin_node(
        site,
        InnerKind::Expression(Expr::Field),
        [
            ("expression", link(path)),
            ("type", link(v)),
            ("fieldNo", Field::Data(1.into())),
        ],
    )?;
    let deref = edit.dereference(edit.expression(field)?)?;
    let result = edit.clone_value(deref)?;
    let closure = edit.closure_from_variables(result, &[path])?;
    let node = edit.node(closure.into())?;
    let fields = ["type", "parameters", "body"]
        .map(|k| (k, node.field(k).expect("constructed closure").clone()));
    let id = edit.inner_at(site, InnerKind::Expression(Expr::Closure), fields)?;
    edit.expression(id)
}

pub(super) fn resolve_output(
    edit: &CircuitEditor,
    mut port: OutputPort,
) -> Result<OutputPort, Error> {
    let mut seen = std::collections::BTreeSet::new();
    while seen.insert(port) {
        let node = edit.node(port.operator)?;
        if node.operator_kind() != Some(Op::Nested) {
            return Ok(port);
        }
        let Some(Field::Array(outputs)) = node.field("internalOutputs") else {
            return Err(Error::new("nested input has no output list"));
        };
        port = outputs
            .get(port.number)
            .and_then(OutputPort::from_field)
            .ok_or_else(|| Error::new("missing nested output"))?;
    }
    Err(Error::new("cyclic nested output"))
}

pub(super) fn input_type(edit: &CircuitEditor, port: OutputPort) -> Result<TypeId, Error> {
    let port = resolve_output(edit, port)?;
    let field = match port.number {
        0 => "outputType",
        1 => "errorType",
        _ => return Err(Error::new("the output port has no stored stream type")),
    };
    edit.type_id(edit.node(port.operator)?.edge(field)?)
}

pub(super) fn noop_function(
    edit: &mut CircuitEditor,
    stream: TypeId,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let element = edit.type_id(edit.node(stream.into())?.edge("elementType")?)?;
    let value_ref = edit.reference_type(element)?;
    let (var, result) = match edit.type_kind(stream)? {
        TypeKind::ZSet => {
            let var = edit.variable(value_ref, next)?;
            let value = edit.dereference(var)?;
            (var, edit.clone_value(value)?)
        }
        TypeKind::IndexedZSet => {
            let key = edit.type_id(edit.node(stream.into())?.edge("keyType")?)?;
            let key_ref = edit.reference_type(key)?;
            let pair = edit.tuple_type(stream.into(), true, [key_ref.into(), value_ref.into()])?;
            let var = edit.variable(pair, next)?;
            let mut fields = Vec::with_capacity(2);
            for index in 0..2 {
                let field = edit.tuple_field(var, index)?;
                let value = edit.dereference(field)?;
                fields.push(edit.clone_value(value)?);
            }
            (var, edit.tuple_expr(stream.into(), true, &fields)?)
        }
        _ => return Err(Error::new("Noop requires a collection input")),
    };
    edit.closure_from_variables(result, &[var])
}

pub(super) fn rebuild_input_closures(
    rewrite: CircuitRewrite,
    next: &mut u64,
) -> Result<Circuit, Error> {
    let mut counter = *next;
    let circuit = rebuild_projections(rewrite, |_| {
        let name = counter;
        counter = counter
            .checked_add(1)
            .ok_or_else(|| Error::new("generated names exhausted"))?;
        Ok(name)
    })?;
    *next = counter;
    Ok(circuit)
}

/// Graph reconstruction knows which input types changed. The pass that visited
/// the operators supplies the generated variable for each reconstructed closure.
pub(super) fn rebuild_projections(
    rewrite: CircuitRewrite,
    mut name_for: impl FnMut(NodeId) -> Result<u64, Error>,
) -> Result<Circuit, Error> {
    let affected: Vec<_> = rewrite
        .rebuilt_inputs
        .into_iter()
        .filter_map(|id| {
            let kind = rewrite.circuit.node(id)?.operator_kind()?;
            matches!(kind, Op::Deindex | Op::Noop).then_some((id, kind))
        })
        .collect();
    if affected.is_empty() {
        return Ok(rewrite.circuit);
    }
    let mut edit = CircuitEditor::new(rewrite.circuit);
    for (id, kind) in affected {
        let mut counter = name_for(id)?;
        let node = edit.node(id)?;
        let input = node
            .inputs()
            .next()
            .ok_or_else(|| Error::new("projection requires one input"))?;
        let function = if kind == Op::Noop {
            // withInputs already inherited the exact wrapper, including implicit error outputs.
            let stream = edit.type_id(node.edge("outputType")?)?;
            noop_function(&mut edit, stream, &mut counter)?
        } else {
            let site = node.edge("function")?;
            let stream = input_type(&edit, input)?;
            let ty = edit.node(stream.into())?;
            let key = edit.type_id(ty.edge("keyType")?)?;
            let value = edit.type_id(ty.edge("elementType")?)?;
            let closure = deindex_function(&mut edit, site, key, value, &mut counter)?;
            let output = edit.zset_type(value)?;
            edit.set_operator_field(id, "outputType", link(output))?;
            closure
        };
        edit.set_operator_field(id, "function", link(function))?;
    }
    edit.finish()
}
