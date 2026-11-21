//! Project columns before a single-reader filter. Both the predicate and its
//! reader must be able to run on the narrower stream element.
use crate::rebuild::ProjectionNames;
use crate::row_projection::RowProjection;
use crate::stream_rewrites::{body_members, reader_counts};
use feldera_sql_expr::input_field_paths;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort, TypeId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

fn add_map(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    kind: Op,
    input: OutputPort,
    function: ExpressionId,
) -> Result<(NodeId, TypeId), Error> {
    let result = edit.expression(edit.node(function.into())?.edge("body")?)?;
    let ty = edit.expression_type(result)?;
    let stream = match kind {
        Op::Map => edit.zset_type(ty)?,
        Op::MapIndex => edit.indexed_zset_from_pair(ty)?,
        _ => {
            return Err(Error::new("filter projection requires a map reader"));
        }
    };
    let id = edit.append_operator(
        body,
        site,
        kind,
        &[input],
        [
            ("function", Field::Reference(function.into())),
            ("outputType", Field::Reference(stream.into())),
        ],
    )?;
    Ok((id, stream))
}

pub fn trim_filters(
    circuit: Circuit,
    candidates: BTreeSet<u64>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if candidates.is_empty() {
        return Ok(circuit);
    }
    let candidates: BTreeSet<_> = candidates.into_iter().map(NodeId::Outer).collect();
    let members = body_members(&circuit)?;
    let readers = reader_counts(&circuit, &members);
    let parents: HashMap<_, _> = members.into_iter().map(|(body, id)| (id, body)).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut names = *next;
    let mut projections = ProjectionNames::default();
    for id in order {
        let reader = edit.node(id)?;
        projections.visit(id, reader, &mut names)?;
        if !candidates.contains(&id) {
            continue;
        }
        let kind = reader
            .operator_kind()
            .ok_or_else(|| Error::new("missing reader kind"))?;
        let function = reader.edge("function")?;
        let filter = reader
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing filter input"))?
            .operator;
        let source = edit.node(filter)?;
        if source.operator_kind() != Some(Op::Filter) || readers.get(&filter) != Some(&1) {
            continue;
        }
        let input = source
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing predicate input"))?;
        let predicate = source.edge("function")?;
        let mut used = input_field_paths(edit.circuit(), function)?;
        if !RowProjection::new(&edit, function, &used)?.has_unused_fields() {
            continue;
        }
        let reader_uses_input = !used.is_empty();
        used.extend(input_field_paths(edit.circuit(), predicate)?);
        let layout = RowProjection::new(&edit, function, &used)?;
        if !layout.has_unused_fields() {
            continue;
        }
        let predicate = layout.rebind(&mut edit, predicate)?;
        let pre = layout.project(&mut edit, &mut names, reader_uses_input)?;
        let post = layout.rebind(&mut edit, function)?;
        let body = parents[&id];
        let (pre, stream) = add_map(&mut edit, body, filter, Op::Map, input, pre)?;
        let narrowed = edit.append_operator(
            body,
            filter,
            Op::Filter,
            &[output(pre)],
            [
                ("function", Field::Reference(predicate.into())),
                ("outputType", Field::Reference(stream.into())),
            ],
        )?;
        let (post, _) = add_map(&mut edit, body, id, kind, output(narrowed), post)?;
        edit.place_before(body, pre, id)?;
        replacements.insert(id, output(post));
        projections.replaced(id);
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(circuit)
}
