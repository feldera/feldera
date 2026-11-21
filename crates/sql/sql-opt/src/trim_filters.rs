//! Project columns before a single-reader filter. Both the predicate and its
//! reader must be able to run on the narrower stream element.
use crate::rebuild::ProjectionNames;
use crate::row_projection::RowProjection;
use crate::stream_rewrites::{append_map, body_members, reader_counts};
use feldera_sql_expr::input_field_paths;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op,
    OutputPort,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn output(operator: NodeId) -> OutputPort {
    OutputPort {
        operator,
        number: 0,
    }
}

pub fn trim_filters(
    circuit: Circuit,
    candidates: BTreeSet<u64>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if candidates.is_empty() {
        return Ok(circuit);
    }
    let members = body_members(&circuit)?;
    let candidates = single_reader_filters(&circuit, &members, candidates)?;
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
        let input = source
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing predicate input"))?;
        let predicate = source.edge("function")?;
        let Some([pre, predicate, post]) =
            trim_filter_closures(&mut edit, function, predicate, &mut names)?
        else {
            continue;
        };
        let body = parents[&id];
        let (pre, stream) = append_map(&mut edit, body, filter, Op::Map, input, pre)?;
        let narrowed = edit.append_operator(
            body,
            filter,
            Op::Filter,
            &[output(pre)],
            [
                ("outputType", Field::Reference(stream.into())),
                ("function", Field::Reference(predicate.into())),
            ],
        )?;
        let (post, _) = append_map(&mut edit, body, id, kind, output(narrowed), post)?;
        edit.place_before(body, pre, id)?;
        replacements.insert(id, output(post));
        projections.replaced(id);
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok(circuit)
}

/// The live caller supplies only the topology until a filter has a single reader.
pub fn trim_filter_readers(
    circuit: &Circuit,
    candidates: BTreeSet<u64>,
) -> Result<BTreeSet<NodeId>, Error> {
    single_reader_filters(circuit, &body_members(circuit)?, candidates)
}

fn single_reader_filters(
    circuit: &Circuit,
    members: &[(NodeId, NodeId)],
    candidates: BTreeSet<u64>,
) -> Result<BTreeSet<NodeId>, Error> {
    let counts = reader_counts(circuit, members);
    let mut selected = BTreeSet::new();
    for id in candidates.into_iter().map(NodeId::Outer) {
        let reader = circuit
            .node(id)
            .ok_or_else(|| Error::new("unknown filter reader"))?;
        let input = reader
            .inputs()
            .next()
            .ok_or_else(|| Error::new("missing filter input"))?;
        let filter = circuit.node(input.operator).expect("validated input");
        if filter.operator_kind() == Some(Op::Filter) && counts.get(&input.operator) == Some(&1) {
            selected.insert(id);
        }
    }
    Ok(selected)
}

/// Return the pre-projection, narrower predicate and reader closure in operator order.
pub fn trim_filter_closures(
    edit: &mut CircuitEditor,
    reader: NodeId,
    predicate: NodeId,
    next: &mut u64,
) -> Result<Option<[ExpressionId; 3]>, Error> {
    let mut used = input_field_paths(edit.circuit(), reader)?;
    if !RowProjection::new(edit, reader, &used)?.has_unused_fields() {
        return Ok(None);
    }
    let reader_uses_input = !used.is_empty();
    used.extend(input_field_paths(edit.circuit(), predicate)?);
    let layout = RowProjection::new(edit, reader, &used)?;
    if !layout.has_unused_fields() {
        return Ok(None);
    }
    let predicate = layout.rebind(edit, predicate)?;
    let pre = layout.project(edit, next, reader_uses_input)?;
    let post = layout.rebind(edit, reader)?;
    Ok(Some([pre, predicate, post]))
}
