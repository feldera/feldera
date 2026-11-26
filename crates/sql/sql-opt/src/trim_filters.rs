//! Project columns before a single-reader filter. Both the predicate and its
//! reader must be able to run on the narrower stream element.
use crate::rebuild::ProjectionNames;
use crate::row_projection::RowProjection;
use crate::stream_rewrites::{append_map, body_members, reader_counts};
use feldera_sql_expr::{
    input_field_paths,
    lexical_scope::{inspect_references, BindingDiagnostic},
};
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
    let mut edit = CircuitEditor::new(circuit.clone());
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
        let trimmed = trim_filter_fields(
            &circuit,
            &mut edit,
            function,
            predicate,
            &mut names,
            |closure| input_field_paths(&circuit, closure).map(Some),
        )?;
        let trimmed = match trimmed {
            FilterTrim::Narrowed(trimmed) => trimmed,
            FilterTrim::Unchanged => continue,
            FilterTrim::Binding(issue) => {
                return Err(Error::new(format!("invalid filter binding: {issue:?}")));
            }
            FilterTrim::InspectFields(_) => {
                return Err(Error::new("missing detached filter dependencies"));
            }
        };
        let body = parents[&id];
        let [pre, _, post] = trimmed.append(&mut edit, body, [filter, id], kind, input)?;
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

pub struct TrimmedFilter {
    pre: ExpressionId,
    predicate: ExpressionId,
    post: ExpressionId,
}

impl TrimmedFilter {
    /// The projection and filter inherit the filter site. The final reader keeps its own.
    pub fn append(
        self,
        edit: &mut CircuitEditor,
        body: NodeId,
        [filter, reader]: [NodeId; 2],
        reader_kind: Op,
        input: OutputPort,
    ) -> Result<[NodeId; 3], Error> {
        let (pre, stream) = append_map(edit, body, filter, Op::Map, input, self.pre)?;
        let narrowed = edit.append_operator(
            body,
            filter,
            Op::Filter,
            &[output(pre)],
            [
                ("outputType", Field::Reference(stream.into())),
                ("function", Field::Reference(self.predicate.into())),
            ],
        )?;
        let (post, _) = append_map(edit, body, reader, reader_kind, output(narrowed), self.post)?;
        Ok([pre, narrowed, post])
    }
}

pub enum FilterTrim<'a> {
    Unchanged,
    InspectFields(NodeId),
    Binding(BindingDiagnostic<'a>),
    Narrowed(TrimmedFilter),
}

/// A reader using every column never inspects its predicate. When both can be
/// narrowed, resolve each original closure at the point where it is rewritten.
pub fn trim_filter_fields<'a>(
    original: &'a Circuit,
    edit: &mut CircuitEditor,
    reader: NodeId,
    predicate: NodeId,
    next: &mut u64,
    mut fields: impl FnMut(NodeId) -> Result<Option<BTreeSet<Vec<usize>>>, Error>,
) -> Result<FilterTrim<'a>, Error> {
    let Some(mut used) = fields(reader)? else {
        return Ok(FilterTrim::InspectFields(reader));
    };
    if !RowProjection::new(edit, reader, &used)?.has_unused_fields() {
        return Ok(FilterTrim::Unchanged);
    }
    let Some(predicate_fields) = fields(predicate)? else {
        return Ok(FilterTrim::InspectFields(predicate));
    };
    let reader_uses_input = !used.is_empty();
    used.extend(predicate_fields);
    let layout = RowProjection::new(edit, reader, &used)?;
    if !layout.has_unused_fields() {
        return Ok(FilterTrim::Unchanged);
    }
    if let Some(issue) = inspect_references(original, predicate, false)?.diagnostic {
        return Ok(FilterTrim::Binding(issue));
    }
    let predicate = layout.rebind(edit, predicate)?;
    let pre = layout.project(edit, next, reader_uses_input)?;
    if let Some(issue) = inspect_references(original, reader, false)?.diagnostic {
        return Ok(FilterTrim::Binding(issue));
    }
    let post = layout.rebind(edit, reader)?;
    Ok(FilterTrim::Narrowed(TrimmedFilter {
        pre,
        predicate,
        post,
    }))
}
