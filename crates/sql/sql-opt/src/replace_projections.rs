//! Insert a shared projection after a producer and rebind readers whose input narrowed.
use crate::rebuild::{input_type, rebuild_input_closures};
use crate::statistics::type_width;
use crate::stream_rewrites::body_members;
use feldera_sql_ir::{
    Circuit, CircuitEditor, Error, Field, NodeId, OperatorKind as Op, OutputPort, TypeId, TypeKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionClosures {
    outputs: BTreeMap<u64, Value>,
    inputs: BTreeMap<u64, Value>,
    origin: Option<Value>,
}

fn supports_projection(kind: Op) -> bool {
    matches!(
        kind,
        Op::Integrate
            | Op::Differentiate
            | Op::StreamJoin
            | Op::StreamJoinIndex
            | Op::SourceMultiset
            | Op::SourceMap
            | Op::SourceLazyMap
            | Op::Filter
            | Op::Deindex
            | Op::Map
            | Op::MapIndex
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
enum ProjectionMap {
    Map,
    MapIndex,
}

impl ProjectionMap {
    fn reader(kind: Op) -> Option<Self> {
        match kind {
            Op::Map => Some(Self::Map),
            Op::MapIndex => Some(Self::MapIndex),
            _ => None,
        }
    }

    fn producer(indexed: bool) -> Self {
        if indexed { Self::MapIndex } else { Self::Map }
    }

    fn operator(self) -> Op {
        match self {
            Self::Map => Op::Map,
            Self::MapIndex => Op::MapIndex,
        }
    }
}

/// A reader replacement takes precedence when its rewired input has a
/// different type. Otherwise a producer projection follows the original node.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionEdits {
    reader_if_narrowed: Option<ProjectionMap>,
    after_producer: Option<ProjectionMap>,
}

pub fn projection_edits(
    kind: Op,
    has_reader_closure: bool,
    indexed_output: Option<bool>,
) -> Option<ProjectionEdits> {
    let reader_if_narrowed = ProjectionMap::reader(kind).filter(|_| has_reader_closure);
    let after_producer = indexed_output
        .filter(|_| supports_projection(kind))
        .map(ProjectionMap::producer);
    (reader_if_narrowed.is_some() || after_producer.is_some()).then_some(ProjectionEdits {
        reader_if_narrowed,
        after_producer,
    })
}

pub(crate) fn projected_type(
    edit: &mut CircuitEditor,
    closure: NodeId,
    kind: Op,
) -> Result<TypeId, Error> {
    let body = edit.node(closure)?.edge("body")?;
    let result = edit.expression_type(edit.expression(body)?)?;
    if kind == Op::MapIndex {
        edit.indexed_zset_from_pair(result)
    } else {
        edit.zset_type(result)
    }
}

struct Replacements {
    outputs: HashMap<NodeId, NodeId>,
    inputs: HashMap<NodeId, NodeId>,
    narrowed: HashMap<NodeId, OutputPort>,
    widths: BTreeMap<NodeId, usize>,
    origin: Value,
}

impl Replacements {
    fn reader(
        &mut self,
        edit: &mut CircuitEditor,
        id: NodeId,
        kind: Op,
        before: OutputPort,
        after: OutputPort,
    ) -> Result<bool, Error> {
        let Some(&function) = self.inputs.get(&id) else {
            return Ok(false);
        };
        if ProjectionMap::reader(kind).is_none() || after == before {
            return Ok(false);
        }
        let ty = input_type(edit, after)?;
        if edit.same_type(input_type(edit, before)?, ty)? {
            return Ok(false);
        }
        let size = type_width(edit.circuit(), ty.into(), &mut self.widths, 0)?;
        let output = projected_type(edit, function, kind)?;
        edit.set_operator_field(id, "function", Field::Reference(function))?;
        edit.set_operator_field(id, "outputType", Field::Reference(output.into()))?;
        edit.clear_annotations(id)?;
        edit.annotate(id, json!({"class":"IsProjection","outputSize":size}))?;
        Ok(true)
    }

    fn producer(
        &mut self,
        edit: &mut CircuitEditor,
        body: NodeId,
        id: NodeId,
        following: Option<NodeId>,
    ) -> Result<(), Error> {
        let Some(&function) = self.outputs.get(&id) else {
            return Ok(());
        };
        let ty = edit.node(id)?.edge("outputType")?;
        let size = type_width(edit.circuit(), ty, &mut self.widths, 0)?;
        let result = edit.node(function)?.edge("body")?;
        let result = edit.expression_type(edit.expression(result)?)?;
        let kind =
            ProjectionMap::producer(edit.type_kind(result)? == TypeKind::RawTuple).operator();
        let output = projected_type(edit, function, kind)?;
        // Java decodes the output type first. The closure can share its tuple type.
        let mut fields = vec![
            ("outputType", Field::Reference(output.into())),
            ("function", Field::Reference(function)),
        ];
        if edit.node(id)?.field("provenance").is_some() {
            let origin_id = self
                .origin
                .get("id")
                .or_else(|| self.origin.get("ref"))
                .and_then(Value::as_u64)
                .ok_or_else(|| Error::new("missing shared projection origin"))?;
            fields.push((
                "provenance",
                Field::Data(std::mem::replace(
                    &mut self.origin,
                    json!({"ref":origin_id}),
                )),
            ));
        }
        let port = OutputPort {
            operator: id,
            number: 0,
        };
        // The inserted projection has an empty relational origin, with no inherited source position.
        let projection =
            edit.append_operator(body, edit.circuit().root_id(), kind, &[port], fields)?;
        if let Some(following) = following {
            edit.place_before(body, projection, following)?;
        }
        edit.annotate(
            projection,
            json!({"class":"IsProjection","outputSize":size}),
        )?;
        self.narrowed.insert(
            id,
            OutputPort {
                operator: projection,
                number: 0,
            },
        );
        Ok(())
    }
}

pub fn replace_common_projections(
    circuit: Circuit,
    state: ProjectionClosures,
    next: &mut u64,
) -> Result<Circuit, Error> {
    if state.outputs.is_empty() {
        return Ok(circuit);
    }
    let (producers, output_closures): (Vec<_>, Vec<_>) = state.outputs.into_iter().unzip();
    let (readers, input_closures): (Vec<_>, Vec<_>) = state.inputs.into_iter().unzip();
    let (circuit, roots) =
        circuit.with_inner_roots(output_closures.into_iter().chain(input_closures))?;
    let (outputs, inputs) = roots.split_at(producers.len());
    let mut state = Replacements {
        outputs: producers
            .into_iter()
            .map(NodeId::Outer)
            .zip(outputs.iter().copied())
            .collect(),
        inputs: readers
            .into_iter()
            .map(NodeId::Outer)
            .zip(inputs.iter().copied())
            .collect(),
        origin: state.origin.unwrap_or(Value::Null),
        narrowed: HashMap::new(),
        widths: BTreeMap::new(),
    };
    let members = body_members(&circuit)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut iter = members.into_iter().peekable();
    while let Some((body, id)) = iter.next() {
        let node = edit.node(id)?;
        let kind = node
            .operator_kind()
            .ok_or_else(|| Error::new("expected a projection site operator"))?;
        let original: Vec<_> = node.inputs().collect();
        let mapped: Vec<_> = original
            .iter()
            .map(|port| *state.narrowed.get(&port.operator).unwrap_or(port))
            .collect();
        if mapped != original {
            edit.rewire_inputs(id, &mapped)?;
            if let ([before], [after]) = (&original[..], &mapped[..]) {
                if state.reader(&mut edit, id, kind, *before, *after)? {
                    continue;
                }
            }
        }
        if supports_projection(kind) {
            let following = iter
                .peek()
                .filter(|(parent, _)| *parent == body)
                .map(|(_, next)| *next);
            state.producer(&mut edit, body, id, following)?;
        }
    }
    rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retain_both_edits_when_a_reader_is_also_a_producer() {
        let edits = projection_edits(Op::Map, true, Some(true)).unwrap();
        assert_eq!(edits.reader_if_narrowed, Some(ProjectionMap::Map));
        assert_eq!(edits.after_producer, Some(ProjectionMap::MapIndex));

        let edits = projection_edits(Op::Deindex, true, Some(false)).unwrap();
        assert_eq!(edits.reader_if_narrowed, None);
        assert_eq!(edits.after_producer, Some(ProjectionMap::Map));
        assert!(projection_edits(Op::Constant, true, Some(false)).is_none());
    }
}
