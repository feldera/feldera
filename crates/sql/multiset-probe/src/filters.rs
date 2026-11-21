//! Literal filter elimination on the flat circuit subset.

use crate::model::{Circuit, CollectionKind, Edit, FilterPredicate, OperatorKind, Port};
use crate::{Error, Result};

pub(crate) fn remove_constants(circuit: &Circuit, incremental: bool) -> Result<Vec<Edit>> {
    let mut streams = Vec::with_capacity(circuit.operators.len());
    let mut aliases = Vec::<Option<Port>>::with_capacity(circuit.operators.len());
    let mut edits = Vec::with_capacity(circuit.operators.len());
    let source_kind = if incremental {
        CollectionKind::Delta
    } else {
        CollectionKind::Snapshot
    };
    for operator in &circuit.operators {
        let input_kind = operator.inputs.first().and_then(|port| match port.output {
            0 => streams[port.operator.0],
            // The only second port in the supported subset emits filter errors.
            1 => Some(CollectionKind::Delta),
            _ => None,
        });
        let stream = match &operator.kind {
            OperatorKind::Source { .. } => Some(source_kind),
            OperatorKind::Now | OperatorKind::Integrate => Some(CollectionKind::Snapshot),
            OperatorKind::Constant { stream, .. } => Some(*stream),
            OperatorKind::InnerJoin(_, stream) => Some(*stream),
            OperatorKind::Differentiate => Some(CollectionKind::Delta),
            OperatorKind::SingleValue => None,
            _ => input_kind,
        };
        let inputs: Vec<_> = operator
            .inputs
            .iter()
            .map(|port| aliases[port.operator.0].unwrap_or(*port))
            .collect();
        let mut alias = None;
        let edit = match &operator.kind {
            OperatorKind::Filter(FilterPredicate::Literal(true)) => {
                alias = Some(inputs[0]);
                Edit::Bypass
            }
            OperatorKind::Filter(FilterPredicate::Literal(false)) => Edit::Empty(
                stream.ok_or_else(|| Error("constant filter has no collection input".into()))?,
            ),
            OperatorKind::Filter(FilterPredicate::Null) => {
                return Err(Error(
                    "filter predicate cannot be a NULL boolean literal".into(),
                ));
            }
            _ => Edit::Retain(inputs),
        };
        streams.push(stream);
        aliases.push(alias);
        edits.push(edit);
    }
    Ok(edits)
}
