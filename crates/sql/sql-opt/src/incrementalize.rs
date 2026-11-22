//! Convert snapshot sources and sinks into an incremental circuit before stream optimization.
use crate::OperatorInputs;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, OperatorKind as Op};
use std::collections::BTreeMap;

#[derive(Clone, Copy, serde::Serialize)]
pub enum BoundaryStep {
    DeltaSource,
    DeltaConstant,
    Original,
    Differentiate,
    Integrate,
    AnnotatedIntegral,
    Sink,
}

/// The first step consumes the mapped input of a sink or constructs a source.
/// Each following step consumes the preceding output at the same circuit site.
fn boundary(kind: Op) -> &'static [BoundaryStep] {
    use BoundaryStep::*;
    match kind {
        Op::SourceMultiset | Op::SourceMap => &[DeltaSource, Integrate],
        Op::Constant => &[DeltaConstant, AnnotatedIntegral],
        Op::Now => &[Original, Differentiate, Integrate],
        Op::Sink => &[Differentiate, Sink],
        _ => &[],
    }
}

pub fn incremental_boundaries(
    operators: &[OperatorInputs],
) -> BTreeMap<u64, &'static [BoundaryStep]> {
    operators
        .iter()
        .filter_map(|op| {
            let steps = boundary(op.kind);
            (!steps.is_empty()).then_some((op.id, steps))
        })
        .collect()
}

pub fn incrementalize(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    // Inserting a boundary never changes which operators were in the original body.
    let sites = crate::stream_rewrites::body_members(&circuit)?;

    let mut edit = CircuitEditor::new(circuit);
    edit.mark_incremental();
    for (body, id) in sites {
        let kind = edit
            .node(id)?
            .operator_kind()
            .ok_or_else(|| Error::new("expected a circuit operator"))?;
        match boundary(kind) {
            [] => {}
            [.., BoundaryStep::Sink] => edit.differentiate_sink(body, id)?,
            _ => edit.integrate_source(body, id)?,
        }
    }
    crate::rebuild::rebuild_input_closures(edit.finish_with_rewrites()?, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{BodyOrder, NodeId};
    use serde_json::json;

    #[test]
    fn clock_keeps_its_snapshot_before_the_new_integral_and_sink_delta() {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"incremental":false,"declarations":[],"allOperators":[
                {"class":"DBSPNowOperator","id":1,"annotations":[],"inputs":[],
                 "outputType":{"class":"DBSPTypeZSet","id":1,"elementType":{"class":"DBSPTypeTimestamp","id":2}}},
                {"class":"DBSPSinkOperator","id":2,"annotations":[{"class":"Recursive"}],
                 "inputs":[{"operator":{"node":1},"outputNumber":0}],"outputType":{"node":1}}
            ]
        })).unwrap();
        let mut next = 17;
        let result = incrementalize(circuit, &mut next).unwrap();
        let order = result.operator_order(BodyOrder::Preorder).unwrap();
        let kinds: Vec<_> = order
            .iter()
            .map(|&id| result.node(id).unwrap().operator_kind().unwrap())
            .collect();
        assert_eq!(
            kinds,
            [
                Op::Now,
                Op::Differentiate,
                Op::Integrate,
                Op::Differentiate,
                Op::Sink
            ]
        );
        for pair in order.windows(2) {
            let node = result.node(pair[1]).unwrap();
            assert_eq!(node.inputs().next().unwrap().operator, pair[0]);
            assert_eq!(node.edge("outputType").unwrap(), NodeId::Inner(1));
        }
        assert_eq!(result.to_json()["incremental"], true);
        assert_eq!(
            result.to_json()["allOperators"][4]["annotations"],
            json!([])
        );
        assert_eq!(next, 17);
    }
}
