use super::{fragment::Fragment, operator_number, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, NodeId, OperatorKind, OutputPort};
use serde::Deserialize;
use serde_json::{json, Value};
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    ports: Vec<usize>,
}

pub(super) fn delta(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let ports: Vec<_> = request
        .ports
        .into_iter()
        .enumerate()
        .map(|(i, number)| OutputPort {
            operator: NodeId::Outer(i as u64 + 1),
            number,
        })
        .collect();
    let (fragment, roots) = Fragment::with_inputs(
        request.site,
        ports,
        std::iter::repeat(OperatorKind::MapIndex),
        input,
    )?;
    let [function, output, operands @ ..] = roots.as_slice() else {
        return Err("missing star join function or output type".into());
    };
    let (collections, tail) = operands
        .split_at_checked(fragment.inputs.len())
        .ok_or("missing star join input type")?;
    let filter_map = match tail {
        [] => None,
        [filter] => Some((*filter, None)),
        [filter, map] => Some((*filter, Some(*map))),
        _ => return Err("unexpected operands after the star join filter and map".into()),
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let inputs = fragment
        .inputs
        .iter()
        .zip(collections)
        .map(|(&port, &ty)| edit.type_id(ty).map(|ty| (port, ty)))
        .collect::<Result<Vec<_>, _>>()?;
    let join = feldera_sql_opt::DeltaStarJoin {
        function: *function,
        output_type: edit.type_id(*output)?,
        filter_map,
    };
    let mut warnings = Vec::new();
    let graph = feldera_sql_opt::expand_star_delta(
        &mut edit,
        fragment.base.root_id(),
        join,
        &inputs,
        &mut next,
        &mut warnings,
    )?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["integrals"] = json!(graph
        .integrals
        .into_iter()
        .map(operator_number)
        .collect::<Vec<_>>());
    reply["terms"] = json!(graph
        .terms
        .into_iter()
        .map(|term| json!({
            "joins": term.joins.into_iter().map(operator_number).collect::<Vec<_>>(),
            "projection": operator_number(term.projection)
        }))
        .collect::<Vec<_>>());
    reply["sum"] = operator_number(graph.sum).into();
    if let Some(filter) = graph.filter {
        reply["filter"] = operator_number(filter).into();
    }
    if let Some(map) = graph.map {
        reply["map"] = operator_number(map).into();
    }
    reply["warnings"] = json!(super::pass_warnings(warnings));
    Ok(reply)
}
