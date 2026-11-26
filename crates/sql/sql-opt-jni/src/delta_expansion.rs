use super::{fragment::Fragment, operator_number, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, OperatorKind, OutputPort};
use feldera_sql_opt::UnaryDelta;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    input_port: usize,
}

pub(super) fn unary(
    kind: OperatorKind,
    input: &[u8],
    state: &[u8],
    next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(request.site, [OperatorKind::SourceMultiset], input)?;
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let (&input_type, rest) = roots.split_first().ok_or("missing delta input type")?;
    let operation = match (kind, rest) {
        (OperatorKind::Positive | OperatorKind::Distinct, []) => UnaryDelta::Distinct {
            positive: kind == OperatorKind::Positive,
        },
        (OperatorKind::Aggregate, [output, function @ ..]) if function.len() <= 1 => {
            UnaryDelta::Aggregate {
                output_type: edit.type_id(*output)?,
                function: function
                    .first()
                    .map(|&id| edit.expression(id))
                    .transpose()?,
            }
        }
        _ => return Err("unexpected operands for unary delta expansion".into()),
    };
    let ty = edit.type_id(input_type)?;
    let delta = OutputPort {
        number: request.input_port,
        ..fragment.inputs[0]
    };
    let body = fragment.base.root_id();
    let graph = operation.expand(&mut edit, body, body, delta, ty)?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["integral"] = operator_number(graph.integral).into();
    reply["output"] = operator_number(graph.output).into();
    if let Some(aggregate) = graph.aggregate {
        reply["aggregate"] = operator_number(aggregate).into();
    }
    Ok(reply)
}

#[derive(Deserialize)]
struct BinaryRequest {
    #[serde(flatten)]
    site: ExpressionSite,
    form: feldera_sql_opt::JoinForm,
    balanced: bool,
    ports: [usize; 2],
}

pub(super) fn binary(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn Error>> {
    let request: BinaryRequest = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(request.site, [OperatorKind::MapIndex; 2], input)?;
    let [function, output, left, right, original_left, predicate @ ..] = roots.as_slice() else {
        return Err("missing binary join operands".into());
    };
    let filter = match predicate {
        [] => None,
        [predicate] => Some(*predicate),
        _ => return Err("too many predicates for a binary join".into()),
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let operation = feldera_sql_opt::DeltaJoin {
        form: request.form,
        balanced: request.balanced,
        function: *function,
        output: edit.type_id(*output)?,
        filter,
    };
    let mut ports = fragment.inputs;
    for (port, number) in ports.iter_mut().zip(request.ports) {
        port.number = number;
    }
    let inputs = [
        (ports[0], edit.type_id(*left)?),
        (ports[1], edit.type_id(*right)?),
    ];
    let original_left = edit.type_id(*original_left)?;
    let mut warnings = Vec::new();
    let graph = operation.expand(
        &mut edit,
        fragment.base.root_id(),
        inputs,
        original_left,
        &mut next,
        &mut warnings,
    )?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["terms"] = json!(graph
        .into_iter()
        .map(|(term, node)| (term, operator_number(node)))
        .collect::<BTreeMap<_, _>>());
    reply["warnings"] = json!(super::pass_warnings(warnings));
    Ok(reply)
}
