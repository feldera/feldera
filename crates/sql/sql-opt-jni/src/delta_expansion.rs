use super::{ExpressionSite, fragment::Fragment, operator_number};
use feldera_sql_ir::{CircuitEditor, OperatorKind, OutputPort};
use feldera_sql_opt::UnaryDelta;
use serde::Deserialize;
use serde_json::Value;
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
