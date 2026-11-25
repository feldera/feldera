//! Bind the inputs of a linear projection rewrite to live ports.
use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, OperatorKind as Op};
use feldera_sql_opt::ProjectionPushdown;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    producer: String,
    ports: Vec<usize>,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let producer = Op::from_wire(&request.producer).ok_or("unknown projection producer")?;
    let (fragment, roots) = Fragment::with_ports(request.site, request.ports, Op::Map, input)?;
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let (reader, output, producer_function) = match *roots.as_slice() {
        [reader, output] => (reader, output, None),
        [reader, output, function] if matches!(producer, Op::Delay | Op::AggregateZero) => {
            (reader, output, Some(edit.expression(function)?))
        }
        _ => {
            return Err(
                "expected a projection, its output type and an optional producer value".into(),
            );
        }
    };
    let pushdown = ProjectionPushdown {
        producer,
        reader: edit.expression(reader)?,
        output: edit.type_id(output)?,
        producer_function,
    };
    let mut warnings = Vec::new();
    pushdown.append(
        &mut edit,
        fragment.base.root_id(),
        &fragment.inputs,
        &mut next,
        &mut warnings,
    )?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["warnings"] = crate::pass_warnings(warnings).into();
    Ok(reply)
}
