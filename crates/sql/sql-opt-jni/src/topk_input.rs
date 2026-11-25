use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, OperatorKind, OutputPort};
use feldera_sql_opt::ProjectedTopK;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    port: usize,
    numbering: String,
    row: String,
    used: Vec<bool>,
    width: usize,
}

pub(super) fn operators(input: &[u8], state: &[u8], next: u64) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(request.site, [OperatorKind::MapIndex], input)?;
    let &[projection, producer, rank, equality, limit] = roots.as_slice() else {
        return Err("TopK trimming requires a projection, producer, comparators and limit".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let plan = ProjectedTopK {
        projection: edit.expression(projection)?,
        producer: edit.expression(producer)?,
        rank,
        equality,
        limit,
        numbering: &request.numbering,
        printed_row: &request.row,
        used: &request.used,
        projection_width: request.width,
    };
    let site = fragment.base.root_id();
    plan.append(
        &mut edit,
        site,
        site,
        OutputPort {
            number: request.port,
            ..fragment.inputs[0]
        },
    )?;
    fragment.reply(&edit, next)
}
