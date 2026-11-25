//! Bind a projected filtered join to its live function, predicate and indexed inputs.
use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, OperatorKind};
use feldera_sql_opt::FilteredJoinProjection;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    ports: Vec<usize>,
    balanced: Option<bool>,
    has_filter: bool,
}

pub(super) fn operators(input: &[u8], state: &[u8], next: u64) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) =
        Fragment::with_ports(request.site, request.ports, OperatorKind::MapIndex, input)?;
    let [output, function, reader, optional @ ..] = roots.as_slice() else {
        return Err(
            "a projected filtered join requires its output type, function and reader".into(),
        );
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let (filter, map) = match (request.has_filter, optional) {
        (false, []) => (None, None),
        (false, [map]) => (None, Some(edit.expression(*map)?)),
        (true, [filter]) => (Some(edit.expression(*filter)?), None),
        (true, [filter, map]) => (
            Some(edit.expression(*filter)?),
            Some(edit.expression(*map)?),
        ),
        _ => return Err("invalid filter and map operands for a filtered join".into()),
    };
    let join = FilteredJoinProjection {
        output: edit.type_id(*output)?,
        function: edit.expression(*function)?,
        reader: edit.expression(*reader)?,
        filter,
        map,
        balanced: request.balanced,
    };
    let mut warnings = Vec::new();
    if let Err(error) = join.append(
        &mut edit,
        fragment.base.root_id(),
        &fragment.inputs,
        &mut warnings,
    ) {
        return crate::composition_error(error);
    }
    let mut reply = fragment.reply(&edit, next)?;
    reply["warnings"] = crate::pass_warnings(warnings).into();
    Ok(reply)
}
