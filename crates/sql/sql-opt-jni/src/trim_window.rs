//! Bind the live window inputs while Rust narrows its single indexed reader.
use crate::{binding_diagnostic, fragment::Fragment, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, OperatorKind};
use feldera_sql_opt::{limiters::WindowBounds, trim_window_fields, FieldRewriteError};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, error::Error};

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    ports: [usize; 2],
    #[serde(flatten)]
    bounds: WindowBounds,
    paths: BTreeSet<Vec<usize>>,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) =
        Fragment::with_ports(request.site, request.ports.into(), OperatorKind::Map, input)?;
    let [function] = roots[..] else {
        return Err("window trimming requires its reader closure".into());
    };
    let base = &fragment.base;
    let mut edit = CircuitEditor::new(base.clone());
    let trim = match trim_window_fields(base, &mut edit, function, request.paths, &mut next) {
        Ok(None) => return Ok(Value::Null),
        Ok(Some(trim)) => trim,
        Err(FieldRewriteError::Binding(issue)) => {
            return Ok(json!({
                "binding_error": binding_diagnostic(issue), "next_generated_name":next
            }));
        }
        Err(FieldRewriteError::InvalidIr(error)) => return Err(error.into()),
    };
    let body = base.root_id();
    trim.append(
        &mut edit,
        body,
        [body, body],
        [fragment.inputs[0], fragment.inputs[1]],
        request.bounds,
    )?;
    fragment.reply(&edit, next)
}
