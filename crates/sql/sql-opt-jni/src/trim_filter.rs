//! Exchange the filter's live input port and closures with the field-trimming pass.
use crate::{ExpressionSite, binding_diagnostic, fragment::Fragment};
use feldera_sql_expr::live_parameter_field_paths;
use feldera_sql_ir::{CircuitEditor, Error as IrError, OperatorKind};
use feldera_sql_opt::{FilterTrim, trim_filter_fields};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, error::Error};

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    port: usize,
    indexed: bool,
    reader_fields: Option<BTreeSet<Vec<usize>>>,
    predicate_fields: Option<BTreeSet<Vec<usize>>>,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) =
        Fragment::with_ports(request.site, vec![request.port], OperatorKind::Map, input)?;
    let [reader, predicate] = roots[..] else {
        return Err("filter trimming requires the reader and predicate closures".into());
    };
    let base = &fragment.base;
    let mut edit = CircuitEditor::new(base.clone());
    let rewrite = trim_filter_fields(base, &mut edit, reader, predicate, &mut next, |closure| {
        let checked = if closure == reader {
            &request.reader_fields
        } else {
            &request.predicate_fields
        };
        if let Some(paths) = checked {
            return Ok(Some(paths.clone()));
        }
        live_parameter_field_paths(base, closure)?
            .map(|paths| {
                let [paths] = paths
                    .try_into()
                    .map_err(|_| IrError::new("expected one filter row parameter"))?;
                Ok(paths)
            })
            .transpose()
    })?;
    let trimmed = match rewrite {
        FilterTrim::Unchanged => return Ok(Value::Null),
        FilterTrim::InspectFields(id) => {
            return Ok(json!({
                "inspect": if id == reader { "reader_fields" } else { "predicate_fields" }
            }));
        }
        FilterTrim::Binding(issue) => {
            return Ok(json!({
                "binding_error": binding_diagnostic(issue), "next_generated_name": next
            }));
        }
        FilterTrim::Narrowed(trimmed) => trimmed,
    };
    let body = base.root_id();
    let kind = if request.indexed {
        OperatorKind::MapIndex
    } else {
        OperatorKind::Map
    };
    trimmed.append(&mut edit, body, [body, body], kind, fragment.inputs[0])?;
    fragment.reply(&edit, next)
}
