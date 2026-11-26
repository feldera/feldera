use crate::{binding_diagnostic, fragment::Fragment, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, OperatorKind, OutputPort, TypeKind};
use feldera_sql_opt::{
    append_split_map, checked_split_map_input, split_map_input, FieldRewriteError, MapOutput,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, error::Error};

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    port: usize,
    inspected: bool,
    paths: BTreeSet<Vec<usize>>,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(request.site, [OperatorKind::Map], input)?;
    let &[reader, input_ty, output_ty] = roots.as_slice() else {
        return Err("map split requires a reader, input type and output type".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let input_ty = edit.type_id(input_ty)?;
    let output_ty = edit.type_id(output_ty)?;
    let output = if edit.type_kind(output_ty)? == TypeKind::IndexedZSet {
        MapOutput::Indexed
    } else {
        MapOutput::Row
    };
    let split = if request.inspected {
        match checked_split_map_input(
            &fragment.base,
            &mut edit,
            reader,
            &request.paths,
            output,
            &mut next,
        ) {
            Ok(split) => split,
            Err(FieldRewriteError::InvalidIr(error)) => return Err(error.into()),
            Err(FieldRewriteError::Binding(issue)) => {
                return Ok(json!({
                    "diagnostic": binding_diagnostic(issue), "next_generated_name": next
                }));
            }
        }
    } else {
        split_map_input(&mut edit, reader, &request.paths, output, &mut next)?
    };
    let body = fragment.base.root_id();
    let port = OutputPort {
        number: request.port,
        ..fragment.inputs[0]
    };
    let inherit_annotations = if let Some(split) = split {
        append_split_map(&mut edit, body, body, port, input_ty, output_ty, split)?
    } else {
        false
    };
    let mut reply = fragment.reply(&edit, next)?;
    reply["inherit_annotations"] = inherit_annotations.into();
    Ok(reply)
}
