use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, OperatorKind};
use feldera_sql_opt::{AntijoinReader, AntijoinRewrite};
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    join: String,
    ports: [usize; 2],
    indexed: bool,
    width: usize,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::with_ports(
        request.site,
        request.ports.into(),
        OperatorKind::MapIndex,
        input,
    )?;
    let [left, right, function] = roots[..] else {
        return Err("antijoin projection requires both stream types and the reader".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let reader = AntijoinReader {
        join: OperatorKind::from_wire(&request.join).ok_or("unknown antijoin kind")?,
        inputs: [fragment.inputs[0], fragment.inputs[1]],
        streams: [edit.type_id(left)?, edit.type_id(right)?],
        function,
        indexed: request.indexed,
        width: request.width,
    };
    let mut warnings = Vec::new();
    let replaces =
        match reader.append(&mut edit, fragment.base.root_id(), &mut next, &mut warnings)? {
            AntijoinRewrite::CannotSplitKey => return Ok(Value::Null),
            AntijoinRewrite::KeepReader => false,
            AntijoinRewrite::ReplaceReader => true,
        };
    let mut reply = fragment.reply(&edit, next)?;
    reply["replaces_reader"] = replaces.into();
    reply["warnings"] = crate::pass_warnings(warnings).into();
    Ok(reply)
}
