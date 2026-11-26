//! Install a shared projection or rebind a reader to a changed input type.
use crate::{fragment::Fragment, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, OperatorKind};
use feldera_sql_opt::SharedProjection;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    indexed: bool,
    port: usize,
}

pub(super) fn operator(input: &[u8], state: &[u8], next: u64) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) =
        Fragment::with_ports(request.site, vec![request.port], OperatorKind::Map, input)?;
    let [function, width, original @ ..] = roots.as_slice() else {
        return Err("shared projection requires its closure and input type".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let width = edit.type_id(*width)?;
    match original {
        [before] if edit.same_type(edit.type_id(*before)?, width)? => return Ok(Value::Null),
        [] | [_] => {}
        _ => return Err("expected at most one original reader input type".into()),
    }
    let kind = if request.indexed {
        OperatorKind::MapIndex
    } else {
        OperatorKind::Map
    };
    let projection =
        SharedProjection::new(&mut edit, kind, *function, width, &mut Default::default())?;
    projection.append(&mut edit, fragment.base.root_id(), fragment.inputs[0], None)?;
    fragment.reply(&edit, next)
}
