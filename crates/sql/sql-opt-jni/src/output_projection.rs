//! Fold a live Map or MapIndex into the producer chosen by the projection pass.
use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, Field, OperatorKind};
use feldera_sql_opt::OutputProjection;
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    producer: String,
    ports: Vec<usize>,
    indexed: bool,
    single_reader: bool,
    balanced: Option<bool>,
    timestamps: Option<[usize; 2]>,
    is_left: Option<bool>,
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    next: u64,
    join_selected: bool,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let kind = OperatorKind::from_wire(&request.producer).ok_or("unknown projection producer")?;
    let (fragment, roots) =
        Fragment::with_ports(request.site, request.ports, OperatorKind::MapIndex, input)?;
    let [before, reader, output, extra @ ..] = roots.as_slice() else {
        return Err(
            "output projection requires the producer function, reader and output type".into(),
        );
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let projection = OutputProjection {
        producer: kind,
        indexed: request.indexed,
        single_reader: request.single_reader,
        output_type: edit.type_id(*output)?,
    };
    let before = edit.expression(*before)?;
    let mut warnings = Vec::new();
    let projected = if join_selected {
        projection
            .compose_join(&mut edit, before, *reader, &mut warnings)
            .map(Some)
    } else {
        projection.apply(&mut edit, before, *reader, &mut warnings)
    };
    let projected = match projected {
        Ok(Some(output)) => output,
        Ok(None) => return Ok(Value::Null),
        Err(error) => return crate::composition_error(error),
    };
    let mut properties = Vec::new();
    if let Some(balanced) = request.balanced {
        properties.push(("balanced", Field::Data(balanced.into())));
    }
    match (request.timestamps, request.is_left, extra) {
        (Some([left, right]), Some(is_left), [comparator]) if kind == OperatorKind::AsofJoin => {
            properties.extend([
                ("leftTimestampIndex", Field::Data(left.into())),
                ("rightTimestampIndex", Field::Data(right.into())),
                ("isLeft", Field::Data(is_left.into())),
                ("comparator", Field::Reference(*comparator)),
            ]);
        }
        (None, None, []) if kind != OperatorKind::AsofJoin => {}
        _ => return Err("invalid ASOF timestamp fields or comparator".into()),
    }
    let site = fragment.base.root_id();
    projected.append(&mut edit, site, site, &fragment.inputs, properties)?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["warnings"] = crate::pass_warnings(warnings).into();
    Ok(reply)
}
