//! Fold a live Map or MapIndex into the producer chosen by the projection pass.
use crate::{ExpressionSite, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, ExpressionKind, Field, OperatorKind};
use feldera_sql_opt::{OutputProjection, ProjectionMode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::error::Error;

#[derive(Deserialize)]
struct UnaryRequest {
    #[serde(flatten)]
    site: ExpressionSite,
    producer: String,
    port: usize,
    indexed: bool,
    only_projections: bool,
    reader_width: usize,
    producer_width: Option<usize>,
}

pub(super) fn unary(input: &[u8], state: &[u8], mut next: u64) -> Result<Value, Box<dyn Error>> {
    use OperatorKind as Op;
    let request: UnaryRequest = serde_json::from_slice(state)?;
    let kind = Op::from_wire(&request.producer).ok_or("unknown unary producer")?;
    let (fragment, roots) = Fragment::with_ports(request.site, vec![request.port], Op::Map, input)?;
    let [producer, reader, output] = roots[..] else {
        return Err("unary projection requires two functions and the output type".into());
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let blocked = matches!(kind, Op::Map | Op::MapIndex)
        && request.only_projections
        && request
            .producer_width
            .is_some_and(|width| width <= request.reader_width);
    if blocked && !request.indexed {
        return Ok(Value::Null);
    }
    let before = edit.expression(producer)?;
    let producer_kind = edit.expr_kind(before)?;
    let reader_kind = edit.expr_kind(edit.expression(reader)?)?;
    if !blocked {
        if kind == Op::FlatMap {
            if producer_kind != ExpressionKind::Flatmap || reader_kind != ExpressionKind::Closure {
                return Ok(Value::Null);
            }
        } else {
            if producer_kind != ExpressionKind::Closure {
                return Ok(if kind == Op::Map && !request.indexed {
                    Value::Null
                } else {
                    json!({"closure":"producer"})
                });
            }
            if reader_kind != ExpressionKind::Closure {
                return Ok(json!({"closure":"reader"}));
            }
            if kind == Op::MapIndex && edit.node(reader)?.edges("parameters")?.len() != 1 {
                return Ok(json!({"invalid_arity":true}));
            }
        }
    }
    if matches!(kind, Op::Map | Op::MapIndex) {
        // A width-blocked indexed reader still goes through strict projection
        // analysis in the reference visitor before it keeps its producer.
        let mode = if blocked || kind == Op::MapIndex {
            ProjectionMode::Strict
        } else {
            ProjectionMode::NestedWithCasts
        };
        let result = crate::checked_projection(&edit, reader, mode)?;
        if !result.is_boolean() {
            return Ok(json!({"projection_error":result}));
        }
        if blocked {
            return Ok(Value::Null);
        }
    }
    let projection = OutputProjection {
        producer: kind,
        indexed: request.indexed,
        single_reader: true,
        output_type: edit.type_id(output)?,
    };
    let mut warnings = Vec::new();
    let result = match projection.compose_unary(
        &mut edit,
        before,
        reader,
        request.only_projections,
        &mut next,
        &mut warnings,
    ) {
        Ok(Some(projected)) => projected,
        Ok(None) => return Ok(Value::Null),
        Err(error) => return crate::composition_error(error),
    };
    let site = fragment.base.root_id();
    result.append(&mut edit, site, site, &fragment.inputs, [])?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["warnings"] = crate::pass_warnings(warnings).into();
    Ok(reply)
}

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
