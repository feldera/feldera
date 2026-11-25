//! Rebuild a join and each projected input in the order of its live ports.
use crate::{ExpressionSite, binding_diagnostic, fragment::Fragment};
use feldera_sql_ir::{CircuitEditor, Field, NodeId, OperatorKind as Op, OutputPort};
use feldera_sql_opt::{JoinInputRewrite, append_input_projection, narrow_join_inputs};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, error::Error};

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    kind: String,
    ports: Vec<usize>,
    extra: Vec<Extra>,
    balanced: Option<bool>,
    left: Option<bool>,
    timestamps: Option<[usize; 2]>,
    paths: Option<Vec<BTreeSet<Vec<usize>>>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum Extra {
    Comparator,
    Filter,
    Map,
}

impl Extra {
    fn field(self) -> &'static str {
        match self {
            Self::Comparator => "comparator",
            Self::Filter => "filter",
            Self::Map => "map",
        }
    }
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let kind = Op::from_wire(&request.kind).ok_or("unknown field trimming join kind")?;
    let ports: Vec<_> = request
        .ports
        .into_iter()
        .enumerate()
        .map(|(i, number)| OutputPort {
            operator: NodeId::Outer(i as u64 + 1),
            number,
        })
        .collect();
    let (fragment, roots) =
        Fragment::with_inputs(request.site, ports, std::iter::repeat(Op::MapIndex), input)?;
    let [function, output, operands @ ..] = roots.as_slice() else {
        return Err("join trimming requires the original reader and declared output type".into());
    };
    let (pairs, extra) = operands
        .split_at_checked(fragment.inputs.len())
        .ok_or("missing join input type")?;
    if request.extra.len() != extra.len() {
        return Err("missing join comparator, filter or map".into());
    }
    let base = &fragment.base;
    let mut edit = CircuitEditor::new(base.clone());
    let rewrite = narrow_join_inputs(
        base,
        &mut edit,
        *function,
        pairs,
        request.timestamps,
        request.paths,
        &mut next,
    )?;
    let (projections, reader, timestamps) = match rewrite {
        JoinInputRewrite::InspectReader => return Ok(Value::Null),
        JoinInputRewrite::InvalidInput { parameter, paths } => {
            return Ok(json!({
                "invalid_input": parameter, "paths": paths, "next_generated_name": next
            }));
        }
        JoinInputRewrite::Binding(issue) => {
            return Ok(json!({
                "diagnostic": binding_diagnostic(issue), "next_generated_name": next
            }));
        }
        JoinInputRewrite::Unchanged => return Ok(json!({"narrowed": false})),
        JoinInputRewrite::Narrowed {
            inputs,
            reader,
            timestamps,
        } => (inputs, reader, timestamps),
    };
    let body = base.root_id();
    let mut inputs = fragment.inputs.clone();
    for (input, projection) in inputs.iter_mut().zip(projections) {
        if let Some(projection) = projection {
            *input =
                append_input_projection(&mut edit, body, body, *input, Op::MapIndex, projection)?;
        }
    }
    let mut fields = vec![
        ("outputType", Field::Reference(*output)),
        ("function", Field::Reference(reader.into())),
    ];
    fields.extend(
        request
            .extra
            .into_iter()
            .zip(extra)
            .map(|(name, &root)| (name.field(), Field::Reference(root))),
    );
    if let Some(balanced) = request.balanced {
        fields.push(("balanced", Field::Data(balanced.into())));
    }
    if let Some([left, right]) = timestamps {
        fields.extend([
            ("leftTimestampIndex", Field::Data(left.into())),
            ("rightTimestampIndex", Field::Data(right.into())),
            (
                "isLeft",
                Field::Data(request.left.ok_or("missing ASOF join direction")?.into()),
            ),
        ]);
    }
    edit.append_operator(body, body, kind, &inputs, fields)?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["narrowed"] = true.into();
    Ok(reply)
}
