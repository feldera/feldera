//! Rebuild a narrowed stream aggregate or TopK with its downstream indexed map.
use crate::{fragment::Fragment, ExpressionSite};
use feldera_sql_ir::{CircuitEditor, Field, OperatorKind as Op, OutputPort};
use feldera_sql_opt::{
    append_input_projection, trim_aggregate_projection, trim_topk_projection, TopK,
};
use serde::Deserialize;
use serde_json::Value;
use std::error::Error;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    port: usize,
    numbering: Option<String>,
}

pub(super) fn operators(
    pass: &str,
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn Error>> {
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(request.site, [Op::MapIndex], input)?;
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let site = fragment.base.root_id();
    let input = OutputPort {
        number: request.port,
        ..fragment.inputs[0]
    };
    match (pass, roots.as_slice()) {
        (
            "OptimizeProjections/TopKOutput",
            &[reader, collection, producer, rank, equality, limit],
        ) => {
            let Some([producer, reader]) = trim_topk_projection(&mut edit, producer, reader)?
            else {
                return Ok(Value::Null);
            };
            let topk = TopK {
                producer,
                rank,
                equality,
                limit,
                numbering: request
                    .numbering
                    .as_deref()
                    .ok_or("missing TopK numbering")?,
            }
            .append(&mut edit, site, site, input, collection)?;
            append_input_projection(&mut edit, site, site, topk, Op::MapIndex, reader)?;
        }
        ("OptimizeProjections/AggregateOutput", &[reader, output, list, reader_output]) => {
            let key = edit.node(output)?.edge("keyType")?;
            let empty = edit.node(list)?.edge("cachedEmptySetResult")?;
            let Some(projection) =
                trim_aggregate_projection(&mut edit, reader, empty, key, &mut next)?
            else {
                return Ok(Value::Null);
            };
            let list = projection.narrow_list(&mut edit, list)?;
            let value = edit.expression_type(projection.empty_row)?;
            let key = edit.type_id(key)?;
            let narrowed = edit.indexed_zset_type(output, key, value)?;
            let operator = edit.append_operator(
                site,
                site,
                Op::StreamAggregate,
                &[input],
                [
                    // The decoder reads the list first. Its cached row defines the shared value type.
                    ("aggregate", Field::Reference(list)),
                    ("outputType", Field::Reference(narrowed.into())),
                ],
            )?;
            let filler = append_input_projection(
                &mut edit,
                site,
                site,
                OutputPort {
                    operator,
                    number: 0,
                },
                Op::MapIndex,
                projection.filler,
            )?;
            edit.append_operator(
                site,
                site,
                Op::MapIndex,
                &[filler],
                [
                    ("function", Field::Reference(reader)),
                    ("outputType", Field::Reference(reader_output)),
                ],
            )?;
        }
        _ => return Err(format!("invalid roots for {pass}").into()),
    }
    fragment.reply(&edit, next)
}
