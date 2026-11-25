//! Marshal a narrowed unary reader together with its input projection.
use crate::{ExpressionSite, binding_diagnostic, fragment::Fragment, pass_warnings};
use feldera_sql_expr::SimplifyWarning;
use feldera_sql_ir::{CircuitEditor, Error, ExpressionId, Field, NodeId, OperatorKind as Op};
use feldera_sql_opt::{
    AggregateInput, FieldRewriteError, IndexedValueUse, append_input_projection,
    trim_aggregate_input, trim_flatmap_input, trim_indexed_reader,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, error::Error as StdError};

type Paths = BTreeSet<Vec<usize>>;

#[derive(Deserialize)]
struct Request {
    #[serde(flatten)]
    site: ExpressionSite,
    port: usize,
    #[serde(flatten)]
    reader: Reader,
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "reads")]
enum Reader {
    Flatmap(Paths),
    LinearAggregate(Paths),
    StreamAggregate(Vec<Paths>),
}

struct Narrowed {
    projection: ExpressionId,
    kind: Op,
    fields: Vec<(&'static str, Field)>,
}

impl Reader {
    fn narrow<'a>(
        self,
        base: &'a feldera_sql_ir::Circuit,
        edit: &mut CircuitEditor,
        roots: &[NodeId],
        next: &mut u64,
        warnings: &mut Vec<SimplifyWarning>,
    ) -> Result<Option<Narrowed>, FieldRewriteError<'a>> {
        let plan = match (self, roots) {
            (Self::Flatmap(paths), &[flatmap]) => {
                let Some([projection, reader]) =
                    trim_flatmap_input(base, edit, flatmap, &paths, next)?
                else {
                    return Ok(None);
                };
                Narrowed {
                    projection,
                    kind: Op::FlatMap,
                    fields: vec![("function", Field::Reference(reader.into()))],
                }
            }
            (Self::LinearAggregate(paths), &[reader, pair, postprocess]) => {
                let pair = edit.type_id(pair)?;
                let Some(plan) = trim_indexed_reader(
                    base,
                    edit,
                    reader,
                    pair,
                    IndexedValueUse::LinearAggregate { paths },
                    next,
                    warnings,
                )?
                else {
                    return Ok(None);
                };
                Narrowed {
                    projection: plan.projection,
                    kind: Op::AggregateLinearPostprocess,
                    fields: vec![
                        ("function", Field::Reference(plan.reader.into())),
                        ("postProcess", Field::Reference(postprocess)),
                    ],
                }
            }
            (Self::StreamAggregate(reads), [pair, variable, empty_tuple, components @ ..]) => {
                let row = edit.type_id(edit.node(*variable)?.edge("type")?)?;
                let input = AggregateInput {
                    pair: edit.type_id(*pair)?,
                    row,
                    components,
                };
                let Some(plan) = trim_aggregate_input(base, edit, input, reads, next, warnings)?
                else {
                    return Ok(None);
                };
                let aggregate = plan.aggregate_list(edit, *variable, *empty_tuple)?;
                Narrowed {
                    projection: plan.projection,
                    kind: Op::StreamAggregate,
                    fields: vec![("aggregate", Field::Reference(aggregate))],
                }
            }
            _ => return Err(Error::new("missing operands for unary field trimming").into()),
        };
        Ok(Some(plan))
    }
}

pub(super) fn operators(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn StdError>> {
    let request: Request = serde_json::from_slice(state)?;
    let map = match request.reader {
        Reader::Flatmap(_) => Op::Map,
        _ => Op::MapIndex,
    };
    let (fragment, roots) = Fragment::bind(request.site, [map], input)?;
    let (&output, operands) = roots.split_first().ok_or("missing declared output type")?;
    let base = &fragment.base;
    let mut edit = CircuitEditor::new(base.clone());
    let mut warnings = Vec::new();
    let mut plan = match request
        .reader
        .narrow(base, &mut edit, operands, &mut next, &mut warnings)
    {
        Ok(Some(plan)) => plan,
        Ok(None) => return Ok(Value::Null),
        Err(FieldRewriteError::Binding(issue)) => {
            return Ok(
                json!({"diagnostic": binding_diagnostic(issue), "next_generated_name": next}),
            );
        }
        Err(FieldRewriteError::InvalidIr(error)) => return Err(error.into()),
    };
    let body = base.root_id();
    let mut input = fragment.inputs[0];
    input.number = request.port;
    let projected = append_input_projection(&mut edit, body, body, input, map, plan.projection)?;
    plan.fields.push(("outputType", Field::Reference(output)));
    edit.append_operator(body, body, plan.kind, &[projected], plan.fields)?;
    let mut reply = fragment.reply(&edit, next)?;
    reply["warnings"] = json!(pass_warnings(warnings));
    Ok(reply)
}
