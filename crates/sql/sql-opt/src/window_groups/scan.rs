//! Select shared temporal windows by tracing their keys to a common input.
use super::key::{row_parameter, same_row_spelling, TimestampKey};
use super::source::{compose_timestamp, filter_sources, TemporalFilterSource};
use super::temporal::{shared_temporal_inputs, TemporalCandidate};
use crate::conjunct_windows;
use feldera_sql_expr::ensure_expression_tree;
use feldera_sql_ir::{Circuit, CircuitEditor, Error, ExpressionId, NodeId, OutputPort, TypeId};
use std::collections::HashMap;

pub enum TemporalScan {
    /// Groups and their members are in selection order. Each member identifies
    /// its original filter and the conjunct to move to the front.
    Groups(Vec<Vec<(NodeId, usize)>>),
    /// Replay expressions that need diagnostics or the full canonical printer.
    Paths(Vec<TemporalFilterSource>),
}

pub fn scan_temporal_windows(circuit: Circuit, threshold: usize) -> Result<TemporalScan, Error> {
    let mut edit = CircuitEditor::new(circuit);
    let paths = filter_sources(&edit)?;
    let mut candidates = Vec::with_capacity(paths.len());
    let mut input_types = HashMap::new();
    for path in &paths {
        let Some(keys) = source_candidates(&mut edit, path, &mut input_types)? else {
            return Ok(TemporalScan::Paths(paths));
        };
        candidates.push(keys);
    }
    let groups = shared_temporal_inputs(&candidates, threshold)
        .into_iter()
        .map(|group| {
            group
                .into_iter()
                .map(|(i, conjunct)| (paths[i].filter, conjunct))
                .collect()
        })
        .collect();
    Ok(TemporalScan::Groups(groups))
}

type SourceCandidate = TemporalCandidate<(OutputPort, TimestampKey)>;

fn source_candidates(
    edit: &mut CircuitEditor,
    path: &TemporalFilterSource,
    input_types: &mut HashMap<OutputPort, TypeId>,
) -> Result<Option<Vec<SourceCandidate>>, Error> {
    let function = edit.node(path.filter)?.edge("function")?;
    let tree = ensure_expression_tree(edit, function)?;
    let predicate = edit.node(tree)?;
    let parameter = predicate.edges("parameters")?[0]; // filter_sources checked the arity
    let body = edit.expression(predicate.edge("body")?)?;
    let Some(windows) = conjunct_windows(edit, parameter, body)? else {
        return Ok(None);
    };
    let maps = path
        .maps
        .iter()
        .map(|&id| edit.node(id)?.edge("function"))
        .collect::<Result<Vec<_>, _>>()?;
    let mut candidates = Vec::new();
    for (conjunct, window) in windows.into_iter().enumerate() {
        let Some(window) = window else { continue };
        let Some((input, key)) = source_key(edit, parameter, window.key, &maps)? else {
            return Ok(None);
        };
        let common = *input_types.entry(path.source).or_insert(input);
        if !same_row_spelling(edit, common, input)? {
            return Ok(None);
        }
        candidates.push(TemporalCandidate {
            fingerprint: (path.source, key),
            conjunct,
        });
    }
    Ok(Some(candidates))
}

fn source_key(
    edit: &mut CircuitEditor,
    mut parameter: NodeId,
    mut key: ExpressionId,
    maps: &[NodeId],
) -> Result<Option<(TypeId, TimestampKey)>, Error> {
    if !maps.is_empty() {
        let closure = edit.closure_with_parameters(key, &[parameter])?;
        let mut warnings = Vec::new();
        let timestamp = compose_timestamp(edit, closure, maps, &mut warnings)?;
        // Replay the ordinary composition when it has a diagnostic to report.
        if !warnings.is_empty() {
            return Ok(None);
        }
        let closure = edit.node(timestamp.into())?;
        let [input] = closure.edges("parameters")?[..] else {
            return Ok(None);
        };
        parameter = input;
        key = edit.expression(closure.edge("body")?)?;
    }
    let parameter = row_parameter(edit, parameter)?;
    Ok(TimestampKey::read(edit, key, parameter)?.map(|key| (parameter.ty, key)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sql_ir::{BinaryOp, ExpressionKind, Field, OperatorKind};
    use serde_json::json;

    #[test]
    fn matching_columns_share_only_within_one_input_port() -> Result<(), Error> {
        let graph = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
                {"class":"DBSPSourceMultisetOperator","id":1,"inputs":[]},
                {"class":"DBSPSourceMultisetOperator","id":2,"inputs":[]}
            ]
        }))?;
        let (graph, types) = graph.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false
        })])?;
        let mut edit = CircuitEditor::new(graph);
        let site = edit.circuit().root_id();
        let timestamp = edit.type_id(types[0])?;
        let boolean = edit.bool_type(site, false)?;
        let function = edit.function_path(site, "now")?;
        let clock = edit.make_expr(
            site,
            ExpressionKind::Apply,
            timestamp,
            [
                ("function", Field::Reference(function.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        let mut filters = Vec::new();
        let mut names = 0;
        for (source, column, transform) in [
            (1, 0, None),
            (2, 0, None),
            (1, 1, None),
            (1, 0, None),
            (1, 0, Some("floor_ts")),
            (1, 0, Some("ceil_ts")),
            (1, 0, Some("floor_ts")),
        ] {
            // The seam can give copies of the same row signature different IDs.
            let row = edit.tuple_type(site, false, [timestamp.into(), timestamp.into()])?;
            let param = edit.ref_parameter(row, &mut names)?;
            let value = edit.deref_parameter(&param)?;
            let key = edit.tuple_field(value, column)?;
            let key = if let Some(name) = transform {
                let function = edit.function_path(site, name)?;
                edit.make_expr(
                    site,
                    ExpressionKind::Apply,
                    timestamp,
                    [
                        ("function", Field::Reference(function.into())),
                        (
                            "arguments",
                            Field::Array(vec![Field::Reference(key.into())]),
                        ),
                    ],
                )?
            } else {
                key
            };
            let predicate = edit.binary_expression(site, boolean, BinaryOp::Gte, key, clock)?;
            let closure = edit.closure(predicate, &[param])?;
            filters.push(edit.append_operator(
                site,
                site,
                OperatorKind::Filter,
                &[OutputPort {
                    operator: NodeId::Outer(source),
                    number: 0,
                }],
                [("function", Field::Reference(closure.into()))],
            )?);
        }
        let TemporalScan::Groups(groups) = scan_temporal_windows(edit.finish()?, 2)? else {
            panic!("direct columns must not require printed expressions");
        };
        assert_eq!(
            groups,
            [
                vec![(filters[0], 0), (filters[3], 0)],
                vec![(filters[4], 0), (filters[6], 0)],
            ]
        );
        Ok(())
    }
}
