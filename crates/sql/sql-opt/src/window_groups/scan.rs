//! Select shared temporal windows by tracing their keys to a common input.
use crate::projection_key::{row_parameter, SourceRows, ProjectionKey};
use super::source::{compose_timestamp, filter_sources, TemporalFilterSource};
use super::temporal::{shared_temporal_inputs, TemporalCandidate};
use crate::conjunct_windows;
use crate::projection_composition::CompositionError;
use feldera_sql_expr::{ensure_expression_tree, SimplifyWarning};
use feldera_sql_ir::{Circuit, CircuitEditor, Error, ExpressionId, NodeId, OutputPort, TypeId};

pub enum TemporalScan {
    /// Groups and their members are in selection order. Each member identifies
    /// its original filter and the conjunct to move to the front.
    Groups {
        groups: Vec<Vec<(NodeId, usize)>>,
        warnings: Vec<SimplifyWarning>,
        generated_names: u64,
    },
    /// Replay expressions that require the general visitor or the full canonical printer.
    Paths(Vec<TemporalFilterSource>),
}

pub fn scan_temporal_windows(circuit: Circuit, threshold: usize) -> Result<TemporalScan, Error> {
    let mut edit = CircuitEditor::new(circuit);
    let paths = filter_sources(&edit)?;
    let mut candidates = Vec::with_capacity(paths.len());
    let mut input_types = SourceRows::default();
    let mut warnings = Vec::new();
    let mut generated_names = 0;
    for path in &paths {
        let Some(keys) = source_candidates(
            &mut edit,
            path,
            &mut input_types,
            &mut warnings,
            &mut generated_names,
        )?
        else {
            // Replaying the paths will report every diagnostic, including earlier ones.
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
    Ok(TemporalScan::Groups {
        groups,
        warnings,
        generated_names,
    })
}

type SourceCandidate = TemporalCandidate<(OutputPort, TypeId, ProjectionKey)>;

fn source_candidates(
    edit: &mut CircuitEditor,
    path: &TemporalFilterSource,
    input_types: &mut SourceRows,
    warnings: &mut Vec<SimplifyWarning>,
    next: &mut u64,
) -> Result<Option<Vec<SourceCandidate>>, Error> {
    let function = edit.node(path.filter)?.edge("function")?;
    let tree = ensure_expression_tree(edit, function)?;
    let predicate = edit.node(tree)?;
    let parameter = predicate.edges("parameters")?[0]; // filter_sources checked the arity
    let body = edit.expression(predicate.edge("body")?)?;
    let Some(windows) = conjunct_windows(edit, parameter, body, next)? else {
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
        let Some((input, key)) = source_key(edit, parameter, window.key, &maps, warnings)? else {
            return Ok(None);
        };
        let Some(row) = input_types.representative(edit, path.source, input)? else {
            return Ok(None);
        };
        candidates.push(TemporalCandidate {
            fingerprint: (path.source, row, key),
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
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<(TypeId, ProjectionKey)>, Error> {
    if !maps.is_empty() {
        let closure = edit.closure_with_parameters(key, &[parameter])?;
        let timestamp = match compose_timestamp(edit, closure, maps, warnings) {
            Ok(timestamp) => timestamp,
            // The live replay can identify both declarations in the failing map.
            Err(CompositionError::BindingConflict { .. }) => return Ok(None),
            Err(CompositionError::Ir(error)) => return Err(error),
        };
        let closure = edit.node(timestamp.into())?;
        let [input] = closure.edges("parameters")?[..] else {
            return Ok(None);
        };
        parameter = input;
        key = edit.expression(closure.edge("body")?)?;
    }
    let parameter = row_parameter(edit, parameter)?;
    Ok(ProjectionKey::read(edit, key, parameter)?.map(|key| (parameter.ty, key)))
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
        let TemporalScan::Groups {
            groups,
            warnings,
            generated_names: 0,
        } = scan_temporal_windows(edit.finish()?, 2)?
        else {
            panic!("direct columns must not require printed expressions");
        };
        assert!(warnings.is_empty());
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
