//! Split temporal predicates and merge bounds that share a NOW window.
use crate::monotonicity::row_projection::WaterlineTypeError;
use crate::{
    TemporalPredicate, WindowTerm, accepts_temporal_bound,
    filters::rewrite_filters,
    function_calls::contains_now,
    temporal_comparisons::{
        ClockError, MissingClockKey, checked_temporal_predicates, speculative_clock,
    },
};
use feldera_sql_expr::{SimplifyWarning, ensure_expression_tree, simplify_closure_with_warnings};
use feldera_sql_ir::{
    BinaryOp, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind as Expr, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};

/// Decide if adjacent filters can use a single window. An unknown result asks
/// the caller for its general closure analysis. Publish temporary names only
/// after both predicates and their key comparisons have been decided.
fn can_merge_temporal_filters(
    edit: &mut CircuitEditor,
    source: ExpressionId,
    filter: ExpressionId,
    next: &mut u64,
) -> Result<Option<bool>, ClockError> {
    if !contains_now(edit.circuit(), &[filter.into()])?
        || !contains_now(edit.circuit(), &[source.into()])?
    {
        return Ok(Some(false));
    }
    let Some(upstream) = window_terms(edit, source, next)? else {
        return Ok(None);
    };
    let Some(downstream) = window_terms(edit, filter, next)? else {
        return Ok(None);
    };
    let decision = match downstream.as_slice() {
        [term] => accepts_temporal_bound(edit, &upstream, *term)?,
        _ => Some(false),
    };
    Ok(decision)
}

/// Merge adjacent bounds in producer order. An intermediate filter remains
/// available to its other readers. Later filters see its rewritten predicate
/// and input. An unsupported expression leaves the caller's pass state intact.
pub fn merge_temporal_filters(
    circuit: Circuit,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Circuit>, Error> {
    match merge_temporal_filters_with(circuit, next, warnings, |_| Ok(None))? {
        Some(TemporalMerge::Rewritten(circuit)) => Ok(Some(circuit)),
        _ => Ok(None),
    }
}

/// The upstream closure can already contain earlier merges. The live analyzer
/// consumes queued warnings and resumes the same generated-name sequence.
pub struct TemporalFilterPair<'a> {
    pub graph: &'a Circuit,
    pub operators: [NodeId; 2],
    pub closures: [ExpressionId; 2],
    pub next: &'a mut u64,
    pub warnings: &'a mut Vec<SimplifyWarning>,
}

/// The live caller publishes earlier diagnostics and names before reporting
/// a missing key. A detached caller can discard this state and replay the pass.
pub enum TemporalMerge {
    Rewritten(Circuit),
    MissingBound {
        next_name: u64,
        warnings: Vec<SimplifyWarning>,
    },
    TypeError {
        kind: WaterlineTypeError,
        ty: TypeId,
        next_name: u64,
        warnings: Vec<SimplifyWarning>,
    },
    MissingKey {
        key: MissingClockKey,
        next_name: u64,
        warnings: Vec<SimplifyWarning>,
    },
}

pub fn merge_temporal_filters_with(
    circuit: Circuit,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
    mut analyze: impl FnMut(TemporalFilterPair<'_>) -> Result<Option<bool>, Error>,
) -> Result<Option<TemporalMerge>, Error> {
    let order = circuit.operator_order(feldera_sql_ir::BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit.clone());
    let mut projections = crate::rebuild::ProjectionNames::default();
    let mut names = *next;
    let mut diagnostics = Vec::new();
    for id in order {
        let filter = edit.node(id)?;
        projections.visit(id, filter, &mut names)?;
        if filter.operator_kind() != Some(Op::Filter) {
            continue;
        }
        let input = filter
            .inputs()
            .next()
            .ok_or_else(|| Error::new("filter requires an input"))?;
        let source = edit.node(input.operator)?;
        if source.operator_kind() != Some(Op::Filter)
            || edit.type_kind(edit.type_id(filter.edge("outputType")?)?)?
                == feldera_sql_ir::TypeKind::IndexedZSet
        {
            continue;
        }
        let upstream = edit.expression(source.edge("function")?)?;
        let downstream = edit.expression(filter.edge("function")?)?;
        let rows = source
            .inputs()
            .next()
            .ok_or_else(|| Error::new("upstream filter requires an input"))?;
        let mut trial_names = names;
        let decision =
            match can_merge_temporal_filters(&mut edit, upstream, downstream, &mut trial_names) {
                Ok(Some(merge)) => {
                    names = trial_names;
                    Some(merge)
                }
                Err(ClockError::Missing(key))
                    if circuit.node(key.expression().into()).is_some() =>
                {
                    return Ok(Some(TemporalMerge::MissingKey {
                        key,
                        next_name: trial_names,
                        warnings: diagnostics,
                    }));
                }
                Err(ClockError::MissingBound) => {
                    return Ok(Some(TemporalMerge::MissingBound {
                        next_name: trial_names,
                        warnings: diagnostics,
                    }));
                }
                Err(ClockError::Type(kind, ty)) if circuit.node(ty.into()).is_some() => {
                    return Ok(Some(TemporalMerge::TypeError {
                        kind,
                        ty,
                        next_name: trial_names,
                        warnings: diagnostics,
                    }));
                }
                Err(ClockError::InvalidIr(error)) => return Err(error),
                // Recreated expressions lack a live Java ID. Resume from the start
                // of this predicate pair when the diagnostic needs one.
                Ok(None) | Err(ClockError::Missing(_) | ClockError::Type(..)) => {
                    analyze(TemporalFilterPair {
                        graph: edit.circuit(),
                        operators: [input.operator, id],
                        closures: [upstream, downstream],
                        next: &mut names,
                        warnings: &mut diagnostics,
                    })?
                }
            };
        match decision {
            None => return Ok(None),
            Some(false) => continue,
            Some(true) => {}
        }
        let function = merge_temporal_predicates(
            &mut edit,
            upstream,
            downstream,
            &mut names,
            &mut diagnostics,
        )?;
        edit.rewire_inputs(id, &[rows])?;
        edit.set_operator_field(id, "function", Field::Reference(function.into()))?;
        edit.clear_annotations(id)?;
        projections.replaced(id);
    }
    let circuit = projections.finish(edit.finish_with_rewrites()?)?;
    *next = names;
    warnings.extend(diagnostics);
    Ok(Some(TemporalMerge::Rewritten(circuit)))
}

fn window_terms(
    edit: &mut CircuitEditor,
    closure: ExpressionId,
    next: &mut u64,
) -> Result<Option<Vec<WindowTerm>>, ClockError> {
    Ok(
        checked_closure(edit, closure, next)?.map(|(parameter, predicates)| {
            predicates
                .iter()
                .map(|predicate| WindowTerm::from_predicate(parameter, predicate))
                .collect()
        }),
    )
}

pub(super) fn classify_closure(
    edit: &mut CircuitEditor,
    closure: ExpressionId,
    next: &mut u64,
) -> Result<Option<(NodeId, Vec<TemporalPredicate>)>, Error> {
    speculative_clock(next, |names| checked_closure(edit, closure, names))
}

fn checked_closure(
    edit: &mut CircuitEditor,
    closure: ExpressionId,
    next: &mut u64,
) -> Result<Option<(NodeId, Vec<TemporalPredicate>)>, ClockError> {
    let tree = ensure_expression_tree(edit, closure.into())?;
    let node = edit.node(tree)?;
    let [parameter] = node.edges("parameters")?[..] else {
        return Err(Error::new("a temporal filter requires one row parameter").into());
    };
    let body = crate::filters::strip_wrap_bool(edit, edit.expression(node.edge("body")?)?)?;
    Ok(checked_temporal_predicates(edit, parameter, body, next)?
        .map(|predicates| (parameter, predicates)))
}

/// Combine two compatible temporal bounds on a single row. Both calls use the
/// same parameter. Reducing the completed closure preserves shared expressions.
pub fn merge_temporal_predicates(
    edit: &mut CircuitEditor,
    source: ExpressionId,
    filter: ExpressionId,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<ExpressionId, Error> {
    let [parameter] = edit
        .node(source.into())?
        .edges("parameters")?
        .try_into()
        .map_err(|_| Error::new("a temporal filter requires one row parameter"))?;
    let input = edit.type_id(edit.node(parameter)?.edge("type")?)?;
    let row = edit.variable(input, next)?;
    let left = edit.call_closure(source, &[row])?;
    let right = edit.call_closure(filter, &[row])?;
    let result = edit.expression_type(right)?;
    let both = edit.binary_expression(source.into(), result, BinaryOp::And, left, right)?;
    let closure = edit.closure_from_variables(both, &[row])?;
    let reduced = feldera_sql_expr::reduce_closure_body_with_warnings(
        edit,
        closure.into(),
        &[],
        &[],
        Some(warnings),
    )?;
    edit.expression(reduced)
}

pub fn break_temporal_filters(
    circuit: Circuit,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    rewrite_filters(circuit, next, |edit, body, id, _, warnings| {
        split_filter(edit, body, id, warnings)
    })
}

fn split_filter(
    edit: &mut CircuitEditor,
    body: NodeId,
    id: NodeId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<(NodeId, OutputPort)>, Error> {
    let node = edit.node(id)?;
    let function = node.edge("function")?;
    if !contains_now(edit.circuit(), &[function])? {
        return Ok(None);
    }
    let input = node
        .inputs()
        .next()
        .ok_or_else(|| Error::new("filter requires an input"))?;
    let output_type = node.edge("outputType")?;
    let function = simplify_closure_with_warnings(edit, function, Some(warnings))?;
    let parameters = edit.node(function)?.edges("parameters")?;
    let predicate = crate::filters::strip_wrap_bool(
        edit,
        edit.expression(edit.node(function)?.edge("body")?)?,
    )?;
    let binary = edit.node(predicate.into())?;
    if edit.expr_kind(predicate)? != Expr::Binary {
        return Ok(None);
    }
    let disjunction = match binary.data("opcode").and_then(|v| v.as_str()) {
        Some("AND") => false,
        Some("OR") => true,
        _ => return Ok(None),
    };
    let left = edit.expression(binary.edge("left")?)?;
    let right = edit.expression(binary.edge("right")?)?;
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    let collection = || ("outputType", Field::Reference(output_type));
    let filter = |edit: &mut CircuitEditor, condition, input| {
        let condition = edit.wrap_bool_if_needed(condition)?;
        let closure = edit.closure_with_parameters(condition, &parameters)?;
        edit.append_operator(
            body,
            id,
            Op::Filter,
            &[input],
            [collection(), ("function", Field::Reference(closure.into()))],
        )
        .map(port)
    };
    let first = filter(edit, left, input)?;
    let second = filter(edit, right, if disjunction { input } else { first })?;
    if !disjunction {
        return Ok(Some((first.operator, second)));
    }

    // Rows accepted by both predicates occur twice in the sum. Subtract that
    // intersection once to preserve the multiplicity of every input row.
    let sum = edit.append_operator(body, id, Op::Sum, &[first, second], [collection()])?;
    let first_closure = edit.node(first.operator)?.edge("function")?;
    let nullable = edit.nullable(edit.expression_type(left)?)?
        || edit.nullable(edit.expression_type(right)?)?;
    let ty = edit.bool_type(first_closure, nullable)?;
    let both = edit.make_expr(
        first_closure,
        Expr::Binary,
        ty,
        [
            ("left", Field::Reference(left.into())),
            ("right", Field::Reference(right.into())),
            ("opcode", Field::Data("AND".into())),
        ],
    )?;
    let intersection = filter(edit, both, input)?;
    let result = edit.append_operator(
        body,
        id,
        Op::Subtract,
        &[port(sum), intersection],
        [collection()],
    )?;
    Ok(Some((first.operator, port(result))))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_chain_merges_compatible_bounds_and_preserves_other_readers() -> Result<(), Error> {
        let circuit = Circuit::from_json(json!({
            "class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[]
        }))?;
        let (circuit, roots) = circuit.with_inner_roots([json!({
            "class":"DBSPTypeTimestamp","id":1,"code":"TIMESTAMP","mayBeNull":false
        })])?;
        let body = circuit.root_id();
        let mut edit = CircuitEditor::new(circuit);
        let site = roots[0];
        let timestamp = edit.type_id(site)?;
        let row = edit.tuple_type(site, false, [site, site])?;
        let reference = edit.reference_type(row)?;
        let collection = edit.zset_type(row)?;
        let output = || ("outputType", Field::Reference(collection.into()));
        let source = edit.append_operator(body, body, Op::Constant, &[], [output()])?;
        let now = edit.function_path(site, "now")?;
        let clock = edit.make_expr(
            site,
            Expr::Apply,
            timestamp,
            [
                ("function", Field::Reference(now.into())),
                ("arguments", Field::Array(vec![])),
            ],
        )?;
        let boolean = edit.bool_type(site, false)?;
        let port = |operator| OutputPort {
            operator,
            number: 0,
        };
        let filter = |edit: &mut CircuitEditor, input, predicate| {
            edit.append_operator(
                body,
                body,
                Op::Filter,
                &[port(input)],
                [output(), ("function", Field::Reference(predicate))],
            )
        };
        let bound = |edit: &mut CircuitEditor, input, column, comparison| {
            let variable = edit.named_variable(reference, "row")?;
            let value = edit.dereference(variable)?;
            let key = edit.tuple_field(value, column)?;
            let predicate = edit.binary_expression(site, boolean, comparison, key, clock)?;
            let closure = edit.closure_from_variables(predicate, &[variable])?;
            filter(edit, input, closure.into())
        };
        let lower = bound(&mut edit, source, 0, BinaryOp::Gt)?;
        let upper = bound(&mut edit, lower, 0, BinaryOp::Lte)?;
        let compatible = bound(&mut edit, upper, 0, BinaryOp::Gt)?;
        let different_key = bound(&mut edit, upper, 1, BinaryOp::Gt)?;
        let different_inclusivity = bound(&mut edit, lower, 0, BinaryOp::Gte)?;
        let circuit = edit.finish()?;
        let mut next = 10;
        let mut warnings = Vec::new();
        let merged = merge_temporal_filters(circuit.clone(), &mut next, &mut warnings)?.unwrap();
        for (filter, input) in [
            (lower, source),
            (upper, source),
            (compatible, source),
            (different_key, upper),
            (different_inclusivity, lower),
        ] {
            assert_eq!(
                merged.node(filter).unwrap().inputs().next(),
                Some(port(input))
            );
        }
        assert_eq!(next, 12);
        assert!(warnings.is_empty());

        let mut edit = CircuitEditor::new(circuit);
        let variable = edit.named_variable(reference, "row")?;
        let row = edit.dereference(variable)?;
        let key = edit.tuple_field(row, 0)?;
        let cloned = edit.clone_value(clock)?;
        let predicate = edit.binary_expression(site, boolean, BinaryOp::Gt, key, cloned)?;
        let closure = edit.closure_from_variables(predicate, &[variable])?;
        let cloned_filter = filter(&mut edit, compatible, closure.into())?;
        let circuit = edit.finish()?;
        let mut next = 10;
        let merged = merge_temporal_filters(circuit.clone(), &mut next, &mut warnings)?.unwrap();
        assert_eq!(
            merged.node(cloned_filter).unwrap().inputs().next(),
            Some(port(source))
        );

        // A conditional containing a clock is a residual, so it stays on its original input.
        let mut edit = CircuitEditor::new(circuit);
        let conditional = edit.make_expr(
            site,
            Expr::If,
            timestamp,
            [
                ("condition", Field::Reference(predicate.into())),
                ("positive", Field::Reference(clock.into())),
                ("negative", Field::Reference(key.into())),
            ],
        )?;
        let predicate = edit.binary_expression(site, boolean, BinaryOp::Gt, key, conditional)?;
        let closure = edit.closure_from_variables(predicate, &[variable])?;
        let residual = filter(&mut edit, cloned_filter, closure.into())?;
        let circuit = edit.finish()?;
        let mut next = 10;
        let merged = merge_temporal_filters(circuit.clone(), &mut next, &mut warnings)?.unwrap();
        assert_eq!(
            merged.node(residual).unwrap().inputs().next(),
            Some(port(cloned_filter))
        );

        // A mutable declaration with a clock initializer still supplies a bound.
        let mut edit = CircuitEditor::new(circuit);
        let binding = edit.let_statement("t", clock)?;
        let binding = edit.derive_inner(binding, [("mutable", Field::Data(true.into()))])?;
        let read = edit.named_variable(timestamp, "t")?;
        let block = edit.make_expr(
            site,
            Expr::Block,
            timestamp,
            [
                ("contents", Field::Array(vec![Field::Reference(binding)])),
                ("lastExpression", Field::Reference(read.into())),
            ],
        )?;
        let predicate = edit.binary_expression(site, boolean, BinaryOp::Gt, key, block)?;
        let closure = edit.closure_from_variables(predicate, &[variable])?;
        let mutable = filter(&mut edit, cloned_filter, closure.into())?;
        let circuit = edit.finish()?;
        let mut next = 10;
        let merged = merge_temporal_filters(circuit.clone(), &mut next, &mut warnings)?.unwrap();
        assert_eq!(
            merged.node(mutable).unwrap().inputs().next(),
            Some(port(source))
        );

        // A detached caller can replay the diagnostic with its original names and warnings.
        let mut edit = CircuitEditor::new(circuit);
        let missing = edit.inner_at(
            site,
            feldera_sql_ir::InnerKind::Other("DBSPLetStatement".into()),
            [
                ("variable", Field::Data("u".into())),
                ("mutable", Field::Data(true.into())),
                ("type", Field::Reference(timestamp.into())),
            ],
        )?;
        let read = edit.named_variable(timestamp, "u")?;
        let block = edit.derive_inner(
            block.into(),
            [
                (
                    "contents",
                    Field::Array(vec![Field::Reference(binding), Field::Reference(missing)]),
                ),
                ("lastExpression", Field::Reference(read.into())),
            ],
        )?;
        let block = edit.expression(block)?;
        let predicate = edit.binary_expression(site, boolean, BinaryOp::Gt, key, block)?;
        let closure = edit.closure_from_variables(predicate, &[variable])?;
        filter(&mut edit, mutable, closure.into())?;
        let mut next = 10;
        assert!(merge_temporal_filters(edit.finish()?, &mut next, &mut warnings)?.is_none());
        assert_eq!(next, 10);
        assert!(warnings.is_empty());
        Ok(())
    }
}
