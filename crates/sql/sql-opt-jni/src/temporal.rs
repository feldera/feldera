use super::ExpressionSite;
use feldera_sql_ir::{Circuit, CircuitEditor, NodeId, OperatorKind, OutputPort};
use feldera_sql_opt::TemporalPredicate;
use serde_json::{Value, json};
use std::collections::HashSet;

/// A local operator sequence has one external port per live input, in call order.
struct Fragment<const N: usize> {
    base: Circuit,
    inputs: [OutputPort; N],
}

impl<const N: usize> Fragment<N> {
    fn bind(
        site: ExpressionSite,
        kinds: [OperatorKind; N],
        bytes: &[u8],
    ) -> Result<(Self, Vec<NodeId>), Box<dyn std::error::Error>> {
        let inputs = std::array::from_fn(|i| OutputPort {
            operator: NodeId::Outer(i as u64 + 1),
            number: 0,
        });
        let headers: Vec<_> = kinds.into_iter().zip(inputs).map(|(kind, port)| json!({
            "class":kind.wire_name(), "id":super::operator_number(port.operator), "inputs":[]
        })).collect();
        let (base, roots) = site.arena_with_operators(bytes, &headers)?;
        Ok((Self { base, inputs }, roots))
    }

    fn reply(&self, edit: &CircuitEditor, next: u64) -> Result<Value, Box<dyn std::error::Error>> {
        let order = edit.circuit().root().edges("allOperators")?;
        let inputs = self.inputs.map(|port| port.operator);
        let added = order
            .strip_prefix(inputs.as_slice())
            .ok_or("temporal rewrite changed an external input")?;
        let operators = edit.circuit().additional_operators(&self.base, added)?;
        Ok(
            json!({"inputs":inputs.map(super::operator_number).as_slice(), "operators":operators, "next_generated_name":next}),
        )
    }
}

/// Bind the clock stream as an external port. Its upstream circuit and
/// Calcite origin remain live in the calling JVM.
pub(super) fn scalar_clock(
    input: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    let site = ExpressionSite {
        empty: true,
        position: None,
    };
    let (fragment, roots) = Fragment::bind(site, [OperatorKind::Now], input)?;
    let [collection, empty_key, minimum] = roots.as_slice() else {
        return Err(
            "scalar clock needs a clock collection, an empty key and an initial timestamp".into(),
        );
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let collection = edit.type_id(*collection)?;
    let empty_key = edit.type_id(*empty_key)?;
    let minimum = edit.expression(*minimum)?;
    feldera_sql_opt::scalar_clock_operators(
        &mut edit,
        fragment.base.root_id(),
        fragment.inputs[0],
        collection,
        empty_key,
        minimum,
        &mut next,
    )?;
    fragment.reply(&edit, next)
}

pub(super) fn window(
    input: &[u8],
    state: &[u8],
    mut next: u64,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::limiters::{Bound, Stream, StreamKind};
    use feldera_sql_opt::{InitialBounds, TemporalStreams, WindowComparison, WindowKey};
    #[derive(serde::Deserialize)]
    struct Request {
        #[serde(flatten)]
        site: super::ExpressionSite,
        comparisons: Vec<WindowComparison>,
    }
    let request: Request = serde_json::from_slice(state)?;
    let (fragment, roots) = Fragment::bind(
        request.site,
        [OperatorKind::SourceMultiset, OperatorKind::Waterline],
        input,
    )?;
    let [
        parameter,
        collection,
        scalar_type,
        common,
        key,
        minimum_clock,
        lower,
        upper,
        operands @ ..,
    ] = roots.as_slice()
    else {
        return Err("missing temporal window input".into());
    };
    if request.comparisons.len() != operands.len() {
        return Err("expected one expression for each temporal comparison".into());
    }
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let streams = TemporalStreams {
        rows: Stream {
            port: fragment.inputs[0],
            ty: edit.type_id(*collection)?,
            kind: StreamKind::Collection,
        },
        clock: Bound {
            port: fragment.inputs[1],
            ty: edit.type_id(*scalar_type)?,
        },
    };
    let row = edit.type_id(edit.node(*collection)?.edge("elementType")?)?;
    let key = WindowKey {
        parameter: *parameter,
        row,
        expression: edit.expression(*common)?,
        nonnull_type: edit.type_id(*key)?,
    };
    let key_type = key.nonnull_type;
    let initial = InitialBounds {
        minimum_clock: edit.expression(*minimum_clock)?,
        lower: edit.expression(*lower)?,
        upper: edit.expression(*upper)?,
    };
    let values = operands
        .iter()
        .map(|&id| edit.expression(id))
        .collect::<Result<Vec<_>, _>>()?;
    let functions = feldera_sql_opt::temporal_window(
        &mut edit,
        key,
        initial,
        request.comparisons.into_iter().zip(values),
        &mut next,
    )?;
    let body = fragment.base.root_id();
    feldera_sql_opt::append_temporal_window(
        &mut edit, body, body, streams, key_type, functions, &mut next,
    )?;
    fragment.reply(&edit, next)
}

pub(super) fn clock_join(
    input: &[u8],
    mut next: u64,
    consumer: feldera_sql_opt::ClockConsumer,
) -> Result<Value, Box<dyn std::error::Error>> {
    let site = ExpressionSite {
        empty: true,
        position: None,
    };
    let (fragment, roots) = Fragment::bind(
        site,
        [OperatorKind::SourceMultiset, OperatorKind::MapIndex],
        input,
    )?;
    let [row, key, timestamp, function, output] = roots.as_slice() else {
        return Err(
            "clock join requires a row, an empty key, a timestamp, a closure and an output type"
                .into(),
        );
    };
    let mut edit = CircuitEditor::new(fragment.base.clone());
    let body = fragment.base.root_id();
    let [rows, indexed_clock] = fragment.inputs;
    let inputs = feldera_sql_opt::ClockJoinInputs {
        rows,
        indexed_clock,
        row_type: edit.type_id(*row)?,
    };
    let key = edit.type_id(*key)?;
    let timestamp = edit.type_id(*timestamp)?;
    let join = feldera_sql_opt::append_clock_join(
        &mut edit, body, body, inputs, key, timestamp, &mut next,
    )?;
    let projection = feldera_sql_opt::ClockProjection {
        consumer,
        function: edit.expression(*function)?,
        output: edit.type_id(*output)?,
    };
    match feldera_sql_opt::apply_joined_clock(
        &mut edit, body, join, projection, timestamp, &mut next,
    )? {
        feldera_sql_opt::ClockRewrite::Rewritten(_) => fragment.reply(&edit, next),
        feldera_sql_opt::ClockRewrite::UnsupportedLambda(lambda) => {
            let functions = edit
                .circuit()
                .additional_expressions(&fragment.base, &[lambda])?;
            Ok(json!({"unsupported_lambda": functions[0], "next_generated_name": next}))
        }
    }
}

/// Encode all predicate roots together so consecutive residuals can share types.
pub(super) fn classify(
    input: &Circuit,
    edit: &mut CircuitEditor,
    body: NodeId,
    state: &[u8],
) -> Result<Value, Box<dyn std::error::Error>> {
    let ids: Vec<u64> = serde_json::from_slice(state)?;
    let monotone = ids
        .into_iter()
        .map(|id| edit.expression(NodeId::Inner(id)))
        .collect::<Result<HashSet<_>, _>>()?;
    let body = edit.expression(body)?;
    let parts = feldera_sql_opt::classify_temporal_predicates(edit, body, &monotone)?;
    let mut roots = Vec::new();
    let mut headers = Vec::with_capacity(parts.len());
    for part in parts {
        let (kind, expression, clock) = match part {
            TemporalPredicate::NoNow(expr) => (json!("NoNow"), expr, None),
            TemporalPredicate::Residual(expr) => (json!("Residual"), expr, None),
            TemporalPredicate::Bound {
                key,
                clock,
                comparison,
            } => (json!(comparison), key, Some(clock)),
        };
        roots.push(expression);
        roots.extend(clock);
        headers.push((kind, clock.is_some()));
    }

    let mut expressions = edit
        .circuit()
        .additional_expressions(input, &roots)?
        .into_iter();
    let mut next = || expressions.next().expect("one encoded expression per root");
    Ok(Value::Array(
        headers
            .into_iter()
            .map(|(kind, has_clock)| {
                let mut part = json!({"kind":kind, "expression":next()});
                if has_clock {
                    part["clock"] = next();
                }
                part
            })
            .collect(),
    ))
}

/// A bound supplies its declaring parameter and key. A residual supplies just
/// its Boolean expression. Consecutive roots share the request's node table.
pub(super) fn groups(
    input: &Circuit,
    edit: &mut CircuitEditor,
    roots: &[NodeId],
    state: &[u8],
    merge: bool,
) -> Result<Value, Box<dyn std::error::Error>> {
    use feldera_sql_opt::{TemporalBoundKey, WindowTerm};
    let kinds: Vec<String> = serde_json::from_slice(state)?;
    let mut roots = roots.iter().copied();
    let mut terms = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let root = roots.next().ok_or("missing temporal predicate")?;
        terms.push(match kind.as_str() {
            "NoNow" => WindowTerm::NoNow(edit.expression(root)?),
            "Residual" => WindowTerm::Residual(edit.expression(root)?),
            _ => WindowTerm::Bound(TemporalBoundKey {
                comparison: serde_json::from_value(Value::String(kind))?,
                parameter: root,
                key: edit.expression(roots.next().ok_or("missing temporal comparison key")?)?,
            }),
        });
    }
    if roots.next().is_some() {
        return Err("extra temporal predicate roots".into());
    }
    if merge {
        let (target, upstream) = terms
            .split_last()
            .ok_or("missing downstream temporal filter")?;
        return Ok(serde_json::to_value(
            feldera_sql_opt::accepts_temporal_bound(edit, upstream, *target)?,
        )?);
    }
    let Some(groups) = feldera_sql_opt::group_temporal_filters(edit, &terms)? else {
        return Ok(Value::Null);
    };
    let expressions = groups
        .iter()
        .filter_map(|g| g.predicate)
        .collect::<Vec<_>>();
    let mut encoded = edit
        .circuit()
        .additional_expressions(input, &expressions)?
        .into_iter();
    Ok(Value::Array(
        groups
            .into_iter()
            .map(|g| {
                let expression = g
                    .predicate
                    .map(|_| encoded.next().expect("one expression per residual group"));
                json!({"first":g.first,"count":g.count,"expression":expression})
            })
            .collect(),
    ))
}
