use feldera_sql_ir::{Circuit, CircuitEditor, NodeId};
use feldera_sql_opt::TemporalPredicate;
use serde_json::{Value, json};
use std::collections::HashSet;

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
