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
