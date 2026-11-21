//! Select bounded windows that can use the same indexed input. Java supplies
//! canonical closure text while it owns the expression printer.
mod implement;
mod reorder;
mod temporal;
pub use reorder::reorder_temporal_predicate;
use feldera_sql_ir::{
    BodyOrder, Circuit, Error, ExpressionKind as Expr, Field, InnerKind, NodeId, OperatorKind,
};
pub use implement::{implement_window_groups, WindowProjection};
pub use temporal::{
    shared_temporal_inputs, TemporalBound, TemporalCandidate,
};
use serde::Serialize;
use std::collections::HashMap;

/// The left input must index a stream by a key and a tuple of projected values.
pub fn window_candidates(circuit: &Circuit) -> Result<Vec<NodeId>, Error> {
    let mut candidates = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let window = circuit.node(id).expect("validated member");
        if window.operator_kind() != Some(OperatorKind::Window) {
            continue;
        }
        if window
            .data("lowerUnbounded")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| Error::new("window requires lowerUnbounded"))?
        {
            continue;
        }
        let input = window
            .inputs()
            .next()
            .ok_or_else(|| Error::new("window requires a data input"))?;
        let index = circuit.node(input.operator).expect("validated producer");
        if index.operator_kind() != Some(OperatorKind::MapIndex) {
            continue;
        }
        let closure = circuit
            .node(index.edge("function")?)
            .expect("validated function");
        if !matches!(
            closure.inner_kind(),
            Some(InnerKind::Expression(Expr::Closure))
        ) || closure.edges("parameters")?.len() != 1
        {
            continue;
        }
        let pair = circuit.node(closure.edge("body")?).expect("validated body");
        if !matches!(
            pair.inner_kind(),
            Some(InnerKind::Expression(Expr::RawTuple))
        ) {
            continue;
        }
        let Some(Field::Array(fields)) = pair.field("fields") else {
            continue;
        };
        let [_, value] = fields.as_slice() else {
            continue;
        };
        let value = circuit
            .node(
                value
                    .node_id()
                    .ok_or_else(|| Error::new("index value requires an expression"))?,
            )
            .expect("validated index value");
        if matches!(
            value.inner_kind(),
            Some(InnerKind::Expression(Expr::RawTuple | Expr::Tuple))
        ) && matches!(value.field("fields"), Some(Field::Array(_)))
        {
            candidates.push(id);
        }
    }
    Ok(candidates)
}

/// The key includes the upstream output port and canonical timestamp closure.
/// Both group order and member order follow the first use in circuit order.
pub fn qualifying_window_groups(inputs: &[String], threshold: usize) -> Vec<Vec<usize>> {
    if threshold == 0 {
        return Vec::new();
    }
    let mut positions = HashMap::new();
    let mut groups = Vec::<Vec<usize>>::new();
    for (window, input) in inputs.iter().enumerate() {
        let position = *positions.entry(input).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[position].push(window);
    }
    groups.retain(|members| members.len() >= threshold);
    groups
}

#[derive(Debug, PartialEq, Eq, Serialize)]
pub struct WindowFields {
    /// (Member, field) defining each column of the shared value.
    pub sources: Vec<(usize, usize)>,
    /// Shared column numbers in each window's original field order.
    pub projections: Vec<Vec<usize>>,
}

/// Deduplicate field computations only within one qualifying group.
pub fn shared_window_fields(members: &[Vec<String>]) -> WindowFields {
    let mut columns = HashMap::new();
    let mut sources = Vec::new();
    let projections = members
        .iter()
        .enumerate()
        .map(|(member, fields)| {
            fields
                .iter()
                .enumerate()
                .map(|(ordinal, field)| {
                    *columns.entry(field).or_insert_with(|| {
                        sources.push((member, ordinal));
                        sources.len() - 1
                    })
                })
                .collect()
        })
        .collect();
    WindowFields {
        sources,
        projections,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserve_first_use_with_interleaved_groups_and_repeated_fields() {
        let inputs = ["b", "a", "b", "orphan", "a"].map(str::to_owned);
        assert_eq!(
            qualifying_window_groups(&inputs, 2),
            [vec![0, 2], vec![1, 4]]
        );
        assert!(qualifying_window_groups(&inputs, 0).is_empty());
        let fields = [
            vec!["y".into(), "x".into(), "y".into()],
            vec![],
            vec!["z".into(), "x".into()],
        ];
        assert_eq!(
            shared_window_fields(&fields),
            WindowFields {
                sources: vec![(0, 0), (0, 1), (2, 0)],
                projections: vec![vec![0, 1, 0], vec![], vec![2, 1]]
            }
        );
    }
}
