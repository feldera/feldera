//! Forward view outputs after the consuming analyses have finished.
use feldera_sql_ir::{BodyOrder, Circuit, Error, Field, Node, NodeId, OperatorKind, OutputPort};
use std::collections::BTreeSet;

/// The first placement retains metadata used by recursive and temporal analyses.
/// The second runs after those analyses and can remove every view operator.
pub enum ViewRemoval<'a> {
    Ordinary { error_view: &'a str },
    All,
}

fn preserve(node: &Node, error_view: &str) -> Result<bool, Error> {
    let Some(Field::Object(metadata)) = node.field("metadata") else {
        return Err(Error::new("view requires metadata"));
    };
    let Some(Field::Data(recursive)) = metadata.get("recursive") else {
        return Err(Error::new("view requires a recursive flag"));
    };
    let Some(Field::Array(columns)) = metadata.get("columns") else {
        return Err(Error::new("view requires column metadata"));
    };
    if node.relation_name("viewName")? == error_view
        || recursive
            .as_bool()
            .ok_or_else(|| Error::new("invalid recursive flag"))?
    {
        return Ok(true);
    }
    for column in columns {
        let Field::Object(properties) = column else {
            return Err(Error::new("invalid view column"));
        };
        if properties
            .get("lateness")
            .and_then(Field::node_id)
            .is_some()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn remove_views(
    circuit: Circuit,
    mode: ViewRemoval<'_>,
    next: &mut u64,
) -> Result<Circuit, Error> {
    let mut removed = BTreeSet::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        if node.operator_kind() != Some(OperatorKind::View) {
            continue;
        }
        if let ViewRemoval::Ordinary { error_view } = mode {
            if preserve(node, error_view)? {
                continue;
            }
        }
        removed.insert(id);
    }
    crate::rebuild::rebuild_input_closures(circuit.bypass_unary_operators(&removed)?, next)
}

/// Sinks whose only input is a literal stream, possibly differentiated. The
/// compiler emits their warnings in visitation order with the live SQL positions.
pub fn constant_views(circuit: &Circuit, error_view: &str) -> Result<Vec<NodeId>, Error> {
    let mut constants = BTreeSet::new();
    let mut sinks = Vec::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated circuit member");
        let constant_input = || {
            node.inputs()
                .next()
                .is_some_and(|port| constants.contains(&port))
        };
        match node.operator_kind() {
            Some(OperatorKind::Constant | OperatorKind::IncrementalConstant) => {
                constants.insert(OutputPort {
                    operator: id,
                    number: 0,
                });
            }
            Some(OperatorKind::Differentiate) if constant_input() => {
                constants.insert(OutputPort {
                    operator: id,
                    number: 0,
                });
            }
            Some(OperatorKind::Sink)
                if constant_input() && node.relation_name("viewName")? != error_view =>
            {
                sinks.push(id);
            }
            _ => {}
        }
    }
    Ok(sinks)
}
