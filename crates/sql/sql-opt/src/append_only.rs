//! Classify streams whose updates only insert rows, before expanding aggregates.
use crate::recursive_views::metadata_field;
use feldera_sql_ir::{BodyOrder, Circuit, Error, NodeId, OperatorKind as Op};
use std::collections::BTreeSet;

/// The Java analysis marks the primary output of each qualifying operator.
/// A secondary or nested output has its own stream identity.
pub fn append_only_streams(circuit: &Circuit) -> Result<BTreeSet<NodeId>, Error> {
    let mut streams = BTreeSet::new();
    for id in circuit.operator_order(BodyOrder::Postorder)? {
        let node = circuit.node(id).expect("validated body member");
        let guaranteed = match node.operator_kind() {
            Some(Op::SourceMap | Op::SourceLazyMap | Op::SourceMultiset | Op::ViewDeclaration) => {
                match metadata_field(node, "changes")?.as_str() {
                    Some("AppendOnly") => true,
                    Some("Unrestricted") => false,
                    _ => return Err(Error::new("unknown table change mode")),
                }
            }
            Some(
                Op::Constant
                | Op::IncrementalConstant
                | Op::Map
                | Op::Filter
                | Op::Join
                | Op::StarJoin
                | Op::StarJoinIndex
                | Op::StarJoinFilterMap
                | Op::StreamJoin
                | Op::JoinFilterMap
                | Op::Deindex
                | Op::MapIndex
                | Op::Noop
                | Op::DelayedIntegral
                | Op::Integrate
                | Op::Delay
                | Op::Distinct
                | Op::Positive
                | Op::StreamDistinct
                | Op::Sum
                | Op::AtomicSum
                | Op::BinaryDistinct
                | Op::FlatMap
                | Op::View
                | Op::Sink
                | Op::Hop
                | Op::IndexedTopK
                | Op::Rank
                | Op::RowNumber,
            ) => node
                .inputs()
                .all(|port| port.number == 0 && streams.contains(&port.operator)),
            _ => false,
        };
        if guaranteed {
            streams.insert(id);
        }
    }
    Ok(streams)
}
