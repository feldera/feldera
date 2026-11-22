//! Ordered edges supplied by the live compiler to passes that do not inspect expressions.
use feldera_sql_ir::{NodeId, OperatorKind, OutputPort};
use serde::{Deserialize, Deserializer};

#[derive(Deserialize)]
pub struct OperatorInputs {
    pub(crate) body: u64,
    pub(crate) id: u64,
    #[serde(deserialize_with = "operator_kind")]
    pub(crate) kind: OperatorKind,
    pub(crate) inputs: Vec<(u64, usize)>,
    /// Recursive outputs retain their slot numbers when an internal stream is removed.
    #[serde(default)]
    pub(crate) internal_outputs: Vec<Option<(u64, usize)>>,
}

fn operator_kind<'de, D: Deserializer<'de>>(de: D) -> Result<OperatorKind, D::Error> {
    let name = <&str>::deserialize(de)?;
    OperatorKind::from_wire(name)
        .ok_or_else(|| serde::de::Error::custom(format!("unknown optimizer operator {name}")))
}

pub(crate) fn output_port((operator, number): (u64, usize)) -> OutputPort {
    OutputPort {
        operator: NodeId::Outer(operator),
        number,
    }
}
