use super::ExpressionSite;
use feldera_sql_ir::{Circuit, CircuitEditor, NodeId, OperatorKind, OutputPort};
use serde_json::{Value, json};

/// A local operator sequence has one external port per live input, in call order.
pub(super) struct Fragment<const N: usize> {
    pub(super) base: Circuit,
    pub(super) inputs: [OutputPort; N],
}

impl<const N: usize> Fragment<N> {
    pub(super) fn bind(
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

    pub(super) fn at_empty_site(
        kinds: [OperatorKind; N],
        input: &[u8],
    ) -> Result<(Self, Vec<NodeId>), Box<dyn std::error::Error>> {
        Self::bind(
            ExpressionSite {
                empty: true,
                position: None,
            },
            kinds,
            input,
        )
    }

    pub(super) fn reply(
        &self,
        edit: &CircuitEditor,
        next: u64,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let order = edit.circuit().root().edges("allOperators")?;
        let inputs = self.inputs.map(|port| port.operator);
        let added = order
            .strip_prefix(inputs.as_slice())
            .ok_or("operator fragment changed an external input")?;
        let operators = edit.circuit().additional_operators(&self.base, added)?;
        Ok(
            json!({"inputs":inputs.map(super::operator_number).as_slice(), "operators":operators, "next_generated_name":next}),
        )
    }
}
