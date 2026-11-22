use super::ExpressionSite;
use feldera_sql_ir::{Circuit, CircuitEditor, NodeId, OperatorKind, OutputPort};
use serde_json::{Value, json};

/// A local operator sequence has one external port per live input, in call order.
pub(super) struct Fragment<I> {
    pub(super) base: Circuit,
    pub(super) inputs: I,
}

impl<const N: usize> Fragment<[OutputPort; N]> {
    pub(super) fn bind(
        site: ExpressionSite,
        kinds: [OperatorKind; N],
        bytes: &[u8],
    ) -> Result<(Self, Vec<NodeId>), Box<dyn std::error::Error>> {
        let inputs = std::array::from_fn(|i| OutputPort {
            operator: NodeId::Outer(i as u64 + 1),
            number: 0,
        });
        Self::with_inputs(site, inputs, kinds, bytes)
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
}

impl<I: AsRef<[OutputPort]>> Fragment<I> {
    pub(super) fn with_inputs(
        site: ExpressionSite,
        inputs: I,
        kinds: impl IntoIterator<Item = OperatorKind>,
        bytes: &[u8],
    ) -> Result<(Self, Vec<NodeId>), Box<dyn std::error::Error>> {
        let headers: Vec<_> = kinds.into_iter().zip(inputs.as_ref()).map(|(kind, port)| json!({
            "class":kind.wire_name(), "id":super::operator_number(port.operator), "inputs":[]
        })).collect();
        let (base, roots) = site.arena_with_operators(bytes, &headers)?;
        Ok((Self { base, inputs }, roots))
    }

    pub(super) fn reply(
        &self,
        edit: &CircuitEditor,
        next: u64,
    ) -> Result<Value, Box<dyn std::error::Error>> {
        let order = edit.circuit().root().edges("allOperators")?;
        let inputs: Vec<_> = self
            .inputs
            .as_ref()
            .iter()
            .map(|port| port.operator)
            .collect();
        let added = order
            .strip_prefix(inputs.as_slice())
            .ok_or("operator fragment changed an external input")?;
        let operators = edit.circuit().additional_operators(&self.base, added)?;
        Ok(
            json!({"inputs":inputs.into_iter().map(super::operator_number).collect::<Vec<_>>(), "operators":operators, "next_generated_name":next}),
        )
    }
}
