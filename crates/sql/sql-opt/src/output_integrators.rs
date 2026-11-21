//! Bypass a key-preserving index when the join can read the producer's integrated output.
use crate::{identity::identity_closure, rebuild::rebuild_input_closures};
use feldera_sql_expr::reduce_closure_body;
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};

fn join(kind: Option<Op>) -> bool {
    matches!(
        kind,
        Some(
            Op::Join
                | Op::JoinIndex
                | Op::JoinFilterMap
                | Op::LeftJoin
                | Op::LeftJoinIndex
                | Op::LeftJoinFilterMap
                | Op::StreamJoin
                | Op::StreamJoinIndex
        )
    )
}

fn integrated_index(circuit: &Circuit, port: OutputPort) -> Option<OutputPort> {
    let index = circuit.node(port.operator)?;
    if index.operator_kind() != Some(Op::MapIndex) {
        return None;
    }
    let source = index.inputs().next()?;
    matches!(
        circuit.node(source.operator)?.operator_kind(),
        Some(
            Op::Aggregate
                | Op::ChainAggregate
                | Op::AggregateLinearPostprocess
                | Op::AggregateLinearPostprocessRetainKeys
                | Op::Lag
                | Op::IndexedTopK
                | Op::RowNumber
                | Op::Rank
                | Op::Integrate
                | Op::PartitionedRollingAggregate
                | Op::PartitionedRollingAggregateWithWaterline
        )
    )
    .then_some(source)
}

pub(super) fn has_join_index(circuit: &Circuit) -> Result<bool, Error> {
    Ok(circuit
        .operator_order(BodyOrder::Postorder)?
        .into_iter()
        .any(|id| {
            let operator = circuit.node(id).expect("validated operator");
            join(operator.operator_kind())
                && operator
                    .inputs()
                    .any(|p| integrated_index(circuit, p).is_some())
        }))
}

struct Index {
    source: OutputPort,
    parameter: NodeId,
    body: NodeId,
    value_type: TypeId,
}

impl Index {
    fn read(
        edit: &mut CircuitEditor,
        port: OutputPort,
        next: &mut u64,
    ) -> Result<Option<Self>, Error> {
        let Some(source) = integrated_index(edit.circuit(), port) else {
            return Ok(None);
        };
        let index = edit.node(port.operator)?;
        let stream = edit.node(index.edge("outputType")?)?;
        if edit.nullable(edit.type_id(stream.edge("elementType")?)?)? {
            return Ok(None);
        }
        let closure = edit.node(index.edge("function")?)?;
        let [parameter] = closure
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("index requires one pair parameter"))?;
        let body = closure.edge("body")?;
        let pair = edit.node(edit.node(parameter)?.edge("type")?)?;
        let [key_type, value_type] = pair
            .edges("tupFields")?
            .try_into()
            .map_err(|_| Error::new("index parameter must contain key and value"))?;
        let value_type = edit.type_id(value_type)?;
        let key = edit.variable(edit.type_id(key_type)?, next)?;
        let unused = edit.make_expr(parameter, ExpressionKind::NoExpression, value_type, [])?;
        let index = Self {
            source,
            parameter,
            body,
            value_type,
        };
        let projected = index.project(edit, key, unused, 0)?;
        let closure = edit.closure_from_variables(projected, &[key])?;
        Ok(identity_closure(edit, closure.into(), next)?.then_some(index))
    }

    fn project(
        &self,
        edit: &mut CircuitEditor,
        key: ExpressionId,
        value: ExpressionId,
        column: usize,
    ) -> Result<ExpressionId, Error> {
        let pair = edit.tuple_expr(self.body, true, &[key, value])?;
        let row = reduce_closure_body(edit, self.body, &[self.parameter], &[pair.into()])?;
        edit.tuple_field(edit.expression(row)?, column)
    }
}

pub fn share_output_integrators(circuit: Circuit, next: &mut u64) -> Result<Circuit, Error> {
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut names = *next;
    for id in order {
        let operator = edit.node(id)?;
        if !join(operator.operator_kind()) {
            continue;
        }
        let mut inputs: [OutputPort; 2] = operator
            .inputs()
            .collect::<Vec<_>>()
            .try_into()
            .map_err(|_| Error::new("join requires two inputs"))?;
        let function = operator.edge("function")?;
        let indexes = [
            Index::read(&mut edit, inputs[0], &mut names)?,
            Index::read(&mut edit, inputs[1], &mut names)?,
        ];
        if indexes.iter().all(Option::is_none) {
            continue;
        }
        let function = edit.node(function)?;
        let body = function.edge("body")?;
        let params: [NodeId; 3] = function
            .edges("parameters")?
            .try_into()
            .map_err(|_| Error::new("join closure requires a key and two values"))?;
        let mut vars = Vec::with_capacity(3);
        for param in params {
            let ty = edit.type_id(edit.node(param)?.edge("type")?)?;
            vars.push(edit.variable(ty, &mut names)?);
        }
        let mut args: Vec<_> = vars.iter().copied().map(NodeId::from).collect();
        for (slot, index) in indexes.into_iter().enumerate() {
            if let Some(index) = index {
                let var = edit.variable(index.value_type, &mut names)?;
                let value = index.project(&mut edit, vars[0], var, 1)?;
                let borrowed = edit.borrow_value(value)?;
                args[slot + 1] = reduce_closure_body(&mut edit, borrowed.into(), &[], &[])?;
                vars[slot + 1] = var;
                inputs[slot] = index.source;
            }
        }
        let body = reduce_closure_body(&mut edit, body, &params, &args)?;
        let closure = edit.closure_from_variables(edit.expression(body)?, &vars)?;
        edit.set_operator_field(id, "function", Field::Reference(closure.into()))?;
        edit.rewire_inputs(id, &inputs)?;
    }
    let circuit = rebuild_input_closures(edit.finish_with_rewrites()?, &mut names)?;
    *next = names;
    Ok(circuit)
}
