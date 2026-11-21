//! Replace retainers of the same trace with one policy that admits the union of their rows.

use super::{can_merge, same_policy, Terms};
use crate::{
    rebuild::{input_type, resolve_output, ProjectionNames},
    stream_rewrites::body_members,
    trace_retention::retention_slot,
};
use feldera_sql_expr::{copy_expression, reduce_closure_body};
use feldera_sql_ir::{
    BinaryOp, BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, Field, NodeId,
    OperatorKind as Op, OutputPort, TypeId,
};
use std::collections::{BTreeMap, BTreeSet, HashMap};

fn control_type(edit: &mut CircuitEditor, port: OutputPort) -> Result<TypeId, Error> {
    let port = resolve_output(edit, port)?;
    let node = edit.node(port.operator)?;
    if port.number == 2 && node.operator_kind() == Some(Op::InputMapWithWaterline) {
        let closure = edit.expression(node.edge("lub")?)?;
        let ty = edit
            .node(edit.expression_type(closure)?.into())?
            .edge("resultType")?;
        return edit.dynamic_box_type(edit.type_id(ty)?);
    }
    input_type(edit, port)
}

/// ApplyN takes homogeneous inputs. Apply2 can extend a tuple with a bound of
/// any type. Flatten only the tuple constructed by the preceding Apply2.
fn pack_controls(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    kind: Op,
    inputs: &[OutputPort],
    prefix_fields: usize,
    next: &mut u64,
) -> Result<OutputPort, Error> {
    let mut variables = Vec::with_capacity(inputs.len());
    let mut fields = Vec::new();
    for (i, &input) in inputs.iter().enumerate() {
        let ty = control_type(edit, input)?;
        let reference = edit.reference_type(ty)?;
        let variable = edit.variable(reference, next)?;
        if i == 0 && prefix_fields > 0 {
            for field in 0..prefix_fields {
                let read = edit.dereference(variable)?;
                let value = edit.tuple_field(read, field)?;
                fields.push(edit.clone_if_needed(value)?);
            }
        } else {
            let value = edit.dereference(variable)?;
            fields.push(edit.clone_if_needed(value)?);
        }
        variables.push(variable);
    }
    let tuple = edit.tuple_expr(site, false, &fields)?;
    let ty = edit.expression_type(tuple)?;
    let function = edit.closure_from_variables(tuple, &variables)?;
    let operator = edit.append_operator(
        body,
        site,
        kind,
        inputs,
        [
            ("outputType", Field::Reference(ty.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    Ok(OutputPort {
        operator,
        number: 0,
    })
}

fn combine_controls(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    controls: &[OutputPort],
    next: &mut u64,
) -> Result<(NodeId, OutputPort), Error> {
    let first_type = control_type(edit, controls[0])?;
    for &control in &controls[1..] {
        let ty = control_type(edit, control)?;
        if !edit.same_type(first_type, ty)? {
            let mut current = pack_controls(edit, body, site, Op::Apply2, &controls[..2], 0, next)?;
            let first = current.operator;
            for (i, &next_control) in controls.iter().enumerate().skip(2) {
                current = pack_controls(
                    edit,
                    body,
                    site,
                    Op::Apply2,
                    &[current, next_control],
                    i,
                    next,
                )?;
            }
            return Ok((first, current));
        }
    }
    let packed = pack_controls(edit, body, site, Op::ApplyN, controls, 0, next)?;
    Ok((packed.operator, packed))
}

fn combined_predicate(
    edit: &mut CircuitEditor,
    policies: &[NodeId],
    control: OutputPort,
    next: &mut u64,
) -> Result<ExpressionId, Error> {
    let first = edit.node(policies[0])?.edge("function")?;
    let row_param = *edit
        .node(first)?
        .edges("parameters")?
        .first()
        .ok_or_else(|| Error::new("retention predicate requires a row parameter"))?;
    let row_type = edit.type_id(edit.node(row_param)?.edge("type")?)?;
    let row = edit.variable(row_type, next)?;
    let control_type = input_type(edit, control)?;
    let reference = edit.reference_type(control_type)?;
    let bounds = edit.variable(reference, next)?;
    let mut conditions = Vec::with_capacity(policies.len());
    for (index, &id) in policies.iter().enumerate() {
        let function = edit.node(edit.node(id)?.edge("function")?)?;
        let params = function.edges("parameters")?;
        let predicate = function.edge("body")?;
        let read_bounds = copy_expression(edit, bounds.into())?;
        let tuple = edit.dereference(edit.expression(read_bounds)?)?;
        let field = edit.tuple_field(tuple, index)?;
        let bound = edit.borrow_value(field)?;
        let row = copy_expression(edit, row.into())?;
        let condition = reduce_closure_body(edit, predicate, &params, &[row, bound.into()])?;
        conditions.push(edit.expression(condition)?);
    }
    let ty = edit.expression_type(conditions[0])?;
    let any = edit.binary_expressions(policies[0], ty, BinaryOp::Or, &conditions)?;
    edit.closure_from_variables(any, &[row, bounds])
}

fn merge_group(
    edit: &mut CircuitEditor,
    body: NodeId,
    group: &[NodeId],
    last: NodeId,
    next: &mut u64,
    terms: &mut Terms,
) -> Result<NodeId, Error> {
    let first = group[0];
    let leader = edit.node(first)?;
    let kind = leader
        .operator_kind()
        .filter(|k| retention_slot(Some(*k)).is_some())
        .ok_or_else(|| Error::new("expected a retention policy"))?;
    let data = leader
        .inputs()
        .next()
        .ok_or_else(|| Error::new("missing trace input"))?;
    let mut distinct = Vec::new();
    let mut controls = Vec::new();
    for &id in group {
        let policy = edit.node(id)?;
        if policy.inputs().next() != Some(data) || !can_merge(edit.circuit(), first, id, terms)? {
            return Err(Error::new("incompatible trace retention policies"));
        }
        let mut duplicate = false;
        for &kept in &distinct {
            if same_policy(edit.circuit(), edit.node(kept)?, policy, terms)? {
                duplicate = true;
                break;
            }
        }
        if !duplicate {
            distinct.push(id);
            controls.push(
                policy
                    .inputs()
                    .nth(1)
                    .ok_or_else(|| Error::new("missing retention bound"))?,
            );
        }
    }
    if distinct.len() == 1 {
        if first != last {
            edit.move_before(first, last)?;
        }
        return Ok(first);
    }
    let (start, control) = combine_controls(edit, body, first, &controls, next)?;
    let predicate = combined_predicate(edit, &distinct, control, next)?;
    let mut fields = vec![
        (
            "outputType",
            Field::Reference(input_type(edit, data)?.into()),
        ),
        ("function", Field::Reference(predicate.into())),
    ];
    if kind == Op::IntegrateTraceRetainKeys {
        fields.push((
            "accumulate",
            edit.node(first)?
                .field("accumulate")
                .ok_or_else(|| Error::new("missing accumulation mode"))?
                .clone(),
        ));
    }
    let merged = edit.append_operator(body, first, kind, &[data, control], fields)?;
    edit.place_before(body, start, last)?;
    Ok(merged)
}

/// The last visited retainer is the first legal emission point for all bounds.
/// Groups follow successor order, which also chooses the first of equal policies.
pub fn merge_retention_policies(
    circuit: Circuit,
    groups: &[Vec<u64>],
    next: &mut u64,
) -> Result<Circuit, Error> {
    if groups.is_empty() {
        return Ok(circuit);
    }
    let owners: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    let groups: Vec<Vec<_>> = groups
        .iter()
        .map(|g| g.iter().copied().map(NodeId::Outer).collect())
        .collect();
    let mut pending = HashMap::new();
    for (index, group) in groups.iter().enumerate() {
        if group.len() < 2 {
            return Err(Error::new(
                "merging retention requires at least two policies",
            ));
        }
        for &id in group {
            if !owners.contains_key(&id)
                || owners[&group[0]] != owners[&id]
                || pending.insert(id, index).is_some()
            {
                return Err(Error::new(
                    "expected disjoint retention groups within one circuit body",
                ));
            }
        }
    }
    let mut remaining: Vec<_> = groups.iter().map(Vec::len).collect();
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut terms = Terms::default();
    let mut names = ProjectionNames::default();
    let mut removed = BTreeSet::new();
    let mut forwarded = BTreeMap::new();
    for id in order {
        let Some(&index) = pending.get(&id) else {
            names.visit(id, edit.node(id)?, next)?;
            continue;
        };
        remaining[index] -= 1;
        if remaining[index] == 0 {
            let group = &groups[index];
            let replacement = merge_group(&mut edit, owners[&id], group, id, next, &mut terms)?;
            removed.extend(
                group
                    .iter()
                    .copied()
                    .filter(|&member| member != replacement && member != id),
            );
            if replacement != id {
                forwarded.insert(
                    id,
                    OutputPort {
                        operator: replacement,
                        number: 0,
                    },
                );
                names.replaced(id);
            }
        }
    }
    edit.remove_operators(&removed)?;
    names.finish(edit.finish_forwarding(&forwarded)?)
}
