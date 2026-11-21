//! Share the distinct output columns of sibling maps identified by the expensive-expression analysis.
mod fragments;
mod groups;
mod references;

pub use fragments::map_fusion_fragments;
pub use groups::map_fusion_groups;

use crate::{rebuild::ProjectionNames, stream_rewrites::body_members, terms::Terms};
use feldera_sql_expr::{reduce_closure_body_with_warnings, SimplifyWarning};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind, Node,
    NodeId, OperatorKind, OutputPort,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};

fn expression_children(node: &Node) -> Vec<NodeId> {
    let mut children = Vec::new();
    if !matches!(node.inner_kind(), Some(InnerKind::Type(_))) {
        for (name, field) in node.fields() {
            if !matches!(name, "type" | "originalStruct") {
                field.collect_inner_edges(&mut children);
            }
        }
    }
    children
}

/// The analysis simplifies each member's closure before deciding which siblings to fuse.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FusionMember {
    operator: u64,
    function: Value,
}

fn append_map(
    edit: &mut CircuitEditor,
    body: NodeId,
    site: NodeId,
    input: OutputPort,
    value: ExpressionId,
    variable: ExpressionId,
) -> Result<OutputPort, Error> {
    let function = edit.closure_from_variables(value, &[variable])?;
    let collection = edit.zset_type(edit.expression_type(value)?)?;
    let operator = edit.append_operator(
        body,
        site,
        OperatorKind::Map,
        &[input],
        [
            ("outputType", Field::Reference(collection.into())),
            ("function", Field::Reference(function.into())),
        ],
    )?;
    Ok(OutputPort {
        operator,
        number: 0,
    })
}

/// Assign each equivalent field one column, in order of first use. Every member
/// is evaluated with the same parameter, so its fields share a lexical scope.
fn collect_columns(
    edit: &mut CircuitEditor,
    members: &[(NodeId, NodeId)],
    row: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<(Vec<ExpressionId>, Vec<Vec<usize>>), Error> {
    let mut terms = Terms::default();
    let mut columns = Vec::new();
    let mut indices = HashMap::new();
    let mut projections = Vec::with_capacity(members.len());
    let name = edit
        .node(row.into())?
        .data("variable")
        .and_then(Value::as_str)
        .ok_or_else(|| Error::new("missing fused map parameter name"))?
        .to_owned();
    for &(_, function) in members {
        let closure = edit.node(function)?;
        let reduced = reduce_closure_body_with_warnings(
            edit,
            closure.edge("body")?,
            &closure.edges("parameters")?,
            &[row.into()],
            Some(warnings),
        )?;
        let tuple = edit.node(reduced)?;
        if !matches!(
            tuple.inner_kind(),
            Some(InnerKind::Expression(ExpressionKind::Tuple))
        ) {
            return Err(Error::new("fused map must return a SQL tuple"));
        }
        let mut projection = Vec::new();
        for field in tuple.edges("fields")? {
            let shape = terms.field(
                edit.circuit(),
                &Field::Reference(field),
                std::slice::from_ref(&name),
            );
            let expression = edit.expression(field)?;
            let index = *indices.entry(shape).or_insert_with(|| {
                let index = columns.len();
                columns.push(expression);
                index
            });
            projection.push(index);
        }
        projections.push(projection);
    }
    Ok((columns, projections))
}

fn fuse_cluster(
    edit: &mut CircuitEditor,
    body: NodeId,
    members: &[(NodeId, NodeId)],
    next: &mut u64,
    replacements: &mut BTreeMap<NodeId, OutputPort>,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<(), Error> {
    let (first, function) = members[0];
    let [parameter] = edit.node(function)?.edges("parameters")?[..] else {
        return Err(Error::new("fused map requires one input parameter"));
    };
    let input = edit
        .node(first)?
        .inputs()
        .next()
        .ok_or_else(|| Error::new("fused map has no input"))?;
    let parameter_type = edit.type_id(edit.node(parameter)?.edge("type")?)?;
    let row = edit.variable(parameter_type, next)?;
    let (columns, projections) = collect_columns(edit, members, row, warnings)?;
    let fused_tuple = edit.tuple_expr(row.into(), false, &columns)?;
    let fused = append_map(edit, body, first, input, fused_tuple, row)?;
    for (&(member, _), indices) in members.iter().zip(projections) {
        let operator = edit.node(member)?;
        if operator.operator_kind() != Some(OperatorKind::Map) || !operator.inputs().eq([input]) {
            return Err(Error::new("fused maps must share an input port"));
        }
        let output = edit
            .node(operator.edge("outputType")?)?
            .edge("elementType")?;
        let reference = edit.reference_type(edit.expression_type(fused_tuple)?)?;
        let row = edit.variable(reference, next)?;
        let mut fields = Vec::with_capacity(indices.len());
        for index in indices {
            let value = edit.dereference(row)?;
            let field = edit.tuple_field(value, index)?;
            fields.push(edit.clone_if_needed(field)?);
        }
        let tuple = edit.tuple_with_type_at(member, edit.type_id(output)?, &fields)?;
        let projected = append_map(edit, body, member, fused, tuple, row)?;
        edit.annotate(
            projected.operator,
            json!({"class":"IsProjection","outputSize":columns.len()}),
        )?;
        if replacements.insert(member, projected).is_some() {
            return Err(Error::new("map occurs in more than one fusion cluster"));
        }
    }
    edit.place_before(body, fused.operator, first)
}

pub fn fuse_expensive_maps(
    circuit: Circuit,
    clusters: Vec<Vec<FusionMember>>,
    next: &mut u64,
) -> Result<(Circuit, Vec<SimplifyWarning>), Error> {
    let mut warnings = Vec::new();
    if clusters.is_empty() {
        return Ok((circuit, warnings));
    }
    // Decode the additional closures together. They can share circuit nodes and one another's types.
    let mut roots = Vec::new();
    let mut groups = Vec::with_capacity(clusters.len());
    for cluster in clusters {
        if cluster.is_empty() {
            return Err(Error::new("empty map fusion cluster"));
        }
        groups.push(
            cluster
                .into_iter()
                .map(|member| {
                    roots.push(member.function);
                    NodeId::Outer(member.operator)
                })
                .collect::<Vec<_>>(),
        );
    }
    let (circuit, functions) = circuit.with_inner_roots(roots)?;
    let mut functions = functions.into_iter();
    let mut plans = HashMap::new();
    let bodies: HashMap<_, _> = body_members(&circuit)?
        .into_iter()
        .map(|(body, id)| (id, body))
        .collect();
    for group in groups {
        let first = group[0];
        let body = bodies
            .get(&first)
            .ok_or_else(|| Error::new("unknown map fusion root"))?;
        if group.iter().any(|id| bodies.get(id) != Some(body)) {
            return Err(Error::new(
                "fused maps must belong to the same circuit body",
            ));
        }
        let count = group.len();
        let members = group
            .into_iter()
            .zip(functions.by_ref().take(count))
            .collect::<Vec<_>>();
        if plans.insert(first, members).is_some() {
            return Err(Error::new("duplicate map fusion root"));
        }
    }
    let order = circuit.operator_order(BodyOrder::Postorder)?;
    let mut edit = CircuitEditor::new(circuit);
    let mut replacements = BTreeMap::new();
    let mut projections = ProjectionNames::default();
    let mut names = *next;
    for id in order {
        projections.visit(id, edit.node(id)?, &mut names)?;
        if let Some(members) = plans.remove(&id) {
            fuse_cluster(
                &mut edit,
                bodies[&id],
                &members,
                &mut names,
                &mut replacements,
                &mut warnings,
            )?;
            for (member, _) in members {
                projections.replaced(member);
            }
        }
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    Ok((circuit, warnings))
}
