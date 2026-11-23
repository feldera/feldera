//! Share the distinct output columns of sibling maps identified by the expensive-expression analysis.
mod fragments;
mod groups;

pub use fragments::{has_duplicate_expensive_field, map_fusion_candidate, map_fusion_fragments};
pub use groups::{
    FusionCandidate, FusionScan, map_fusion_groups, scan_map_fusion, sibling_map_groups,
};

use crate::{
    index_key_equivalence::compare_expressions, rebuild::ProjectionNames,
    stream_rewrites::body_members,
};
use feldera_sql_expr::{SimplifyWarning, reduce_closure_body_with_warnings};
use feldera_sql_ir::{
    BodyOrder, Circuit, CircuitEditor, Error, ExpressionId, ExpressionKind, Field, InnerKind,
    NodeId, OperatorKind, OutputPort,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};

/// The analysis simplifies each member's closure before deciding which siblings to fuse.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FusionMember {
    operator: u64,
    function: Value,
    #[serde(default, rename = "expressionDefinitions")]
    definitions: Vec<Value>,
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

#[derive(Default)]
struct FusionColumns {
    fields: Vec<ExpressionId>,
    projections: Vec<Vec<usize>>,
}

impl FusionColumns {
    fn position(
        &mut self,
        edit: &CircuitEditor,
        field: ExpressionId,
        row_name: &str,
    ) -> Result<Option<usize>, Error> {
        for (index, &previous) in self.fields.iter().enumerate() {
            match compare_expressions(edit, previous.into(), field.into(), [(row_name, row_name)])?
            {
                Some(true) => return Ok(Some(index)),
                Some(false) => {}
                None => return Ok(None),
            }
        }
        let index = self.fields.len();
        self.fields.push(field);
        Ok(Some(index))
    }
}

/// Assign each equivalent field one column, in order of first use. Every member
/// is evaluated with the same parameter, so its fields share a lexical scope.
fn collect_columns(
    edit: &mut CircuitEditor,
    members: &[(NodeId, NodeId)],
    row: ExpressionId,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<FusionColumns>, Error> {
    let mut columns = FusionColumns::default();
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
            let Some(index) = columns.position(edit, edit.expression(field)?, &name)? else {
                return Ok(None);
            };
            projection.push(index);
        }
        columns.projections.push(projection);
    }
    Ok(Some(columns))
}

fn fuse_cluster(
    edit: &mut CircuitEditor,
    body: NodeId,
    members: &[(NodeId, NodeId)],
    next: &mut u64,
    replacements: &mut BTreeMap<NodeId, OutputPort>,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<bool, Error> {
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
    let Some(FusionColumns {
        fields: columns,
        projections,
    }) = collect_columns(edit, members, row, warnings)?
    else {
        return Ok(false);
    };
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
    edit.place_before(body, fused.operator, first)?;
    Ok(true)
}

pub fn fuse_expensive_maps(
    circuit: Circuit,
    clusters: Vec<Vec<FusionMember>>,
    next: &mut u64,
    warnings: &mut Vec<SimplifyWarning>,
) -> Result<Option<Circuit>, Error> {
    if clusters.is_empty() {
        return Ok(Some(circuit));
    }
    let mut emitted = Vec::new();
    // Decode the additional closures together. They can share circuit nodes and one another's types.
    let mut roots = Vec::new();
    let mut definitions = Vec::new();
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
                    definitions.extend(member.definitions);
                    NodeId::Outer(member.operator)
                })
                .collect::<Vec<_>>(),
        );
    }
    // Decode all referenced definitions in the same arena. Only the first
    // root per member denotes a function; later roots supply its dependencies.
    let (circuit, functions) = circuit.with_inner_roots(roots.into_iter().chain(definitions))?;
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
            if !fuse_cluster(
                &mut edit,
                bodies[&id],
                &members,
                &mut names,
                &mut replacements,
                &mut emitted,
            )? {
                return Ok(None);
            }
            for (member, _) in members {
                projections.replaced(member);
            }
        }
    }
    let circuit = projections.finish(edit.finish_forwarding(&replacements)?)?;
    *next = names;
    warnings.extend(emitted);
    Ok(Some(circuit))
}
