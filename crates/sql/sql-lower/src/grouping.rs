//! Lower a single grouping set without aggregate calls. Calcite represents
//! DISTINCT as a projection of its keys followed by a set-valued stream.

use super::{
    projection,
    source_table::{range, Ids},
    view::{lower_stream, Stream},
    Error,
};
use feldera_sql_ir::{Circuit, CircuitEditor, Field as IrField, NodeId, OperatorKind, OutputPort};
use feldera_sql_rel_model::{
    catalog::CatalogSchema, program::Program, relation_properties::Properties,
    statements::StatementSource,
};
use serde_json::{json, Value};

pub(super) fn lower(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    root: u32,
    source: &StatementSource,
) -> Result<Stream, Error> {
    let properties = &program.properties.relations[root as usize];
    let Properties::Aggregate {
        group,
        groups,
        calls,
    } = &properties.fields
    else {
        return Err(Error("expected an aggregate".into()));
    };
    let [input] = program.producers.relations[root as usize].inputs.as_slice() else {
        return Err(Error("expected one grouping input".into()));
    };
    if !calls.is_empty()
        || group.is_empty()
        || groups.as_slice() != [group.as_slice()]
        || !properties.metadata.traits.is_empty()
        || !properties.metadata.hints.is_empty()
    {
        return Err(Error(
            "expected one nonempty grouping set without aggregate calls".into(),
        ));
    }
    let child_schema = projection::input_schema(program, *input, schema)?;
    if group.len() != schema.columns.len()
        || group.iter().zip(&schema.columns).any(|(&i, c)| {
            child_schema
                .columns
                .get(i as usize)
                .is_none_or(|f| f.ty != c.ty)
        })
    {
        return Err(Error("grouping key types differ from output schema".into()));
    }
    let child = lower_stream(checkpoint, program, ids, &child_schema, *input, source)?;
    let mut select = child
        .select
        .ok_or_else(|| Error("unsupported grouping input for relational SQL".into()))?;
    select.group(group, &child_schema)?;
    let id = ids.relation(root);
    let input_id = ids.relation(*input);
    let text = child.origin["text"]
        .as_str()
        .ok_or_else(|| Error("missing input plan text".into()))?;
    let form = program.producers.relations[*input as usize].form;
    let keys = group
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let origin = json!({"id":id,"relId":id,"planId":ids.plan(root)?,"tableScan":false,
        "digest":format!("LogicalAggregate.(input={form:?}#{input_id},group={{{keys}}})"),
        "text":format!("LogicalAggregate(group=[{{{keys}}}])\n  {}",text.replace('\n',"\n  ")),
        "sql":select.sql(schema)?, "statefulSqlName":"a DISTINCT","origin":child.origin["origin"]});
    let source_op = child
        .operator
        .as_u64()
        .ok_or_else(|| Error("missing grouping input".into()))?;
    let graph = Circuit::from_json(checkpoint["circuit"].clone())?;
    let body = graph.root_id();
    let mut editor = CircuitEditor::new(graph.clone());
    let port = |operator| OutputPort {
        operator,
        number: 0,
    };
    let input_op = NodeId::Outer(source_op);
    let map = editor.append_operator(body, input_op, OperatorKind::Map, &[port(input_op)], [])?;
    let position = range(0, 0, 0, 0);
    let provenance = |id, kind, relation: Value| json!({"id":id,"kind":kind,"position":position,"positions":[],"relation":relation});
    editor.set_intermediate_origin(
        map,
        Some(provenance(
            ids.next_origin(),
            "IntermediateRel",
            origin.clone(),
        )),
    )?;
    let input_type = editor.node(input_op)?.edge("outputType")?;
    let row = editor.type_id(editor.node(input_type)?.edge("elementType")?)?;
    let mut next_name = checkpoint["next_generated_name"]
        .as_u64()
        .ok_or_else(|| Error("missing generated name counter".into()))?;
    let param = editor.ref_parameter(row, &mut next_name)?;
    let mut fields = Vec::with_capacity(group.len());
    for &i in group {
        let row = editor.deref_parameter(&param)?;
        let field = editor.tuple_field(row, i as usize)?;
        fields.push(editor.expand_tuple(map, field)?);
    }
    let tuple = editor.tuple_expr(map, false, &fields)?;
    let function = editor.closure(tuple, &[param])?;
    let output = editor.zset_type(editor.expression_type(tuple)?)?;
    editor.set_operator_field(map, "outputType", IrField::Reference(output.into()))?;
    editor.set_operator_field(map, "function", IrField::Reference(function.into()))?;
    let distinct = editor.append_operator(
        body,
        map,
        OperatorKind::StreamDistinct,
        &[port(map)],
        [("outputType", IrField::Reference(output.into()))],
    )?;
    let relation = json!({"ref":id});
    editor.set_intermediate_origin(
        distinct,
        Some(provenance(ids.next_origin(), "LastRel", relation.clone())),
    )?;
    // Other views and INSERTs may already own relation IDs. Number
    // provenance once after all statements have attached their origins.
    let operators = editor
        .circuit()
        .additional_operators(&graph, &[map, distinct])?;
    checkpoint["circuit"]["allOperators"]
        .as_array_mut()
        .expect("system operators")
        .extend(operators);
    checkpoint["next_generated_name"] = next_name.into();
    let (NodeId::Outer(operator), NodeId::Inner(output)) = (distinct, output.into()) else {
        unreachable!("typed grouping graph")
    };
    ids.after_subgraph(operator, output);
    Ok(Stream {
        select: Some(select),
        origin,
        operator: operator.into(),
        relation,
        output_type: json!({"node":output}),
    })
}
