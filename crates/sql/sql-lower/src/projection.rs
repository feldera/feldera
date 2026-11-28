//! Lower a relational projection to a DBSP map. The closure shares its
//! parameter across field reads and retains the anonymous Calcite row types.

mod array;
mod computed;
mod field;
mod interval_count;
mod literal;
mod temporal_text;
mod trigonometric;
pub(super) use field::Field;

use super::{
    source_table::{stream_type, Ids, RowLayout},
    view::reference,
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema, program::Program, relation_properties::Properties,
};
use serde_json::{json, Value};

pub(super) fn expressions(
    program: &Program,
    root: u32,
) -> Result<&[feldera_sql_rel_model::expressions::ExprId], Error> {
    let mut uses = program
        .expressions
        .relation_uses
        .iter()
        .filter(|u| u.relation == root);
    let usage = uses
        .next()
        .ok_or_else(|| Error("missing relation expressions".into()))?;
    if uses.any(|u| u.roots != usage.roots) {
        return Err(Error(
            "relation has differing expressions across environments".into(),
        ));
    }
    Ok(&usage.roots)
}

/// ExpressionCompiler converts each result type before visiting the operands
/// and checks function-specific arguments after those visits.
pub(super) fn expression_warnings(
    program: &Program,
    root: u32,
    context: &Value,
    precision_warnings: &mut std::collections::HashSet<(&'static str, u32)>,
    messages: &mut Vec<Value>,
) -> Result<(), Error> {
    use feldera_sql_rel_model::expression_objects::ObjectKind;
    enum Visit {
        Enter,
        LeaveCall,
    }
    let mut pending: Vec<_> = expressions(program, root)?
        .iter()
        .rev()
        .map(|&id| (id, Visit::Enter))
        .collect();
    let mut visited = std::collections::HashSet::new();
    while let Some((id, visit)) = pending.pop() {
        let expr = &program.expressions.expressions[id.0 as usize];
        match visit {
            Visit::Enter if visited.insert(id.0) => {
                super::source_table::precision_warnings(
                    [expr.ty],
                    &program.table.type_table,
                    &super::source_table::expression_range(&expr.position),
                    context,
                    precision_warnings,
                    messages,
                );
                if let ObjectKind::Call { arguments, .. } = &expr.kind {
                    pending.push((id, Visit::LeaveCall));
                    pending.extend(arguments.iter().rev().map(|&id| (id, Visit::Enter)));
                }
            }
            Visit::LeaveCall => temporal_text::check_format(program, expr, context, messages),
            Visit::Enter => {}
        }
    }
    Ok(())
}

pub(super) fn projected_fields(
    program: &Program,
    root: u32,
    output: &CatalogSchema,
) -> Result<(u32, Option<Vec<Field>>), Error> {
    let prop = &program.properties.relations[root as usize];
    match &prop.fields {
        Properties::Scan { .. } | Properties::Filter { .. } => Ok((root, None)),
        Properties::Project { variables, .. }
            if variables.is_empty()
                && prop.metadata.hints.is_empty()
                && prop.metadata.traits.is_empty() =>
        {
            let [input] = program.producers.relations[root as usize].inputs.as_slice() else {
                return Err(Error("projection requires one input".into()));
            };
            let exprs = expressions(program, root)?;
            if exprs.len() != output.columns.len() {
                return Err(Error(
                    "projection width differs from the output schema".into(),
                ));
            }
            let fields = exprs
                .iter()
                .zip(&output.columns)
                .map(|(&expr, column)| Field::resolve(program, expr, column.ty))
                .collect::<Result<_, _>>()?;
            Ok((*input, Some(fields)))
        }
        _ => Err(Error(
            "query lowering requires a table scan or column projection".into(),
        )),
    }
}

/// A constant SELECT with unreduced calls is a Project over a VALUES row.
pub(super) fn over_values(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    output: &CatalogSchema,
    root: u32,
    source: &feldera_sql_rel_model::statements::StatementSource,
) -> Result<super::view::Stream, Error> {
    use feldera_sql_rel_model::catalog::{Column, Name};
    let (input, fields) = projected_fields(program, root, output)?;
    let fields = fields.ok_or_else(|| Error("missing projection fields".into()))?;
    let row = program.producers.relations[input as usize].row;
    let feldera_sql_rel_model::types::TypeKind::Row {
        fields: columns, ..
    } = &program.table.type_table.types[row.0 as usize].kind
    else {
        return Err(Error("VALUES input requires a row type".into()));
    };
    let input_schema = CatalogSchema {
        columns: columns
            .iter()
            .enumerate()
            .map(|(i, field)| Column {
                name: Name {
                    name: field.name.name.clone(),
                    quoted: field.name.name != field.name.name.to_lowercase(),
                },
                index: i as u32,
                ty: field.ty,
                diagnostic: output.diagnostic.clone(),
                primary_key: false,
                unused: false,
                lateness: None,
                default_value: None,
                default_position: None,
            })
            .collect(),
        ..output.clone()
    };
    let stream = super::values::lower(checkpoint, program, ids, &input_schema, input, source)?;
    let projection = Projection {
        input: &input_schema,
        output,
        fields: &fields,
    };
    let id = ids.relation(root);
    let origin = projection.relation(&stream.origin, id, ids.plan(root)?)?;
    let generated = checkpoint["next_generated_name"]
        .as_u64()
        .ok_or_else(|| Error("missing generated name counter".into()))?;
    let map = projection.map(program, ids, &stream.operator, origin.clone(), generated)?;
    let result = super::view::Stream {
        origin,
        operator: map["id"].clone(),
        output_type: reference(&map["outputType"])?,
        relation: json!({"ref":id}),
    };
    checkpoint["circuit"]["allOperators"]
        .as_array_mut()
        .expect("system operators")
        .push(map);
    checkpoint["next_generated_name"] = (generated + 1).into();
    Ok(result)
}

/// Calcite compresses the leading identity fields in its relational digest.
fn digest(columns: &[Field], input: &str, scan: usize) -> String {
    let prefix = columns
        .iter()
        .enumerate()
        .take_while(|(i, col)| matches!(col, Field::Column(index) if i == index))
        .count();
    let mut terms = vec![format!("input={input}#{scan}")];
    match prefix {
        0 => (),
        1 => terms.push("inputs=0".into()),
        n => terms.push(format!("inputs=0..{}", n - 1)),
    }
    if prefix < columns.len() {
        let fields = columns[prefix..]
            .iter()
            .map(Field::digest)
            .collect::<Vec<_>>()
            .join(", ");
        terms.push(format!("exprs=[{fields}]"));
    }
    format!("LogicalProject.({})", terms.join(","))
}

pub(super) struct Projection<'a> {
    pub input: &'a CatalogSchema,
    pub output: &'a CatalogSchema,
    pub fields: &'a [Field],
}

impl Projection<'_> {
    pub(super) fn relation(&self, scan: &Value, id: usize, plan: u64) -> Result<Value, Error> {
        let scan_id = scan["relId"]
            .as_u64()
            .ok_or_else(|| Error("missing physical scan".into()))? as usize;
        let mut text = Vec::new();
        let mut select = Vec::new();
        for (field, output) in self.fields.iter().zip(&self.output.columns) {
            let name = &output.name.name;
            text.push(format!("{name}=[{}]", field.digest()));
            select.push(field.sql(self.input, name)?);
        }
        let from = scan["sql"]
            .as_str()
            .and_then(|sql| sql.strip_prefix("SELECT *\n"))
            .ok_or_else(|| Error("missing SQL for scan".into()))?;
        let child = scan["text"]
            .as_str()
            .ok_or_else(|| Error("missing relational text for scan".into()))?;
        let child_name = child
            .split_once('(')
            .map(|(name, _)| name)
            .ok_or_else(|| Error("missing input relation name".into()))?;
        let child = child.replace('\n', "\n  ");
        Ok(
            json!({"id":id,"relId":id,"planId":plan,"tableScan":false,"digest":digest(self.fields, child_name, scan_id),
            "text":format!("LogicalProject({})\n  {child}",text.join(", ")),
            "sql":format!("SELECT {}\n{from}",select.join(", ")),"statefulSqlName":null,"origin":scan["origin"]}),
        )
    }

    pub(super) fn map(
        &self,
        program: &Program,
        ids: &mut Ids,
        source: &Value,
        relation: Value,
        name: u64,
    ) -> Result<Value, Error> {
        let position =
            json!({"start_line_number":0,"start_column":0,"end_line_number":0,"end_column":0});
        let types = &program.table.type_table;
        let output_layout = RowLayout::new(self.output, types)?;
        let output = output_layout.tuple(ids, &position, true);
        let output_ref = reference(&output)?;
        let input = RowLayout::new(self.input, types)?;
        let mut parameter = super::closure::RowParameter::new(ids, &input, name)?;
        let mut values = Vec::with_capacity(self.fields.len());
        for (index, (field, column)) in self.fields.iter().zip(&self.output.columns).enumerate() {
            values.push(field.lower(
                ids,
                &mut parameter,
                &types.types[column.ty.0 as usize].kind,
                &output_layout,
                index,
                &position,
            )?);
        }
        let mut body = ids.node("DBSPTupleExpression", &position);
        body["type"] = output_ref.clone();
        body["fields"] = values.into();
        let closure = parameter.close(ids, output_ref, body, &position)?;
        Ok(
            json!({"class":"DBSPMapOperator","id":ids.next_operator(),"annotations":[],
            "provenance":{"id":ids.next_origin(),"kind":"LastRel","position":position,"positions":[],"relation":relation},
            "inputs":[{"outputNumber":0,"operator":{"node":source}}],"outputType":stream_type(ids,output,&position)?,"function":closure}),
        )
    }
}
