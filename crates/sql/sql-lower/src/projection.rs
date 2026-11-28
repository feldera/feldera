//! Lower a relational projection to a DBSP map. The closure shares its
//! parameter across field reads and retains the anonymous Calcite row types.

mod computed;
mod field;
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
