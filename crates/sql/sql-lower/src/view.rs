//! Lower view definitions and attach sinks for public outputs.

use super::{
    source_table::{name, statement_position, Ids, RowLayout},
    Error,
};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    program::Program,
    relation_properties::Properties,
    statements::{RelRoot, StatementSource, ViewKind},
};
use serde_json::{json, Value};

/// An inner definition and an earlier use both identify the same arena node.
pub(super) fn reference(node: &Value) -> Result<Value, Error> {
    let id = node
        .get("id")
        .or_else(|| node.get("node"))
        .and_then(Value::as_u64)
        .ok_or_else(|| Error("expected an inner circuit node".into()))?;
    Ok(json!({"node":id}))
}

fn identity(ids: &mut Ids, any: &Value) -> Value {
    let mut ty = ids.generated("DBSPTypeFunction");
    ty["resultType"] = any.clone();
    ty["parameterTypes"] = json!([any]);
    ty["code"] = "FUNCTION".into();
    ty["mayBeNull"] = false.into();
    let mut param = ids.generated("DBSPParameter");
    param["type"] = any.clone();
    param["name"] = "t".into();
    let mut body = ids.generated("DBSPVariablePath");
    body["type"] = any.clone();
    body["variable"] = "t".into();
    let mut closure = ids.generated("DBSPClosureExpression");
    closure["type"] = ty;
    closure["parameters"] = json!([param]);
    closure["body"] = body;
    closure
}

pub(super) fn plan_id(checkpoint: &Value, view: &str) -> Result<u64, Error> {
    checkpoint["calcite_plan"][view]["rels"]
        .as_array()
        .and_then(|rels| rels.last())
        .and_then(|rel| rel["id"].as_u64())
        .ok_or_else(|| Error(format!("missing plan for {view}")))
}

pub(super) fn restore_system_scan(checkpoint: &mut Value, program: &Program) -> Result<(), Error> {
    use feldera_sql_rel_model::statements::Statement;
    let system = program
        .statements
        .iter()
        .find_map(|s| match s {
            Statement::CreateView {
                name, source, root, ..
            } if !source.visible && name.name == "error_view" => Some(root.relation),
            _ => None,
        })
        .ok_or_else(|| Error("missing system view in S1".into()))?;
    let id = program
        .table_scan_order
        .iter()
        .position(|&id| id == system)
        .ok_or_else(|| Error("missing system scan allocation".into()))?;
    let plan = plan_id(checkpoint, "error_view")?;
    let operators = checkpoint["circuit"]["allOperators"]
        .as_array_mut()
        .expect("system operators");
    let source = operators
        .iter_mut()
        .find(|op| op["tableName"]["name"] == "feldera_error_table")
        .ok_or_else(|| Error("missing system source".into()))?;
    let origin = &mut source["provenance"]["children"][0]["relation"];
    origin["relId"] = id.into();
    origin["planId"] = plan.into();
    Ok(())
}

struct Scan<'a> {
    namespace: &'a str,
    table: &'a str,
    alias: &'a str,
    physical_id: usize,
    input: &'a CatalogSchema,
    columns: Option<Vec<usize>>,
    predicate: Option<(u32, super::filter::Predicate)>,
}

fn resolve_scan<'a>(
    program: &'a Program,
    root: &RelRoot,
    schema: &CatalogSchema,
) -> Result<Scan<'a>, Error> {
    let (input_relation, columns) = super::projection::input_columns(program, root.relation)?;
    let (scan_id, predicate) = super::filter::Predicate::resolve(program, input_relation)?;
    let relation = &program.properties.relations[scan_id as usize];
    let Properties::Scan { name: path } = &relation.fields else {
        return Err(Error("query lowering requires a direct table scan".into()));
    };
    let [namespace, table_name] = path.as_slice() else {
        return Err(Error("expected a schema-qualified scan".into()));
    };
    let [hint] = relation.metadata.hints.as_slice() else {
        return Err(Error("expected a single alias hint on the scan".into()));
    };
    let [alias] = hint.list.as_slice() else {
        return Err(Error("expected one scan alias".into()));
    };
    if namespace != "schema"
        || hint.name != "alias"
        || !hint.options.is_empty()
        || !hint.inherit_path.is_empty()
        || !relation.metadata.traits.is_empty()
        || [namespace, table_name, alias]
            .iter()
            .any(|s| !s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
    {
        return Err(Error("unsupported scan or view metadata".into()));
    }
    let physical_id = program
        .table_scan_order
        .iter()
        .position(|id| *id == scan_id)
        .ok_or_else(|| Error("missing table scan allocation".into()))?;
    let input = program
        .statements
        .iter()
        .find_map(|s| match s {
            feldera_sql_rel_model::statements::Statement::CreateTable { name, schema, .. }
            | feldera_sql_rel_model::statements::Statement::CreateView { name, schema, .. }
                if name.name == *table_name =>
            {
                Some(&program.catalog.schemas[*schema as usize])
            }
            _ => None,
        })
        .ok_or_else(|| Error(format!("missing source table {table_name}")))?;
    if root.fields.len() != columns.as_ref().map_or(input.columns.len(), Vec::len)
        || root.fields.len() != schema.columns.len()
        || root
            .fields
            .iter()
            .zip(&schema.columns)
            .enumerate()
            .any(|(i, (field, output))| {
                let index = columns.as_ref().map_or(i, |c| c[i]);
                field.index as usize != i
                    || field.name != output.name.name
                    || input.columns.get(index).is_none_or(|input| {
                        input.ty != output.ty || (columns.is_none() && input.name != output.name)
                    })
            })
    {
        return Err(Error("view fields differ from projection".into()));
    }
    Ok(Scan {
        namespace,
        table: table_name,
        alias,
        physical_id,
        input,
        columns,
        predicate: predicate.map(|p| (input_relation, p)),
    })
}

impl Scan<'_> {
    fn relation(&self, schema: &CatalogSchema, position: &Value, user_plan_id: u64) -> Value {
        let Scan {
            namespace,
            table: table_name,
            alias,
            physical_id,
            ..
        } = self;
        json!({
            "id":physical_id, "relId":physical_id, "planId":user_plan_id, "tableScan":true,
            "digest":format!("LogicalTableScan.(table=[{namespace}, {table_name}],hints=[[alias inheritPath:[] options:[{alias}]]])"),
            "text":format!("LogicalTableScan(table=[[{namespace}, {table_name}]], hints=[[[alias inheritPath:[] options:[{alias}]]]])"),
            "sql":format!("SELECT *\nFROM `{namespace}`.`{table_name}`\n/*+ `alias`('{alias}') */"),
            "statefulSqlName":null, "origin":{"name":schema.name.name,"caseSensitive":schema.name.quoted,"position":position}
        })
    }
}

pub(super) struct Stream {
    pub operator: Value,
    pub output_type: Value,
    pub relation: Value,
}

fn scan_stream(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    root: &RelRoot,
    source: &StatementSource,
) -> Result<Stream, Error> {
    let scan = resolve_scan(program, root, schema)?;
    let user_plan_id = plan_id(checkpoint, &schema.name.name)?;
    let mut generated = checkpoint["next_generated_name"]
        .as_u64()
        .ok_or_else(|| Error("missing generated name counter".into()))?;
    let scan_plan_id = if scan.columns.is_some() || scan.predicate.is_some() {
        checkpoint["calcite_plan"][&schema.name.name]["rels"]
            .as_array()
            .and_then(|rels| rels.first())
            .and_then(|r| r["id"].as_u64())
            .ok_or_else(|| Error("missing projection input plan".into()))?
    } else {
        user_plan_id
    };
    let filter_plan = checkpoint["calcite_plan"][&schema.name.name]["rels"]
        .as_array()
        .and_then(|rels| rels.iter().find(|r| r["relOp"] == "LogicalFilter"))
        .and_then(|r| r["id"].as_u64());
    let graph = &mut checkpoint["circuit"];
    let operators = graph["allOperators"]
        .as_array_mut()
        .expect("system operators");
    let table_name = scan.table;
    let input = operators
        .iter_mut()
        .find(|op| match op["class"].as_str() {
            Some("DBSPSourceMultisetOperator") => op["tableName"]["name"] == table_name,
            Some("DBSPViewOperator") => op["viewName"]["name"] == table_name,
            _ => false,
        })
        .ok_or_else(|| Error(format!("missing input relation {table_name}")))?;
    let position = statement_position(source);
    let mut input_origin = scan.relation(schema, &position, scan_plan_id);
    let mut relation = if input["class"] == "DBSPSourceMultisetOperator" {
        let position = input["sourceNamePosition"].clone();
        input["provenance"]["children"]
            .as_array_mut()
            .expect("source origins")
            .push(json!({
                "id":ids.next_origin(), "kind":"LastRel", "position":position,
                "positions":[position], "relation":input_origin
            }));
        json!({"ref": scan.physical_id})
    } else {
        // Reading a previously declared view reuses its operator. Its first
        // consumer owns the scan origin when the scan itself is the view root.
        input_origin.clone()
    };
    let mut output_type = reference(&input["outputType"])?;
    let mut source_id = input["id"].clone();
    if let Some((filter, predicate)) = &scan.predicate {
        let relation_id = ids.relation(*filter);
        relation = json!({"ref":relation_id});
        let layout = RowLayout::new(scan.input, &program.table.type_table)?;
        input_origin = predicate.relation(
            scan.input,
            &input_origin,
            relation_id,
            filter_plan.ok_or_else(|| Error("missing filter plan".into()))?,
        );
        let closure = predicate.closure(ids, &layout, generated)?;
        generated += 1;
        let filter_id = ids.next_operator();
        let unknown = super::source_table::range(0, 0, 0, 0);
        operators.push(json!({"class":"DBSPFilterOperator","id":filter_id,"annotations":[],
            "provenance":{"id":ids.next_origin(),"kind":"LastRel","position":unknown,"positions":[],"relation":input_origin},
            "inputs":[{"outputNumber":0,"operator":{"node":source_id}}],"outputType":output_type,"function":closure}));
        source_id = filter_id.into();
    }
    if let Some(columns) = &scan.columns {
        let projection = super::projection::Projection {
            input: scan.input,
            output: schema,
            columns,
        };
        let relation_id = ids.relation(root.relation);
        relation = json!({"ref":relation_id});
        let relation = projection.relation(&input_origin, relation_id, user_plan_id)?;
        let map = projection.map(program, ids, &source_id, relation, generated)?;
        generated += 1;
        output_type = reference(&map["outputType"])?;
        source_id = map["id"].clone();
        operators.push(map);
    }
    checkpoint["next_generated_name"] = generated.into();
    Ok(Stream {
        operator: source_id,
        output_type,
        relation,
    })
}

pub(super) fn lower(
    checkpoint: &mut Value,
    program: &Program,
    ids: &mut Ids,
    schema: &CatalogSchema,
    kind: ViewKind,
    root: &RelRoot,
    source: &StatementSource,
) -> Result<(), Error> {
    if !root.hints.is_empty()
        || !root.collation.is_empty()
        || root.kind != "SELECT"
        || schema.properties.is_some()
        || schema
            .columns
            .iter()
            .any(|c| c.primary_key || c.unused || c.lateness.is_some() || c.default_value.is_some())
    {
        return Err(Error("unsupported view metadata".into()));
    }
    // A declared view column list renames the public row. Calcite keeps the
    // SELECT names in the stream feeding that view, including generated EXPR$n names.
    if root.fields.len() != schema.columns.len() {
        return Err(Error("view column count differs from SELECT output".into()));
    }
    let mut query_schema = std::borrow::Cow::Borrowed(schema);
    for (index, field) in root.fields.iter().enumerate() {
        if query_schema.columns[index].name.name != field.name {
            let name = &mut query_schema.to_mut().columns[index].name;
            name.quoted = field.name.to_lowercase() != field.name;
            name.name.clone_from(&field.name);
        }
    }
    let stream = match program.properties.relations[root.relation as usize].fields {
        Properties::Values { .. } => {
            super::values::lower(checkpoint, program, ids, &query_schema, root, source)?
        }
        _ => scan_stream(checkpoint, program, ids, &query_schema, root, source)?,
    };
    let Stream {
        operator: source_id,
        output_type,
        relation,
    } = stream;
    let layout = RowLayout::new(schema, &program.table.type_table)?;
    let position = statement_position(source);
    let graph = &mut checkpoint["circuit"];
    let operators = graph["allOperators"]
        .as_array_mut()
        .expect("system operators");
    let system_view = operators
        .iter()
        .find(|op| op["class"] == "DBSPViewOperator" && op["metadata"]["system"] == true)
        .ok_or_else(|| Error("missing system view".into()))?;
    let any = reference(&system_view["function"]["type"]["resultType"])?;
    let structure = layout.structure(ids, &position);
    let mut fields = Vec::with_capacity(schema.columns.len());
    let mut columns = Vec::with_capacity(schema.columns.len());
    for ((column, ty), descriptor) in schema
        .columns
        .iter()
        .zip(layout.column_types(ids))
        .zip(layout.descriptors())
    {
        fields.push(
            json!({"name":column.name.name,"case_sensitive":column.name.quoted,
                            "columntype":descriptor,"unused":false}),
        );
        columns.push(json!({"position":position,"viewName":name(&schema.name),
                            "columnName":name(&column.name),"type":ty}));
    }
    let metadata = |columns: &[Value]| {
        json!({"viewName":name(&schema.name),"viewKind":kind,
        "emitFinalColumn":-1,"recursive":false,"system":false,"columns":columns})
    };
    let view_id = ids.next_operator();
    let origin = |id: u64, kind: &str, relation: &Value| json!({"id":id,"kind":kind,"position":position,"positions":[position],"relation":relation});
    let relation_ref = json!({"ref":relation.get("ref").unwrap_or(&relation["id"])});
    let row_ref = reference(&structure)?;
    let view_origin = if kind == ViewKind::Local {
        "LastRel"
    } else {
        "IntermediateRel"
    };
    operators.push(json!({"class":"DBSPViewOperator","id":view_id,"annotations":[],
        "provenance":origin(ids.next_origin(),view_origin,&relation),"inputs":[{"outputNumber":0,"operator":{"node":source_id}}],
        "originalRowType":structure,"outputType":output_type,"function":identity(ids,&any),
        "viewName":name(&schema.name),"query":source.statement_text,"metadata":metadata(&columns)}));
    if kind == ViewKind::Local {
        return Ok(());
    }
    for column in &mut columns {
        column["type"] = reference(&column["type"])?;
    }
    operators.push(json!({"class":"DBSPSinkOperator","id":ids.next_operator(),"annotations":[],
        "provenance":origin(ids.next_origin(),"LastRel",&relation_ref),"inputs":[{"outputNumber":0,"operator":{"node":view_id}}],
        "originalRowType":row_ref,"outputType":output_type,"viewName":name(&schema.name),
        "query":source.statement_text,"metadata":metadata(&columns)}));
    graph["metadata"]["outputs"]
        .as_array_mut()
        .expect("system outputs")
        .push(
            json!({"name":schema.name.name,"case_sensitive":schema.name.quoted,
                     "fields":fields,"materialized":kind == ViewKind::Materialized}),
        );
    Ok(())
}
