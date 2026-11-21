//! The frozen S2 bootstrap represents one specific compiler-owned catalog.
//! Check its declarations before attaching any user operators from detached S1.

use super::Error;
use feldera_sql_rel_model::{
    program::Program,
    relation_properties::Properties,
    statements::{Statement, ViewKind},
};
use serde_json::Value;

pub(super) fn check_catalog(program: &Program, circuit: &Value) -> Result<(), Error> {
    let mut hidden = program.statements.iter().filter(|s| !s.source().visible);
    for public in circuit["metadata"]["inputs"]
        .as_array()
        .expect("system inputs")
    {
        let Some(Statement::CreateTable {
            schema,
            foreign_keys,
            ..
        }) = hidden.next()
        else {
            return Err(Error("missing compiler-owned table declaration".into()));
        };
        if !foreign_keys.is_empty() {
            return Err(Error(
                "unexpected foreign key on compiler-owned table".into(),
            ));
        }
        check_schema(program, *schema, public)?;
    }

    let Some(Statement::CreateView {
        schema,
        root,
        view_kind: ViewKind::Standard,
        emit_final: -1,
        source,
        ..
    }) = hidden.next()
    else {
        return Err(Error("missing compiler-owned error view".into()));
    };
    let public = &circuit["metadata"]["outputs"][0];
    check_schema(program, *schema, public)?;
    let operator = circuit["allOperators"]
        .as_array()
        .expect("system operators")
        .iter()
        .find(|o| o["class"] == "DBSPViewOperator" && o["metadata"]["system"] == true)
        .expect("system view");
    let scan = &program.properties.relations[root.relation as usize];
    let Properties::Scan { name } = &scan.fields else {
        return Err(Error("system view requires a table scan".into()));
    };
    let identity = root.fields.len() == program.catalog.schemas[*schema as usize].columns.len()
        && root
            .fields
            .iter()
            .enumerate()
            .all(|(i, f)| f.index as usize == i && public["fields"][i]["name"] == f.name);
    if root.kind != "SELECT"
        || !root.collation.is_empty()
        || !root.hints.is_empty()
        || !identity
        || name.as_slice() != ["schema", "feldera_error_table"]
        || source.statement_text != operator["query"].as_str().expect("system query")
        || hidden.next().is_some()
    {
        return Err(Error("unsupported compiler-owned statements in S1".into()));
    }
    Ok(())
}

fn check_schema(program: &Program, index: u32, public: &Value) -> Result<(), Error> {
    let schema = &program.catalog.schemas[index as usize];
    let fields = public["fields"].as_array().expect("system schema");
    let equal = schema.properties.is_none()
        && public["name"] == schema.name.name
        && public["case_sensitive"] == schema.name.quoted
        && fields.len() == schema.columns.len()
        && fields.iter().zip(&schema.columns).all(|(f, c)| {
            !c.primary_key
                && c.default_value.is_none()
                && c.lateness.is_none()
                && f["unused"] == c.unused
                && f["name"] == c.name.name
                && f["case_sensitive"] == c.name.quoted
                && program.table.type_table.types[c.ty.0 as usize]
                    .scalar_column_type()
                    .as_ref()
                    == f.get("columntype")
        });
    if equal {
        Ok(())
    } else {
        Err(Error(format!(
            "unsupported compiler-owned schema {}",
            schema.name.name
        )))
    }
}
