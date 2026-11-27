//! Public schema before optimization determines column usage.

use crate::{Failure, bind_sql, unsupported};
use feldera_sql_rel_model::{
    catalog::CatalogSchema,
    statements::{Statement, ViewKind},
    types::TypeTable,
};
use serde_json::{Value, json};

/// Bind user SQL and describe the visible tables and views under the default
/// Gen-2 profile. C1a omits optimizer usage flags and default/lateness text.
pub fn preview_schema(sql: &str) -> Result<Value, Failure> {
    let program = bind_sql("<schema>".into(), sql)?;
    let types = &program.table.type_table;
    let mut inputs = Vec::new();
    let mut outputs = Vec::new();
    for statement in &program.statements {
        match statement {
            // The default optimizer removes the unused system inputs in these
            // programs. The error view itself is public.
            Statement::CreateTable { source, .. } if !source.visible => continue,
            Statement::CreateTable {
                schema,
                foreign_keys,
                ..
            } => {
                if !foreign_keys.is_empty() {
                    return Err(unsupported("schema preview for foreign keys"));
                }
                let schema = &program.catalog.schemas[*schema as usize];
                let mut table = relation(schema, types)?;
                let key = schema.primary_key();
                if !key.is_empty() {
                    table["primary_key"] = json!(key);
                }
                table["foreign_keys"] = json!([]);
                inputs.push(table);
            }
            Statement::CreateView {
                view_kind: ViewKind::Local,
                ..
            } => continue,
            Statement::CreateView {
                schema, view_kind, ..
            } => {
                let mut view = relation(&program.catalog.schemas[*schema as usize], types)?;
                view["materialized"] = (*view_kind == ViewKind::Materialized).into();
                outputs.push(view);
            }
            Statement::ModifyTable { .. } => continue,
            _ => return Err(unsupported("schema preview for this statement")),
        }
    }
    Ok(json!({"inputs":inputs, "outputs":outputs}))
}

fn relation(schema: &CatalogSchema, types: &TypeTable) -> Result<Value, Failure> {
    if schema.properties.is_some() {
        return Err(unsupported("schema preview for relation properties"));
    }
    let fields = schema.columns.iter().map(|col| {
        let ty = &types.types[col.ty.0 as usize];
        Ok(json!({"name":col.name.name, "case_sensitive":col.name.quoted, "columntype":ty.scalar_column_type().ok_or_else(|| unsupported("schema preview for this column type"))?}))
    }).collect::<Result<Vec<_>,Failure>>()?;
    Ok(
        json!({"name":schema.name.name, "case_sensitive":schema.name.quoted,
              "fields":fields, "materialized":false}),
    )
}
