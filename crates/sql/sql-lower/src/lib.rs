//! Lower the catalog declarations in a bound default-profile Gen-2 program.
//! The initial circuit contains only compiler-owned operators. Each user table
//! is built from its validated S1 schema, statement and original source spans.

mod closure;
mod filter;
mod origin_order;
mod plan;
mod projection;
mod source_table;
mod system;
mod values;
mod view;

pub use plan::query_plan;

use feldera_sql_ir::Circuit;
use feldera_sql_rel_model::{program::Program, statements::Statement};
use serde_json::Value;
use std::fmt;

#[derive(Debug)]
pub struct Error(String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

/// A SQL invocation supplies the filename used by the direct compiler.
/// Detached lowering preserves the source identity carried by the S1 context.
pub enum Input<'a> {
    Sql(Option<&'a str>),
    Checkpoint,
}

/// Lower the supported default-profile table declarations and table-scan views.
pub fn lower_program(
    program: &Program,
    input: Input<'_>,
    producer: &str,
    runtime: &str,
) -> Result<Value, Error> {
    if producer.trim().is_empty() || runtime.trim().is_empty() {
        return Err(Error("S2 requires compiler and runtime identities".into()));
    }
    program
        .validate(Default::default(), Default::default(), Default::default())
        .map_err(|e| Error(e.to_string()))?;
    let mut checkpoint: Value = serde_json::from_str(include_str!("system.s2.json"))
        .map_err(|e| Error(format!("invalid compiler system circuit: {e}")))?;
    system::check_catalog(program, &checkpoint["circuit"])?;
    let options =
        serde_json::to_value(&program.context.options).map_err(|e| Error(e.to_string()))?;
    for group in ["languageOptions", "ioOptions"] {
        for (key, expected) in checkpoint["options"][group]
            .as_object()
            .expect("system profile")
        {
            let actual = if key == "multiCrates" {
                Value::Bool(
                    options[group]["crates"]
                        .as_str()
                        .is_some_and(|s| !s.is_empty()),
                )
            } else {
                options[group][key].clone()
            };
            if actual != *expected {
                return Err(Error(format!("unsupported S2 profile option {key}")));
            }
        }
    }
    let mut sources =
        serde_json::to_value(&program.context.sources).map_err(|e| Error(e.to_string()))?;
    if let Input::Sql(filename) = input {
        sources["file"] = filename.into();
    }
    if !program.context.unresolved_view_lateness.is_empty()
        || !program.diagnostics.after.messages.is_empty()
        || program
            .diagnostics
            .before
            .iter()
            .any(|b| !b.messages.is_empty())
    {
        return Err(Error(
            "S2 lowering requires a program without pending lateness or frontend messages".into(),
        ));
    }
    checkpoint["sources"] = sources;
    checkpoint["calcite_plan"] = query_plan(program)?;
    checkpoint["producer_commit"] = producer.into();
    checkpoint["runtime_abi"] = runtime.into();

    let has_query = program
        .statements
        .iter()
        .any(|s| s.source().visible && matches!(s, Statement::CreateView { .. }));
    if has_query || matches!(input, Input::Checkpoint) {
        view::restore_system_scan(&mut checkpoint, program)?;
    }
    let mut ids = source_table::Ids::after(&checkpoint["circuit"], program.table_scan_order.len());
    let mut precision_warnings = std::collections::HashSet::new();
    for statement in program.statements.iter().filter(|s| s.source().visible) {
        if let Statement::CreateTable { source, schema, .. }
        | Statement::CreateView { source, schema, .. } = statement
        {
            source_table::precision_warnings(
                &program.catalog.schemas[*schema as usize],
                &program.table.type_table,
                source,
                &mut precision_warnings,
                checkpoint["diagnostics"]["messages"]
                    .as_array_mut()
                    .expect("system diagnostics"),
            );
        }
        if let Statement::CreateView {
            source,
            schema,
            view_kind,
            emit_final: -1,
            root,
            ..
        } = statement
        {
            view::lower(
                &mut checkpoint,
                program,
                &mut ids,
                &program.catalog.schemas[*schema as usize],
                *view_kind,
                root,
                source,
            )?;
            continue;
        }
        let graph = &mut checkpoint["circuit"];
        let Statement::CreateTable {
            source,
            schema,
            name_diagnostic,
            foreign_keys,
            ..
        } = statement
        else {
            return Err(Error("unsupported statement in Rust S2 lowering".into()));
        };
        let schema = &program.catalog.schemas[*schema as usize];
        if !foreign_keys.is_empty()
            || schema.properties.is_some()
            || schema
                .columns
                .iter()
                .any(|c| c.lateness.is_some() || c.default_value.is_some() || c.unused)
        {
            return Err(Error(format!(
                "unsupported metadata on table {}",
                schema.name.name
            )));
        }
        let (operator, metadata) = source_table::lower(
            schema,
            &program.table.type_table,
            source,
            name_diagnostic,
            &mut ids,
        )?;
        graph["allOperators"]
            .as_array_mut()
            .expect("system circuit")
            .push(operator);
        graph["metadata"]["inputs"]
            .as_array_mut()
            .expect("system schema")
            .push(metadata);
    }
    let graph = &mut checkpoint["circuit"];
    origin_order::assign(graph)?;
    let circuit = Circuit::from_json(graph.take()).map_err(|e| Error(e.to_string()))?;
    circuit.provenance().map_err(|e| Error(e.to_string()))?;
    *graph = circuit.to_json();
    Ok(checkpoint)
}
