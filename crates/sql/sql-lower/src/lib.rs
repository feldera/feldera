//! Lower the catalog declarations in a bound default-profile Gen-2 program.
//! The initial circuit contains only compiler-owned operators. Each user table
//! is built from its validated S1 schema, statement and original source spans.

#![forbid(unsafe_code)]

mod closure;
mod filter;
mod grouping;
mod integer;
mod origin_order;
mod plan;
mod projection;
mod select_sql;
mod set;
mod source_table;
mod system;
mod values;
mod view;

pub use plan::query_plan;

use feldera_sql_ir::Circuit;
use feldera_sql_rel_model::{
    diagnostics::DiagnosticBoundary, program::Program, statements::Statement,
};
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

impl From<feldera_sql_ir::Error> for Error {
    fn from(error: feldera_sql_ir::Error) -> Self {
        Self(error.to_string())
    }
}

/// A SQL invocation supplies the filename used by the direct compiler.
/// Detached lowering preserves the source identity carried by the S1 context.
pub enum Input<'a> {
    Sql(Option<&'a str>),
    Checkpoint,
}

/// Constant rows acquire their runtime types while compiling the relation,
/// before the compiler visits the view declaration.
fn has_values_input(program: &Program, root: u32) -> bool {
    use feldera_sql_rel_model::relation_properties::Properties;
    let properties = |id: u32| &program.properties.relations[id as usize].fields;
    match properties(root) {
        Properties::Values { .. } => true,
        Properties::Project { .. } => matches!(
            program.producers.relations[root as usize].inputs.as_slice(),
            &[input] if matches!(properties(input), Properties::Values { .. })
        ),
        _ => false,
    }
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
    if !program.context.unresolved_view_lateness.is_empty() {
        return Err(Error(
            "S2 lowering requires a program without pending lateness".into(),
        ));
    }
    checkpoint["sources"] = sources;
    let (plan, plan_ids) = plan::build(program)?;
    checkpoint["calcite_plan"] = plan;
    checkpoint["producer_commit"] = producer.into();
    checkpoint["runtime_abi"] = runtime.into();

    let has_query = program
        .statements
        .iter()
        .any(|s| s.source().visible && matches!(s, Statement::CreateView { .. }));
    if has_query || matches!(input, Input::Checkpoint) {
        view::restore_system_scan(&mut checkpoint, program)?;
    }
    let mut ids = source_table::Ids::after(&checkpoint["circuit"], program, plan_ids);
    let mut precision_warnings = std::collections::HashSet::new();
    for (index, statement) in program.statements.iter().enumerate() {
        let diagnostics = &mut checkpoint["diagnostics"];
        apply_boundary(diagnostics, &program.diagnostics.before[index]);
        if !statement.source().visible {
            continue;
        }
        // Scalar and collection aliases have already been resolved at each
        // use site. Only struct declarations create a DBSP item.
        if let Statement::CreateType {
            ty,
            attributes,
            name,
            name_diagnostic,
            ..
        } = statement
        {
            if let Some(attributes) = attributes {
                let context = diagnostics["context"].clone();
                source_table::precision_warnings(
                    [*ty],
                    &program.table.type_table,
                    &source_table::location(&name_diagnostic.origin),
                    &context,
                    &mut precision_warnings,
                    diagnostics["messages"]
                        .as_array_mut()
                        .expect("system diagnostics"),
                );
                source_table::declare_record(
                    &mut checkpoint["circuit"],
                    &mut ids,
                    name,
                    name_diagnostic,
                    attributes,
                    &program.table.type_table,
                    *ty,
                )?;
            } else if matches!(
                program.table.type_table.types[ty.0 as usize].kind,
                feldera_sql_rel_model::types::TypeKind::Row { .. }
            ) {
                return Err(Error(
                    "a ROW type declaration requires named attributes".into(),
                ));
            }
            continue;
        }
        let context = diagnostics["context"].clone();
        if let Statement::ModifyTable {
            source,
            name,
            insert: true,
            relation: Some(relation),
            columns,
        } = statement
        {
            // This profile builds a pipeline. Literal inserts have no circuit
            // state and no table contents consumer. Testing mode and inserts
            // that read another table require an actual DML evaluator.
            let warnings = feldera_sql_rel_model::literal_insert::validate(
                program,
                &program.statements[..index],
                name,
                columns.as_deref(),
                *relation,
            )
            .map_err(Error)?;
            if matches!(
                program.properties.relations[*relation as usize].fields,
                feldera_sql_rel_model::relation_properties::Properties::Project { .. }
            ) {
                // compileConstantProject allocates an input-row variable even
                // when every projected expression is independent of that row.
                let next = checkpoint["next_generated_name"]
                    .as_u64()
                    .ok_or_else(|| Error("missing generated name counter".into()))?;
                checkpoint["next_generated_name"] = (next + 1).into();
            }
            let position = source_table::statement_position(source);
            let messages = checkpoint["diagnostics"]["messages"]
                .as_array_mut()
                .expect("system diagnostics");
            messages.push(serde_json::json!({
                "context":context, "range":position, "warning":true, "continuation":false,
                "type":"Statement ignored", "message":"INSERT has no effect in a pipeline definition"
            }));
            // Java constructs the VALUES row type before visiting its literal
            // cells. Each cell can retain a narrower fractional precision.
            let row = program.producers.relations[*relation as usize].row;
            let cells = projection::expressions(program, *relation)?
                .iter()
                .map(|id| program.expressions.expressions[id.0 as usize].ty);
            source_table::precision_warnings(
                std::iter::once(row).chain(cells),
                &program.table.type_table,
                &source_table::range(0, 0, 0, 0),
                &context,
                &mut precision_warnings,
                messages,
            );
            for warning in warnings {
                messages.push(serde_json::json!({
                    "context":context, "range":source_table::range(0, 0, 0, 0), "warning":true,
                    "continuation":false, "type":warning.kind, "message":warning.message
                }));
            }
            continue;
        }
        if let Statement::CreateTable { source, schema, .. }
        | Statement::CreateView { source, schema, .. } = statement
        {
            let position = match statement {
                Statement::CreateView { root, .. } if has_values_input(program, root.relation) => {
                    source_table::range(0, 0, 0, 0)
                }
                _ => source_table::statement_position(source),
            };
            source_table::precision_warnings(
                program.catalog.schemas[*schema as usize]
                    .columns
                    .iter()
                    .map(|c| c.ty),
                &program.table.type_table,
                &position,
                &context,
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
            if matches!(
                program.properties.relations[root.relation as usize].fields,
                feldera_sql_rel_model::relation_properties::Properties::Project { .. }
            ) {
                let messages = checkpoint["diagnostics"]["messages"]
                    .as_array_mut()
                    .expect("system diagnostics");
                projection::expression_warnings(
                    program,
                    root.relation,
                    &context,
                    &mut precision_warnings,
                    messages,
                )?;
            }
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
    let diagnostics = &mut checkpoint["diagnostics"];
    apply_boundary(diagnostics, &program.diagnostics.after);
    // CompilerMessages retains the first occurrence across binding and lowering.
    let mut reported = std::collections::HashSet::new();
    diagnostics["messages"]
        .as_array_mut()
        .expect("system diagnostics")
        .retain(|warning| reported.insert(warning.clone()));
    let graph = &mut checkpoint["circuit"];
    origin_order::assign(graph)?;
    let circuit = Circuit::from_json(graph.take()).map_err(|e| Error(e.to_string()))?;
    circuit.provenance().map_err(|e| Error(e.to_string()))?;
    *graph = circuit.to_json();
    Ok(checkpoint)
}

fn apply_boundary(diagnostics: &mut Value, boundary: &DiagnosticBoundary) {
    let messages = diagnostics["messages"]
        .as_array_mut()
        .expect("system diagnostics");
    messages.extend(boundary.messages.iter().map(|warning| {
        serde_json::json!({
            "context":source_table::location(&warning.context),
            "range":source_table::location(&warning.range),
            "warning":true, "continuation":warning.continuation,
            "type":warning.category, "message":warning.message
        })
    }));
    diagnostics["context"] = source_table::location(&boundary.context);
}
