//! Isolated DataFusion SQL planner experiment for P0.
mod arithmetic;
mod set_operations;
mod signatures;
mod values;

use arrow_schema::{DataType, Schema, SchemaRef};
use datafusion_common::{Result, TableReference, config::ConfigOptions, plan_err};
use datafusion_expr::{
    AggregateUDF, HigherOrderUDF, ScalarUDF, TableSource, WindowUDF,
    planner::{ExprPlanner, RelationPlanner},
};
use datafusion_sql::{
    planner::{ContextProvider, SqlToRel},
    sqlparser::{
        ast::{ColumnOption, SetExpr, Spanned, Statement},
        dialect::GenericDialect,
        parser::Parser,
    },
};
use serde::Deserialize;
use std::{
    env,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    sync::Arc,
};

#[derive(Default)]
struct Catalog {
    options: ConfigOptions,
    signature_functions: bool,
    table: Option<Arc<ProbeTable>>,
    relation_planners: Vec<Arc<dyn RelationPlanner>>,
    expr_planners: Vec<Arc<dyn ExprPlanner>>,
}

struct ProbeTable(SchemaRef);

impl TableSource for ProbeTable {
    fn schema(&self) -> SchemaRef {
        self.0.clone()
    }
}

impl ContextProvider for Catalog {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        if name == TableReference::bare("t")
            && let Some(table) = &self.table
        {
            return Ok(table.clone());
        }
        plan_err!("No table {name} in the probe catalog")
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.signature_functions
            .then(|| signatures::function(name))
            .flatten()
    }
    fn get_type_planner(&self) -> Option<Arc<dyn datafusion_expr::planner::TypePlanner>> {
        self.signature_functions
            .then(|| Arc::new(signatures::Types) as _)
    }
    fn get_expr_planners(&self) -> &[Arc<dyn ExprPlanner>] {
        &self.expr_planners
    }
    fn get_relation_planners(&self) -> &[Arc<dyn RelationPlanner>] {
        &self.relation_planners
    }
    fn get_higher_order_meta(&self, _: &str) -> Option<Arc<HigherOrderUDF>> {
        None
    }
    fn get_aggregate_meta(&self, _: &str) -> Option<Arc<AggregateUDF>> {
        None
    }
    fn get_window_meta(&self, _: &str) -> Option<Arc<WindowUDF>> {
        None
    }
    fn get_variable_type(&self, _: &[String]) -> Option<DataType> {
        None
    }
    fn options(&self) -> &ConfigOptions {
        &self.options
    }
    fn udf_names(&self) -> Vec<String> {
        if self.signature_functions {
            return feldera_type_derivation_probe::FUNCTIONS
                .iter()
                .map(|name| name.to_ascii_lowercase())
                .collect();
        }
        Vec::new()
    }
    fn higher_order_function_names(&self) -> Vec<String> {
        Vec::new()
    }
    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }
    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

fn bind_generated_program(program: &str) -> Result<serde_json::Value> {
    let statements = Parser::parse_sql(&GenericDialect {}, program)
        .map_err(|e| datafusion_common::plan_datafusion_err!("parse: {e}"))?;
    let [Statement::CreateTable(table), Statement::CreateView(view)] = statements.as_slice() else {
        return plan_err!("expected the two generated table/view statements");
    };
    if table.name.to_string() != "t"
        || view.name.to_string() != "v"
        || table.columns.is_empty()
        || table.columns.len() > 2
        || !table.constraints.is_empty()
        || table.query.is_some()
        || !view.columns.is_empty()
    {
        return plan_err!("outside generated table/view scope");
    }
    let fields = table
        .columns
        .iter()
        .map(|column| {
            let nullable = match column.options.as_slice() {
                [] => true,
                [option] if matches!(option.option, ColumnOption::NotNull) => false,
                _ => return plan_err!("unexpected generated column options"),
            };
            signatures::ast_field(&column.name.value, &column.data_type, nullable)
        })
        .collect::<Result<Vec<_>>>()?;
    let catalog = Catalog {
        table: Some(Arc::new(ProbeTable(Arc::new(Schema::new(fields))))),
        signature_functions: true,
        expr_planners: vec![Arc::new(arithmetic::Arithmetic)],
        ..Catalog::default()
    };
    let SetExpr::Select(select) = view.query.body.as_ref() else {
        return plan_err!("expected generated SELECT");
    };
    let [item] = select.projection.as_slice() else {
        return plan_err!("expected one generated projection");
    };
    let span = item.span();
    let plan =
        SqlToRel::new(&catalog).sql_statement_to_plan(Statement::Query(view.query.clone()))?;
    let [field] = plan.schema().fields().as_ref() else {
        return plan_err!("expected one planned field");
    };
    Ok(serde_json::json!({
        "name": field.name(),
        "type": signatures::sql_type(field)?,
        "projection_span": [span.start.line, span.start.column, span.end.line, span.end.column],
    }))
}

#[derive(Deserialize)]
struct ProgramRecord {
    id: u64,
    schema_version: u32,
    program: String,
}

fn run_manifest(input: &str, output: &str) -> std::result::Result<(), Box<dyn std::error::Error>> {
    let source = BufReader::new(File::open(input)?);
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output)?,
    );
    let mut seen = std::collections::HashSet::new();
    for line in source.lines() {
        let line = line?;
        if line.len() > 64 * 1024 || seen.len() >= 100_000 {
            return Err("signature manifest exceeds probe limits".into());
        }
        let record: ProgramRecord = serde_json::from_str(&line)?;
        if record.schema_version != 1 || !seen.insert(record.id) {
            return Err("invalid manifest version or duplicate probe ID".into());
        }
        let value = match bind_generated_program(&record.program) {
            Ok(field) => serde_json::json!({"id": record.id, "accepted": true, "field": field}),
            Err(error) => {
                serde_json::json!({"id": record.id, "accepted": false, "diagnostic": error.to_string()})
            }
        };
        serde_json::to_writer(&mut output, &value)?;
        writeln!(output)?;
    }
    output.flush()?;
    Ok(())
}

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if let [flag, input, output] = args.as_slice()
        && flag == "--signature-manifest"
    {
        return run_manifest(input, output);
    }
    let (mode, sql) = match args.as_slice() {
        [sql] => ("", sql),
        [flag, sql] if matches!(flag.as_str(), "--preserve-set-operations" | "--signature-functions") =>
            (flag.as_str(), sql),
        _ => return Err("usage: feldera-datafusion-binder-probe [--preserve-set-operations|--signature-functions] SQL".into()),
    };
    if sql.len() > 64 * 1024 {
        return Err("SQL exceeds 64 KiB".into());
    }
    let catalog = Catalog {
        signature_functions: mode == "--signature-functions",
        expr_planners: if mode == "--signature-functions" {
            vec![Arc::new(arithmetic::Arithmetic)]
        } else {
            Vec::new()
        },
        relation_planners: if mode == "--signature-functions" {
            vec![Arc::new(values::DerivedValues)]
        } else {
            Vec::new()
        },
        ..Catalog::default()
    };
    let planner = SqlToRel::new(&catalog);
    for statement in Parser::parse_sql(&GenericDialect {}, sql)? {
        if mode == "--preserve-set-operations" {
            let Statement::Query(query) = statement else {
                return Err("set operation probe expects a query".into());
            };
            let relation = set_operations::bind(&planner, *query)?;
            println!(
                "{}",
                serde_json::json!({"relation": &relation, "rows": relation.output()})
            );
        } else {
            let plan = planner.sql_statement_to_plan(statement)?;
            println!("{}", plan.display_indent_schema());
            if mode == "--signature-functions" {
                let types = plan
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| signatures::sql_type(field))
                    .collect::<Result<Vec<_>>>()?;
                println!("{}", serde_json::to_string(&types)?);
            }
        }
    }
    Ok(())
}
