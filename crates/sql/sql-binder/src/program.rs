//! Assemble bounded S1 programs over the fixed compiler system prelude.

mod graph;
mod order;
mod source;
mod table;

pub use table::bind_table_program;

use super::{
    Failure, SelectItem, Statement as ParsedStatement, Submission, Upstream, bind_projection,
    parse_failure, plain_select, unsupported,
};
use feldera_sql_rel_model::{
    TypeId,
    catalog::{CatalogSchema, Column, Name},
    compilation_context::{InputSource, SourceState},
    diagnostics::DiagnosticBoundary,
    expression_objects::ValueUse,
    input_uses::InputOwner,
    operators::DiagnosticSource,
    program::Program,
    relation_metadata::Coordinates,
    source::{Origin, Position, SourceId},
    statements::{RelRoot, ResultField, Statement, StatementSource, ViewKind},
};
use graph::{append_expression, append_project, append_values, insert_row, reserve_environment};
use source::{display_constant, statement_position};

const USER: SourceId = SourceId(0);

/// Bind single-row constant views against an empty-user-source prelude.
/// The prelude supplies only compiler-generated statements and their context.
pub fn bind_constant_program(
    id: String,
    program: Program,
    submission: &Submission,
) -> Result<Program, Failure> {
    if submission.batch != 0 {
        return Err(unsupported("constant program requires one SQL batch"));
    }
    let sql = submission.sql.trim_end_matches('\n');
    let parsed = super::parse_feldera(sql).map_err(parse_failure)?;
    bind_parsed_constant(id, program, &parsed, sql, submission.many)
}

/// Compile one SQL submission using the built-in system catalog in the Gen-2 profile.
/// The bootstrap contains only compiler-generated objects and an empty user source.
pub fn bind_sql(id: String, source: &str) -> Result<Program, Failure> {
    let sql = source.trim_end_matches('\n');
    let statements = super::parse_feldera(sql).map_err(parse_failure)?;
    bind_statements(id, &statements, sql, true)
}

/// Bind a batch of API submissions, preserving per-call statement boundaries
/// and source lines even when adjacent submissions omit their semicolon.
pub fn bind_submissions(id: String, submissions: &[Submission]) -> Result<Program, Failure> {
    let (statements, source) = super::parse_submissions(submissions)?;
    let mut program =
        bind_statements(id, &statements, &source, submissions.iter().any(|s| s.many))?;
    // The API retains each submitted string verbatim in its whole-program buffer.
    // Its separate line index drops trailing empty lines from each append.
    let text = &mut program.context.sources.program;
    text.clear();
    for submission in submissions {
        text.push_str(&submission.sql);
        text.push('\n');
    }
    Ok(program)
}

fn bind_statements(
    id: String,
    statements: &[ParsedStatement],
    sql: &str,
    many: bool,
) -> Result<Program, Failure> {
    let prelude: Program = serde_json::from_str(include_str!("system_catalog.json"))
        .map_err(|e| unsupported(format!("invalid compiler system catalog: {e}")))?;
    match statements {
        [ParsedStatement::Upstream(statement)]
            if matches!(statement.as_ref(), Upstream::CreateView(_)) =>
        {
            bind_parsed_constant(id, prelude, statements, sql, many)
        }
        [ParsedStatement::Upstream(statement), ..]
            if matches!(
                statement.as_ref(),
                Upstream::CreateTable(_) | Upstream::CreateView(_)
            ) =>
        {
            table::bind_parsed_table(id, prelude, statements, sql, many)
        }
        _ => Err(unsupported(
            "expected a constant view or a table followed by a view",
        )),
    }
}

fn bind_parsed_constant(
    id: String,
    mut program: Program,
    parsed: &[ParsedStatement],
    sql: &str,
    many: bool,
) -> Result<Program, Failure> {
    check_prelude(&program)?;
    if !sql.is_ascii() {
        return Err(unsupported("constant program source must be ASCII"));
    }
    set_source(&mut program, id, sql, many);
    append_constant_view(&mut program, parsed, sql, statement_position(sql)?)?;
    finish(program)
}

fn append_constant_view(
    program: &mut Program,
    parsed: &[ParsedStatement],
    sql: &str,
    position: Coordinates,
) -> Result<(), Failure> {
    let [ParsedStatement::Upstream(statement)] = parsed else {
        return Err(unsupported("expected one constant view"));
    };
    let Upstream::CreateView(view) = statement.as_ref() else {
        return Err(unsupported("expected one constant view"));
    };
    let select = plain_select(&view.query)?;
    let (expression, alias) = match select.projection.as_slice() {
        [SelectItem::UnnamedExpr(expression)] => (expression, None),
        [SelectItem::ExprWithAlias { expr, alias }] if alias.quote_style.is_none() => {
            (expr, Some(alias.value.to_ascii_lowercase()))
        }
        _ => {
            return Err(unsupported(
                "expected one scalar projection with an optional plain alias",
            ));
        }
    };
    if !select.from.is_empty() || view.materialized || view.local.is_some() {
        return Err(unsupported(
            "expected a standard view over one constant row",
        ));
    }
    let bound = bind_projection(sql, parsed, false)?;
    if !bound.compiler_diagnostics.is_empty() {
        return Err(unsupported(
            "constant program has unsupported frontend diagnostics",
        ));
    }
    if program
        .catalog
        .schemas
        .iter()
        .any(|schema| schema.name.name == bound.view_name)
    {
        return Err(unsupported("view name already exists"));
    }
    let mut display = display_constant(expression)?;
    if let Some(alias) = alias {
        display.pretty.push_str(&format!(" AS `{alias}`"));
        display.diagnostic.push_str(&format!(" AS {alias}"));
    }
    let statement = StatementText {
        position,
        pretty: format!(
            "CREATE VIEW `{}` AS\nSELECT {}",
            bound.view_name, display.pretty
        ),
        diagnostic: format!(
            "CREATE VIEW {} AS\nSELECT {}",
            bound.view_name, display.diagnostic
        ),
    };

    let fragment = bound.frel_expression(sql)?;
    let (expression, scalar, folded) = append_expression(program, fragment)?;
    let field_name = Name {
        name: if bound.output_quoted {
            bound.name
        } else {
            bound.name.to_ascii_lowercase()
        },
        quoted: bound.output_quoted,
    };
    let row = insert_row(program, &field_name.name, scalar);

    let relation = program.table.relations;
    if folded {
        append_values(program, row, Some(expression))?;
    } else {
        append_project(program, row, expression)?;
    }

    let view_name = Name {
        name: bound.view_name,
        quoted: false,
    };
    append_view(
        program,
        statement,
        view_name,
        vec![(field_name, scalar)],
        row,
        relation,
    )
}

fn check_prelude(program: &Program) -> Result<(), Failure> {
    program
        .validate(Default::default(), Default::default(), Default::default())
        .map_err(|error| unsupported(error.to_string()))?;
    if program
        .table
        .type_table
        .sources
        .first()
        .is_none_or(|source| !source.text.trim().is_empty())
        || program
            .statements
            .iter()
            .any(|statement| statement.source().visible)
    {
        return Err(unsupported("expected only the system prelude"));
    }
    Ok(())
}

fn set_source(program: &mut Program, id: String, sql: &str, many: bool) {
    program.table.type_table.sources[0].text = sql.into();
    program.id = id;
    program.context.input_source = if many {
        InputSource::Api
    } else {
        InputSource::None
    };
    program.context.sources = SourceState {
        file: None,
        program: format!("{sql}\n"),
        lines: sql.split('\n').map(str::to_owned).collect(),
    };
}

struct StatementText {
    position: Coordinates,
    pretty: String,
    diagnostic: String,
}

impl StatementText {
    fn source(self, kind: &str) -> StatementSource {
        StatementSource {
            source: USER,
            kind: kind.into(),
            visible: true,
            statement_text: self.pretty,
            diagnostic_text: self.diagnostic,
            position: self.position,
            position_internal: Some(false),
        }
    }

    fn description(&self) -> DiagnosticSource {
        diagnostic_source(&self.position, &self.diagnostic)
    }
}

fn diagnostic_source(position: &Coordinates, text: &str) -> DiagnosticSource {
    DiagnosticSource {
        origin: source_origin(position),
        text: text.into(),
        internal_text: text.into(),
        empty: false,
    }
}

fn source_origin(position: &Coordinates) -> Origin {
    Origin::Source {
        source: USER,
        start: Position {
            line: position.line,
            column: position.column,
        },
        end: Position {
            line: position.end_line,
            column: position.end_column,
        },
    }
}

fn append_schema(
    program: &mut Program,
    name: &Name,
    diagnostic: DiagnosticSource,
    columns: Vec<Column>,
) -> Result<u32, Failure> {
    let value_environment =
        reserve_environment(program, |owner| matches!(owner, InputOwner::Constant {}))?;
    let values = program.expressions.value_uses.len() as u32;
    program.expressions.value_uses.push(ValueUse {
        environment: value_environment,
        roots: Vec::new(),
    });
    let schema = program.catalog.schemas.len() as u32;
    program.catalog.schemas.push(CatalogSchema {
        source: USER,
        name: name.clone(),
        diagnostic,
        values,
        columns,
        properties: None,
    });
    Ok(schema)
}

fn column(name: Name, index: usize, ty: TypeId, diagnostic: DiagnosticSource) -> Column {
    Column {
        name,
        index: index as u32,
        ty,
        diagnostic,
        primary_key: false,
        unused: false,
        lateness: None,
        default_value: None,
        default_position: None,
    }
}

fn append_view(
    program: &mut Program,
    statement: StatementText,
    name: Name,
    fields: Vec<(Name, TypeId)>,
    row: TypeId,
    relation: u32,
) -> Result<(), Failure> {
    let diagnostic = statement.description();
    let schema = append_schema(
        program,
        &name,
        diagnostic.clone(),
        fields
            .iter()
            .enumerate()
            .map(|(index, (field, ty))| column(field.clone(), index, *ty, diagnostic.clone()))
            .collect(),
    )?;
    program.producers.roots.push(relation);
    append_boundary(program, &statement);
    program.statements.push(Statement::CreateView {
        source: statement.source("CREATE_VIEW"),
        schema,
        name,
        view_kind: ViewKind::Standard,
        emit_final: -1,
        root: RelRoot {
            relation,
            validated_type: row,
            kind: "SELECT".into(),
            fields: fields
                .into_iter()
                .enumerate()
                .map(|(index, (field, _))| ResultField {
                    index: index as u32,
                    name: field.name,
                })
                .collect(),
            collation: Vec::new(),
            hints: Vec::new(),
        },
    });
    Ok(())
}

fn append_boundary(program: &mut Program, statement: &StatementText) {
    program.diagnostics.before.push(DiagnosticBoundary {
        context: source_origin(&statement.position),
        messages: Vec::new(),
    });
}

fn finish(mut program: Program) -> Result<Program, Failure> {
    order::number_relations(&mut program)?;
    program
        .validate(Default::default(), Default::default(), Default::default())
        .map_err(|error| unsupported(error.to_string()))?;
    Ok(program)
}
