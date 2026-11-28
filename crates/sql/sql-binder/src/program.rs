//! Assemble bounded S1 programs over the fixed compiler system prelude.

mod graph;
mod insert;
mod literal;
mod order;
mod source;
mod table;

pub use table::bind_table_program;

use super::{
    Failure, SelectItem, Statement as ParsedStatement, Submission, Upstream, parse_failure,
    plain_select, unsupported,
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
use graph::{
    append_expression, append_project, append_values, insert_row_fields, reserve_environment,
};
use source::{DeclarationTokens, display_constant};

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
    let mut starts = Vec::new();
    let statements = feldera_sql_parser::parse_at_with_starts(sql, 0, |loc| starts.push(loc))
        .map_err(parse_failure)?;
    bind_statements(id, &statements, &starts, sql, true)
}

/// Bind a batch of API submissions, preserving per-call statement boundaries
/// and source lines even when adjacent submissions omit their semicolon.
pub fn bind_submissions(id: String, submissions: &[Submission]) -> Result<Program, Failure> {
    let mut starts = Vec::new();
    let (statements, source) = super::parse_submissions(submissions, |loc| starts.push(loc))?;
    let mut program = bind_statements(
        id,
        &statements,
        &starts,
        &source,
        submissions.iter().any(|s| s.many),
    )?;
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
    starts: &[crate::Location],
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
            table::bind_parsed_table(id, prelude, statements, starts, sql, many)
        }
        [ParsedStatement::CreateType(_), ..] => {
            table::bind_parsed_table(id, prelude, statements, starts, sql, many)
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
    let lines: Vec<_> = sql.split('\n').collect();
    let tokens = DeclarationTokens::single(sql, &lines)?;
    set_source(&mut program, id, sql, many);
    append_constant_view(&mut program, parsed, &tokens)?;
    finish(program)
}

fn append_constant_view(
    program: &mut Program,
    parsed: &[ParsedStatement],
    tokens: &DeclarationTokens<'_>,
) -> Result<(), Failure> {
    let [ParsedStatement::Upstream(statement)] = parsed else {
        return Err(unsupported("expected one constant view"));
    };
    let Upstream::CreateView(view) = statement.as_ref() else {
        return Err(unsupported("expected one constant view"));
    };
    let select = plain_select(&view.query)?;
    if !select.from.is_empty() {
        return Err(unsupported("expected a view over one constant row"));
    }
    super::validate_plain_view(view)?;
    let declaration = ViewDeclaration::new(view)?;
    if super::system_relation(&declaration.name.name)
        || program
            .catalog
            .schemas
            .iter()
            .any(|schema| schema.name == declaration.name)
    {
        return Err(unsupported("view name already exists"));
    }

    let mut fields = Vec::new();
    let mut names = std::collections::HashSet::new();
    let mut roots = Vec::new();
    let mut pretty = Vec::new();
    let mut diagnostic = Vec::new();
    let mut all_literals = true;
    let scope = super::ColumnScope {
        columns: &[],
        qualifier: None,
        sql: tokens.sql,
        preserve_parentheses: true,
        tokens: Default::default(),
    };
    for (index, item) in select.projection.iter().enumerate() {
        let (expr, alias) = match item {
            SelectItem::UnnamedExpr(expr) => (expr, None),
            SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias)),
            _ => return Err(unsupported("expected a scalar SELECT item")),
        };
        let ConstantOperand {
            root,
            ty,
            folded,
            mut display,
        } = constant_operand(program, &scope, tokens, expr)?;
        let field = match alias {
            Some(ident) => {
                let quoted = ident.quote_style.is_some();
                let name = if quoted {
                    ident.value.clone()
                } else {
                    ident.value.to_lowercase()
                };
                display
                    .pretty
                    .push_str(&format!(" AS `{}`", name.replace('`', "``")));
                let printed = if quoted {
                    ident.to_string()
                } else {
                    name.clone()
                };
                display.diagnostic.push_str(&format!(" AS {printed}"));
                // Calcite derives catalog case sensitivity from the resolved
                // field spelling. The statement text retains the SQL quotes.
                Name {
                    quoted: name != name.to_lowercase(),
                    name,
                }
            }
            None => Name {
                name: format!("EXPR${index}"),
                quoted: true,
            },
        };
        if !names.insert(field.name.clone()) {
            return Err(unsupported("duplicate output name in constant projection"));
        }
        all_literals &= folded;
        roots.push(root);
        fields.push((field, ty));
        pretty.push(display.pretty);
        diagnostic.push(display.diagnostic);
    }
    let text = QueryText {
        pretty: format!("SELECT {}", pretty.join(", ")),
        diagnostic: format!("SELECT {}", diagnostic.join(", ")),
    };
    let row = insert_row_fields(
        program,
        fields.iter().map(|(name, ty)| (name.name.as_str(), *ty)),
    );
    let relation = program.table.relations;
    if all_literals {
        append_values(program, row, 1, roots)?;
    } else {
        append_project(program, row, roots)?;
    }
    append_view(
        program,
        tokens.range()?,
        declaration,
        BoundQuery {
            kind: "SELECT",
            fields,
            row,
            relation,
            text,
        },
    )
}

struct ConstantOperand {
    root: feldera_sql_rel_model::expressions::ExprId,
    ty: TypeId,
    folded: bool,
    display: source::SqlDisplay,
}

fn constant_operand(
    program: &mut Program,
    scope: &super::ColumnScope<'_>,
    tokens: &DeclarationTokens<'_>,
    expr: &super::Expr,
) -> Result<ConstantOperand, Failure> {
    use sqlparser_064::ast::Spanned;
    if let Ok(cell) = literal::Cell::parse(expr) {
        let folded = !cell.needs_non_bmp_wrapper();
        let (root, ty) = cell.emit(program, tokens, expr)?;
        // The validator canonicalizes UUID literals in SELECT. INSERT keeps
        // the spelling from its unvalidated VALUES clause.
        let spelling = match &cell.value {
            feldera_sql_rel_model::literals::LiteralValue::Uuid { bytes } => {
                format!(
                    "UUID '{}'",
                    feldera_sql_rel_model::literals::uuid_to_text(*bytes)
                )
            }
            _ => cell.sql,
        };
        return Ok(ConstantOperand {
            root,
            ty,
            folded,
            display: source::SqlDisplay::atom(spelling),
        });
    }

    let (bound, range, warning) = super::bind_projection_value(scope, expr, expr.span(), true)?;
    if warning.is_some() {
        return Err(unsupported(
            "constant program has unsupported frontend diagnostics",
        ));
    }
    let display = display_constant(expr)?;
    let fragment = super::frel::emit(scope.sql, &bound, super::span(range), None)?;
    let (root, ty, folded) = append_expression(program, fragment)?;
    Ok(ConstantOperand {
        root,
        ty,
        folded,
        display,
    })
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

struct ViewDeclaration {
    name: Name,
    kind: ViewKind,
    columns: Vec<Name>,
}

impl ViewDeclaration {
    fn new(view: &sqlparser_064::ast::CreateView) -> Result<Self, Failure> {
        let kind = if view.local.is_some() {
            ViewKind::Local
        } else if view.materialized {
            ViewKind::Materialized
        } else {
            ViewKind::Standard
        };
        let ident = super::local_name(&view.name)?;
        let name = Name {
            name: ident.value.to_lowercase(),
            quoted: false,
        };
        let mut columns = Vec::with_capacity(view.columns.len());
        let mut seen = std::collections::HashSet::new();
        for col in &view.columns {
            if col.name.quote_style.is_some() || col.data_type.is_some() || col.options.is_some() {
                return Err(unsupported("expected an unquoted view column name"));
            }
            let name = col.name.value.to_lowercase();
            if !seen.insert(name.clone()) {
                return Err(unsupported("duplicate declared view column"));
            }
            columns.push(Name {
                name,
                quoted: false,
            });
        }
        Ok(Self {
            name,
            kind,
            columns,
        })
    }

    fn sql(&self, quoted: bool) -> String {
        let render = |name: &Name| {
            if quoted {
                format!("`{}`", name.name)
            } else {
                name.name.clone()
            }
        };
        let mut header = format!("{} {}", self.kind.create_sql(), render(&self.name));
        if !self.columns.is_empty() {
            let columns = self
                .columns
                .iter()
                .map(render)
                .collect::<Vec<_>>()
                .join(", ");
            header.push_str(&format!(" ({columns})"));
        }
        header
    }
}

struct QueryText {
    pretty: String,
    diagnostic: String,
}

struct BoundQuery {
    kind: &'static str,
    fields: Vec<(Name, TypeId)>,
    row: TypeId,
    relation: u32,
    text: QueryText,
}

fn append_view(
    program: &mut Program,
    position: Coordinates,
    declaration: ViewDeclaration,
    query: BoundQuery,
) -> Result<(), Failure> {
    let BoundQuery {
        kind,
        fields,
        row,
        relation,
        text,
    } = query;
    let statement = StatementText {
        position,
        pretty: format!("{} AS\n{}", declaration.sql(true), text.pretty),
        diagnostic: format!("{} AS\n{}", declaration.sql(false), text.diagnostic),
    };
    let ViewDeclaration {
        name,
        kind: view_kind,
        columns,
    } = declaration;
    if !columns.is_empty() && columns.len() != fields.len() {
        return Err(unsupported("view column count differs from SELECT output"));
    }
    let diagnostic = statement.description();
    let schema = append_schema(
        program,
        &name,
        diagnostic.clone(),
        fields
            .iter()
            .enumerate()
            .map(|(index, (field, ty))| {
                column(
                    columns.get(index).unwrap_or(field).clone(),
                    index,
                    *ty,
                    diagnostic.clone(),
                )
            })
            .collect(),
    )?;
    program.producers.roots.push(relation);
    append_boundary(program, &statement);
    program.statements.push(Statement::CreateView {
        source: statement.source("CREATE_VIEW"),
        schema,
        name,
        view_kind,
        emit_final: -1,
        root: RelRoot {
            relation,
            validated_type: row,
            kind: kind.into(),
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
