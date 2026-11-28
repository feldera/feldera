//! Assemble bounded S1 programs over the fixed compiler system prelude.

mod array;
mod graph;
mod insert;
mod literal;
mod literal_type;
mod math;
mod names;
mod order;
mod source;
mod table;
mod timestamp_diff;

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
use names::OutputNames;
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
    let projections = select
        .projection
        .iter()
        .map(|item| match item {
            SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
            SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias))),
            _ => Err(unsupported("expected a scalar SELECT item")),
        })
        .collect::<Result<Vec<_>, _>>()?;
    // The reference executor evaluates a project's reducible expressions in one
    // batch. A checked arithmetic failure leaves every candidate unreduced.
    let fold = projections.len() == 1
        || !projections.iter().any(|(expr, _)| {
            let (inner, _) = without_parentheses(expr);
            matches!(inner, super::Expr::BinaryOp { .. })
                && super::bind(expr, &scope, 0)
                    .is_ok_and(|bound| super::constant::runtime_error(&bound))
        });
    let mut reduced = false;
    for (index, (expr, alias)) in projections.into_iter().enumerate() {
        let ConstantOperand {
            root,
            ty,
            folded,
            mut display,
            reduction_changed,
        } = constant_operand(program, &scope, tokens, expr, fold)?;
        reduced |= reduction_changed;
        let field = match alias {
            Some(ident) => projection_alias(ident, &mut display.pretty, &mut display.diagnostic),
            None => Name {
                name: format!("EXPR${index}"),
                quoted: true,
            },
        };
        all_literals &= folded;
        roots.push(root);
        fields.push((field, ty));
        pretty.push(display.pretty);
        diagnostic.push(display.diagnostic);
    }
    if reduced {
        array::replace_folded_constructors(program, &roots);
    }
    let text = QueryText {
        pretty: format!("SELECT {}", pretty.join(", ")),
        diagnostic: format!("SELECT {}", diagnostic.join(", ")),
    };
    let row = projected_row(program, &fields);
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
            relation,
            text,
        },
    )
}

struct ConstantOperand {
    root: feldera_sql_rel_model::expressions::ExprId,
    ty: TypeId,
    folded: bool,
    reduction_changed: bool,
    display: source::SqlDisplay,
}

fn constant_operand(
    program: &mut Program,
    scope: &super::ColumnScope<'_>,
    tokens: &DeclarationTokens<'_>,
    expr: &super::Expr,
    fold: bool,
) -> Result<ConstantOperand, Failure> {
    use sqlparser_064::ast::Spanned;
    if let Ok(cell) = literal::Cell::parse(expr) {
        let folded = !cell.needs_non_bmp_wrapper();
        let (root, ty) = cell.emit(program, tokens, expr)?;
        // The validator canonicalizes UUID literals in SELECT. INSERT keeps
        // the spelling from its unvalidated VALUES clause.
        let display = match &cell.value {
            feldera_sql_rel_model::literals::LiteralValue::Uuid { bytes } => {
                source::SqlDisplay::atom(format!(
                    "UUID '{}'",
                    feldera_sql_rel_model::literals::uuid_to_text(*bytes)
                ))
            }
            _ => source::SqlDisplay::literal(&cell.text),
        };
        return Ok(ConstantOperand {
            root,
            ty,
            folded,
            reduction_changed: false,
            display,
        });
    }

    if let Some(operand) = constant_call(program, tokens, expr, fold)? {
        return Ok(operand);
    }
    let (bound, range, warning) = super::bind_projection_value(scope, expr, expr.span(), fold)?;
    if warning.is_some() {
        return Err(unsupported(
            "constant program has unsupported frontend diagnostics",
        ));
    }
    let reduction_changed = matches!(bound.kind, super::ExpressionKind::FoldedLiteral { .. });
    let display = display_constant(expr)?;
    let fragment = super::frel::emit(scope.sql, &bound, super::span(range), None)?;
    let (root, ty, folded) = append_expression(program, fragment)?;
    Ok(ConstantOperand {
        root,
        ty,
        folded,
        reduction_changed,
        display,
    })
}

fn without_parentheses(mut expr: &super::Expr) -> (&super::Expr, usize) {
    let mut count = 0;
    while let super::Expr::Nested(inner) = expr {
        count += 1;
        expr = inner;
    }
    (expr, count)
}

fn constant_call(
    program: &mut Program,
    tokens: &DeclarationTokens<'_>,
    expr: &super::Expr,
    fold: bool,
) -> Result<Option<ConstantOperand>, Failure> {
    use super::{Expr, Spanned};
    let (expr, enclosing) = without_parentheses(expr);
    if let Expr::Array(array) = expr {
        return array::LiteralArray::parse(array)?
            .projection(program, tokens, expr.span(), enclosing, fold)
            .map(Some);
    }
    let Expr::Function(function) = expr else {
        return Ok(None);
    };
    let (name, arguments) = super::scalar_args(function)?;
    let name = name.to_ascii_uppercase();
    let position = || tokens.call_position(function.name.span(), enclosing);
    if matches!(name.as_str(), "ARRAY_LENGTH" | "ARRAY_SIZE" | "CARDINALITY") {
        return array::length(program, tokens, &name, &arguments, position()?, fold).map(Some);
    }
    if name == "TIMESTAMPDIFF" {
        return timestamp_diff::bind(program, tokens, &arguments, position()?).map(Some);
    }
    math::trigonometric_call(program, tokens, &name, &arguments, position, fold)
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

/// The statement keeps the alias quotes. Calcite derives the result field's
/// case sensitivity separately, from the resolved spelling.
fn projection_alias(
    ident: &sqlparser_064::ast::Ident,
    pretty: &mut String,
    diagnostic: &mut String,
) -> Name {
    let (mut name, spelling) = table::identifier(ident);
    pretty.push_str(" AS ");
    pretty.push_str(spelling.spelling(true));
    diagnostic.push_str(" AS ");
    diagnostic.push_str(spelling.spelling(false));
    name.quoted = name.name != name.name.to_lowercase();
    name
}

struct QueryText {
    pretty: String,
    diagnostic: String,
}

struct BoundQuery {
    kind: &'static str,
    fields: Vec<(Name, TypeId)>,
    relation: u32,
    text: QueryText,
}

/// Project and Values operators require distinct field names. The validated
/// SELECT row, stored separately in the view root, retains duplicate aliases.
fn projected_row(program: &mut Program, fields: &[(Name, TypeId)]) -> TypeId {
    let mut names = OutputNames::default();
    insert_row_fields(
        program,
        fields.iter().map(|(name, ty)| {
            let field = names
                .disambiguate(&name.name)
                .unwrap_or_else(|| name.name.clone());
            (field, *ty)
        }),
    )
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
    let row = insert_row_fields(
        program,
        fields.iter().map(|(name, ty)| (name.name.as_str(), *ty)),
    );
    let mut names = OutputNames::default();
    let diagnostic = statement.description();
    let schema = append_schema(
        program,
        &name,
        diagnostic.clone(),
        fields
            .iter()
            .enumerate()
            .map(|(index, (field, ty))| {
                let mut name = columns.get(index).unwrap_or(field).clone();
                if let Some(unique) = names.disambiguate(&name.name) {
                    name.name = unique;
                }
                column(name, index, *ty, diagnostic.clone())
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
