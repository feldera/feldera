//! Bind projections over one plain user table.

mod aggregate;
mod compute;
mod declarations;
mod filter;
mod sets;
mod types;

use types::declare_type;
pub(super) use types::{TypeSpelling, null_cast_type};

use super::{
    BoundQuery, QueryText, StatementText, ViewDeclaration, append_boundary, append_schema,
    append_view, check_prelude, column, diagnostic_source, finish,
    graph::{
        ProjectInput, ProjectValue, TableOperand, append_table_filter, append_table_project,
        append_table_scan, append_values, insert_row_fields,
    },
    set_source,
    source::DeclarationTokens,
};
use crate::{
    Expr, Failure, SelectItem, Spanned, Statement as ParsedStatement, Submission, Upstream,
    local_name, map_table_columns, parse_submissions, projection_input, system_relation,
    unsupported, unsupported_niladic, validate_plain_view,
};
use feldera_sql_rel_model::{
    TypeId, ValidationLimits,
    catalog::Name,
    program::Program,
    relation_metadata::{Coordinates, Hint},
    statements::Statement,
    types::TypeKind,
};
use sqlparser_064::ast::{
    ColumnOption, GroupByExpr, Ident, TableConstraint, WildcardAdditionalOptions,
};
use std::collections::HashSet;

struct DeclaredColumn {
    name: Name,
    ty: TypeId,
    declaration: TypeSpelling,
    primary_key: bool,
}

struct Projection {
    name: Name,
    value: ProjectValue,
    pretty: String,
    diagnostic: String,
}

/// Construct a typed table and view without consuming Java user-statement IR.
pub fn bind_table_program(
    id: String,
    program: Program,
    submissions: &[Submission],
) -> Result<Program, Failure> {
    let many = submissions.iter().any(|s| s.many);
    let mut starts = Vec::new();
    let (parsed, sql) = parse_submissions(submissions, |loc| starts.push(loc))?;
    bind_parsed_table(id, program, &parsed, &starts, &sql, many)
}

pub(super) fn bind_parsed_table(
    id: String,
    mut program: Program,
    parsed: &[ParsedStatement],
    starts: &[crate::Location],
    sql: &str,
    many: bool,
) -> Result<Program, Failure> {
    check_prelude(&program)?;
    let lines: Vec<_> = sql.split('\n').collect();
    let declarations = DeclarationTokens::split(sql, &lines, starts)?;
    if declarations.len() != parsed.len() {
        return Err(unsupported(
            "source declaration count differs from parsed statements",
        ));
    }
    set_source(&mut program, id, sql, many);
    let aliases = declarations::register(&mut program, parsed, &declarations)?;
    for (parsed_statement, tokens) in parsed.iter().zip(&declarations) {
        if let ParsedStatement::CreateType(declaration) = parsed_statement {
            match &declaration.definition {
                feldera_sql_parser::TypeDefinition::Alias(ty)
                    if types::warns_on_unspecified_precision(ty) =>
                {
                    types::warn_decimal(&mut program, tokens.alias_type_position()?);
                }
                feldera_sql_parser::TypeDefinition::Struct(fields) => {
                    for field in fields {
                        if types::warns_on_unspecified_precision(&field.data_type) {
                            types::warn_decimal(
                                &mut program,
                                tokens.column_type_position(field.name.span)?,
                            );
                        }
                    }
                }
                _ => {}
            }
            continue;
        }
        let ParsedStatement::Upstream(statement) = parsed_statement else {
            return Err(unsupported("expected CREATE TABLE or CREATE VIEW"));
        };
        match statement.as_ref() {
            Upstream::CreateTable(table) => {
                declare_table(&mut program, table, tokens, &aliases)?;
            }
            Upstream::CreateView(view)
                if view
                    .query
                    .body
                    .as_select()
                    .is_some_and(|select| select.from.is_empty()) =>
            {
                super::append_constant_view(
                    &mut program,
                    std::slice::from_ref(parsed_statement),
                    tokens,
                )?;
            }
            Upstream::CreateView(view) => bind_view(&mut program, view, tokens)?,
            Upstream::Insert(insert) => super::insert::bind(&mut program, insert, tokens)?,
            _ => {
                return Err(unsupported(
                    "expected a table, a view, or INSERT INTO ... VALUES",
                ));
            }
        }
    }
    finish(program)
}

struct TableColumn {
    name: Name,
    ty: TypeId,
}

fn declare_table(
    program: &mut Program,
    table: &sqlparser_064::ast::CreateTable,
    tokens: &DeclarationTokens,
    aliases: &declarations::Aliases,
) -> Result<(), Failure> {
    let table_ident = local_name(&table.name)?;
    let table_name = name(&table_ident.value);
    // Generated from the same operator table as SqlToRelCompiler.functionExists.
    static CONFLICTS: std::sync::LazyLock<HashSet<&'static str>> =
        std::sync::LazyLock::new(|| include_str!("reserved_table_names.txt").lines().collect());
    if CONFLICTS.contains(table_name.name.as_str()) {
        let message = format!(
            "Table '{}' has the same name as a predefined function",
            table_name.name
        );
        let diagnostic = crate::projection_diagnostic(
            &program.table.type_table.sources[0].text,
            table_ident.span,
            crate::DiagnosticKind::ReservedName,
            &message,
        );
        return Err(Failure {
            category: "reserved_name",
            span: Some(crate::span(table_ident.span)),
            compiler_diagnostic: diagnostic.map(Box::new),
            message,
            diagnostic_span: None,
        });
    }
    if system_relation(&table_name.name)
        || program
            .catalog
            .schemas
            .iter()
            .any(|schema| schema.name == table_name)
    {
        return Err(unsupported("table name already exists"));
    }
    let key = table_key(table)?;
    let mut unbound_key: HashSet<_> = key.iter().map(String::as_str).collect();
    let columns = map_table_columns(table, false, |column, nullable| {
        if column.options.iter().any(|option| option.name.is_some()) {
            return Err(unsupported(
                "named column constraints are outside the table slice",
            ));
        }
        let (ty, spelling) = declare_type(
            &mut program.table.type_table,
            aliases,
            &column.data_type,
            nullable,
            ValidationLimits::default().depth,
        )?;
        if types::warns_on_unspecified_precision(&column.data_type) {
            // Calcite checks top-level column type specs for the whole batch
            // before lowering the first compiler-owned statement.
            let position = tokens.column_type_position(column.name.span)?;
            types::warn_decimal(program, position);
        }
        let column_name = name(&column.name.value);
        let inline_key = column
            .options
            .iter()
            .any(|opt| matches!(opt.option, ColumnOption::PrimaryKey(_)));
        if inline_key && !key.is_empty() {
            return Err(unsupported(
                "table declares both an inline and a table primary key",
            ));
        }
        let primary_key = inline_key || unbound_key.remove(column_name.name.as_str());
        if primary_key && nullable {
            return Err(unsupported("primary key column is nullable"));
        }
        if primary_key
            && matches!(
                program.table.type_table.types[ty.0 as usize].kind,
                TypeKind::Float { .. }
            )
        {
            return Err(unsupported(
                "floating-point primary key requires a comparison warning",
            ));
        }
        let nullability = if nullable { "" } else { " NOT NULL" };
        let constraint = if inline_key { " PRIMARY KEY" } else { "" };
        Ok(DeclaredColumn {
            name: column_name,
            ty,
            declaration: TypeSpelling::render(|quoted| {
                format!("{}{nullability}{constraint}", spelling.spelling(quoted))
            }),
            primary_key,
        })
    })?;
    if !unbound_key.is_empty() {
        return Err(unsupported("primary key references an undeclared column"));
    }
    let positions = tokens.column_ranges(table.columns.iter().map(|col| col.name.span))?;
    let key_index = tokens.primary_key_position().map(|pos| {
        table
            .columns
            .partition_point(|col| col.name.span.start < pos)
    });
    let table_source = render_table(tokens.range()?, &table_name, &columns, key_index, &key);
    let table_columns = columns
        .iter()
        .zip(&positions)
        .enumerate()
        .map(|(index, (field, position))| {
            let text = format!("{} {}", field.name.name, field.declaration.diagnostic);
            let mut col = column(
                field.name.clone(),
                index,
                field.ty,
                diagnostic_source(position, &text),
            );
            col.primary_key = field.primary_key;
            col
        })
        .collect();
    let schema = append_schema(
        program,
        &table_name,
        table_source.description(),
        table_columns,
    )?;
    append_boundary(program, &table_source);
    program.statements.push(Statement::CreateTable {
        source: table_source.source("CREATE_TABLE"),
        schema,
        name_diagnostic: diagnostic_source(
            &tokens.coordinates(table_ident.span)?,
            &table_name.name,
        ),
        name: table_name,
        foreign_keys: Vec::new(),
    });

    Ok(())
}

fn table_key(table: &sqlparser_064::ast::CreateTable) -> Result<Vec<String>, Failure> {
    let key = match table.constraints.as_slice() {
        [] => return Ok(Vec::new()),
        [TableConstraint::PrimaryKey(key)] => key,
        _ => return Err(unsupported("expected a single table primary key")),
    };
    let mut seen = HashSet::new();
    key.columns
        .iter()
        .map(|column| match &column.column.expr {
            Expr::Identifier(ident) if ident.quote_style.is_none() => {
                let name = ident.value.to_lowercase();
                if !seen.insert(name.clone()) {
                    return Err(unsupported("duplicate primary key column"));
                }
                Ok(name)
            }
            _ => Err(unsupported("expected an unquoted primary key column")),
        })
        .collect()
}

fn bind_view(
    program: &mut Program,
    view: &sqlparser_064::ast::CreateView,
    tokens: &DeclarationTokens<'_>,
) -> Result<(), Failure> {
    validate_plain_view(view)?;
    let declaration = ViewDeclaration::new(view)?;
    if program
        .catalog
        .schemas
        .iter()
        .any(|schema| schema.name == declaration.name)
        || system_relation(&declaration.name.name)
    {
        return Err(unsupported("view name already exists"));
    }
    let query = sets::bind(program, crate::query_body(&view.query)?, tokens, 0)?;
    append_view(program, tokens.range()?, declaration, query)
}

fn bind_select(
    program: &mut Program,
    select: &crate::Select,
    tokens: &DeclarationTokens<'_>,
) -> Result<BoundQuery, Failure> {
    crate::check_select_clauses(select, true)?;
    let GroupByExpr::Expressions(groups, _) = &select.group_by else {
        return Err(unsupported("expected ordinary grouping expressions"));
    };
    let input =
        projection_input(select, false)?.ok_or_else(|| unsupported("expected a table in FROM"))?;
    let (schema, relation) = program
        .catalog
        .schemas
        .iter()
        .enumerate()
        .find(|(_, relation)| relation.name.name == input.name && !system_relation(&input.name))
        .ok_or_else(|| {
            unsupported(format!(
                "expected a declared relation in FROM, found {}",
                input.name
            ))
        })?;
    let schema = schema as u32;
    let columns: Vec<_> = relation
        .columns
        .iter()
        .map(|col| TableColumn {
            name: col.name.clone(),
            ty: col.ty,
        })
        .collect();
    let columns = columns.as_slice();
    let report_expression_error =
        |failure: Failure| failure.with_compiler_diagnostic(tokens.sql, None);
    let mut aggregate = aggregate::BoundAggregate::bind(
        program,
        &select.projection,
        groups,
        columns,
        &input.qualifier,
        tokens,
    )
    .map_err(report_expression_error)?;
    let mut projection = if let Some(aggregate) = &mut aggregate {
        std::mem::take(&mut aggregate.projection)
    } else if groups.is_empty() {
        bind_projection(
            program,
            &select.projection,
            columns,
            &input.qualifier,
            tokens,
        )
        .map_err(report_expression_error)?
    } else {
        return Err(unsupported("GROUP BY requires a supported aggregate"));
    };
    let predicate = select
        .selection
        .as_ref()
        .map(|expression| filter::bind(program, expression, columns, &input.qualifier, tokens))
        .transpose()
        .map_err(report_expression_error)?;
    if aggregate.is_none() {
        apply_filtered_projection(&mut projection, predicate.as_ref());
    }
    let scan_row = insert_row_fields(
        program,
        columns
            .iter()
            .map(|column| (column.name.name.as_str(), column.ty)),
    );
    let view_row = insert_row_fields(
        program,
        projection
            .iter()
            .map(|field| (field.name.name.as_str(), field.value.ty())),
    );
    let identity = projection.len() == columns.len()
        && projection.iter().enumerate().all(|(index, field)| {
            matches!(
                field.value, ProjectValue::Column(input) if input.field as usize == index
            )
        });
    let scan_hint = Hint {
        name: "alias".into(),
        position: tokens.coordinates(select.from[0].relation.span())?,
        inherit_path: Vec::new(),
        list: vec![input.qualifier.clone()],
        options: Vec::new(),
    };
    let root = if let Some(aggregate) = &aggregate {
        let (root, scan_required) =
            aggregate.append(program, scan_row, view_row, columns, predicate.as_ref())?;
        if scan_required {
            if let Some(filter::Predicate::Expression { value, .. }) = &predicate {
                append_table_filter(program, scan_row, value);
            }
            append_table_scan(program, schema, scan_row, input.name.clone(), scan_hint)?;
        }
        root
    } else if matches!(predicate, Some(filter::Predicate::Constant(false))) {
        append_values(program, view_row, 0, Vec::new())?
    } else {
        let project = (!identity).then(|| {
            append_table_project(
                program,
                view_row,
                scan_row,
                projection.iter().map(|field| &field.value),
            )
        });
        let filter = match &predicate {
            Some(filter::Predicate::Expression { value, .. }) => {
                Some(append_table_filter(program, scan_row, value))
            }
            _ => None,
        };
        let scan = append_table_scan(program, schema, scan_row, input.name.clone(), scan_hint)?;
        project.or(filter).unwrap_or(scan)
    };
    let mut view_source = render_select(&input.name, &input.qualifier, &projection);
    if let Some(predicate) = &predicate {
        let (pretty, diagnostic) = predicate.render();
        view_source.pretty.push_str(&format!("\nWHERE {pretty}"));
        view_source
            .diagnostic
            .push_str(&format!("\nWHERE {diagnostic}"));
    }
    if let Some(aggregate) = aggregate {
        aggregate.render_grouping(&mut view_source, columns, &input.qualifier);
    }
    let fields = projection
        .into_iter()
        .map(|field| (field.name, field.value.ty()))
        .collect();
    Ok(BoundQuery {
        kind: "SELECT",
        text: view_source,
        fields,
        row: view_row,
        relation: root,
    })
}

fn apply_filtered_projection(projection: &mut [Projection], predicate: Option<&filter::Predicate>) {
    let Some(filter::Predicate::Expression {
        value:
            ProjectValue::Call {
                key: "SqlStdOperatorTable.EQUALS",
                arguments,
                ..
            },
        ..
    }) = predicate
    else {
        return;
    };
    let [
        TableOperand::Column(column),
        TableOperand::Integer { ty, value },
    ] = arguments.as_slice()
    else {
        return;
    };
    for field in projection {
        let ProjectValue::Column(input) = field.value else {
            continue;
        };
        if input.field != column.field {
            continue;
        }
        field.value = if input.ty == *ty {
            ProjectValue::Integer {
                ty: *ty,
                value: *value,
            }
        } else {
            ProjectValue::Call {
                registry: crate::frel::CALCITE,
                key: "SqlStdOperatorTable.CAST",
                arguments: vec![TableOperand::Integer {
                    ty: *ty,
                    value: *value,
                }],
                ty: input.ty,
                position: None,
            }
        };
    }
}

fn bind_projection(
    program: &mut Program,
    items: &[SelectItem],
    columns: &[TableColumn],
    qualifier: &str,
    tokens: &DeclarationTokens<'_>,
) -> Result<Vec<Projection>, Failure> {
    if let [
        SelectItem::Wildcard(WildcardAdditionalOptions {
            wildcard_token: _,
            opt_ilike: None,
            opt_exclude: None,
            opt_except: None,
            opt_replace: None,
            opt_rename: None,
            opt_alias: None,
        }),
    ] = items
    {
        return Ok(columns
            .iter()
            .enumerate()
            .map(|(ordinal, column)| column_projection(ordinal, column, qualifier, None))
            .collect());
    }
    let mut output_names = HashSet::new();
    items
        .iter()
        .enumerate()
        .map(|(index, item)| {
            let (expression, alias) = projection_item(item)?;
            let field = match expression {
                Expr::BinaryOp { .. } | Expr::Function(_) => {
                    if let Some(failure) = unsupported_niladic(expression) {
                        return Err(failure);
                    }
                    compute::projection(
                        program, expression, alias, columns, qualifier, tokens, index,
                    )?
                }
                _ => {
                    let ordinal = column_ordinal(expression, columns, qualifier)?;
                    column_projection(ordinal, &columns[ordinal], qualifier, alias)
                }
            };
            if !output_names.insert(field.name.name.clone()) {
                return Err(unsupported(
                    "repeated output names are outside the projection slice",
                ));
            }
            Ok(field)
        })
        .collect()
}

fn projection_item(item: &SelectItem) -> Result<(&Expr, Option<&Ident>), Failure> {
    match item {
        SelectItem::UnnamedExpr(expression) => Ok((expression, None)),
        SelectItem::ExprWithAlias { expr, alias } if alias.quote_style.is_none() => {
            Ok((expr, Some(alias)))
        }
        _ => Err(unsupported("expected a plain output name")),
    }
}

fn column_ordinal(expr: &Expr, columns: &[TableColumn], qualifier: &str) -> Result<usize, Failure> {
    if let Some(failure) = unsupported_niladic(expr) {
        return Err(failure);
    }
    let ident = match expr {
        Expr::Identifier(ident) => ident,
        Expr::CompoundIdentifier(parts) => match parts.as_slice() {
            [prefix, ident]
                if prefix.quote_style.is_none() && prefix.value.to_lowercase() == qualifier =>
            {
                ident
            }
            _ => return Err(unsupported("expected a column of the scanned table")),
        },
        _ => return Err(unsupported("expected a direct column reference")),
    };
    let column = name(&ident.value);
    columns
        .iter()
        .position(|field| field.name == column)
        .filter(|_| ident.quote_style.is_none())
        .ok_or_else(|| unsupported("expected a column of the scanned table"))
}

fn column_projection(
    ordinal: usize,
    column: &TableColumn,
    qualifier: &str,
    alias: Option<&Ident>,
) -> Projection {
    let (pretty, diagnostic) = render_column(qualifier, &column.name.name);
    let value = ProjectValue::Column(ProjectInput {
        field: ordinal as u32,
        ty: column.ty,
    });
    let mut result = Projection {
        name: column.name.clone(),
        value,
        pretty,
        diagnostic,
    };
    result.apply_alias(alias);
    result
}

impl Projection {
    fn apply_alias(&mut self, alias: Option<&Ident>) {
        if let Some(alias) = alias {
            self.name = name(&alias.value);
            self.pretty.push_str(&format!(" AS `{}`", self.name.name));
            self.diagnostic.push_str(&format!(" AS {}", self.name.name));
        }
    }
}

fn render_column(qualifier: &str, column: &str) -> (String, String) {
    (
        format!("`{qualifier}`.`{column}`"),
        format!("{qualifier}.{column}"),
    )
}

fn name(ident: &str) -> Name {
    Name {
        name: ident.to_lowercase(),
        quoted: false,
    }
}

fn render_table(
    position: Coordinates,
    table: &Name,
    columns: &[DeclaredColumn],
    key_index: Option<usize>,
    key: &[String],
) -> StatementText {
    let mut pretty = columns
        .iter()
        .map(|column| format!("`{}` {}", column.name.name, column.declaration.quoted))
        .collect::<Vec<_>>();
    let mut diagnostic = columns
        .iter()
        .map(|column| format!("{} {}", column.name.name, column.declaration.diagnostic))
        .collect::<Vec<_>>();
    if let Some(index) = key_index {
        let quoted = key
            .iter()
            .map(|col| format!("`{col}`"))
            .collect::<Vec<_>>()
            .join(", ");
        pretty.insert(index, format!("PRIMARY KEY ({quoted})"));
        diagnostic.insert(index, format!("PRIMARY KEY ({})", key.join(", ")));
    }
    let pretty = pretty.join(", ");
    let diagnostic = diagnostic.join(", ");
    StatementText {
        position,
        pretty: format!("CREATE TABLE `{}` ({pretty})", table.name),
        diagnostic: format!("CREATE TABLE {} ({diagnostic})", table.name),
    }
}

fn render_select(table: &str, table_alias: &str, projection: &[Projection]) -> QueryText {
    let pretty = projection
        .iter()
        .map(|field| field.pretty.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let diagnostic = projection
        .iter()
        .map(|field| field.diagnostic.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    QueryText {
        pretty: format!("SELECT {pretty}\nFROM `schema`.`{table}` AS `{table_alias}`"),
        diagnostic: format!("SELECT {diagnostic}\nFROM schema.{table} AS {table_alias}"),
    }
}
