//! Bind projections over one plain user table.

mod aggregate;
mod compute;
mod filter;

use super::{
    StatementText, append_boundary, append_schema, append_view, check_prelude, column,
    diagnostic_source, finish,
    graph::{
        ProjectInput, ProjectValue, TableOperand, append_table_filter, append_table_project,
        append_table_scan, append_values, insert_row_fields, insert_type,
    },
    set_source,
    source::{DeclarationTokens, coordinates},
};
use crate::{
    DataType, Expr, Failure, Kind, SelectItem, Spanned, Statement as ParsedStatement, Submission,
    Upstream, checked_type, local_name, map_table_columns, parse_submissions, projection_input,
    select_without_modifiers, system_relation, unsupported, unsupported_niladic,
    validate_plain_view,
};
use feldera_sql_rel_model::{
    TypeId,
    catalog::Name,
    program::Program,
    relation_metadata::{Coordinates, Hint},
    source::Origin,
    statements::Statement,
    types::{
        CharacterMetadata, Coercibility, CollationIdentity, FloatFlavor, IntegerWidth, Precision,
        SqlType, TimeZone, TypeKind,
    },
};
use sqlparser_064::ast::{GroupByExpr, Ident, WildcardAdditionalOptions};
use std::collections::{HashMap, HashSet};

struct TableColumn {
    name: Name,
    ty: TypeId,
    declaration: String,
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
    let (parsed, sql) = parse_submissions(submissions)?;
    bind_parsed_table(id, program, &parsed, &sql, many)
}

pub(super) fn bind_parsed_table(
    id: String,
    mut program: Program,
    parsed: &[ParsedStatement],
    sql: &str,
    many: bool,
) -> Result<Program, Failure> {
    check_prelude(&program)?;
    if !sql.is_ascii() {
        return Err(unsupported("table statement source must be ASCII"));
    }

    let declarations = DeclarationTokens::split(sql)?;
    if declarations.len() != parsed.len() {
        return Err(unsupported(
            "source declaration count differs from parsed statements",
        ));
    }
    set_source(&mut program, id, sql, many);
    let mut tables = HashMap::new();
    for (parsed_statement, tokens) in parsed.iter().zip(&declarations) {
        let ParsedStatement::Upstream(statement) = parsed_statement else {
            return Err(unsupported("expected CREATE TABLE or CREATE VIEW"));
        };
        match statement.as_ref() {
            Upstream::CreateTable(table) => {
                let bound = declare_table(&mut program, table, tokens)?;
                tables.insert(bound.name.name.clone(), bound);
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
                    sql,
                    tokens.range()?,
                )?;
            }
            Upstream::CreateView(view) => {
                bind_view(&mut program, &tables, view, tokens.range()?, sql)?
            }
            _ => return Err(unsupported("expected CREATE TABLE or CREATE VIEW")),
        }
    }
    finish(program)
}

struct BoundTable {
    name: Name,
    columns: Vec<TableColumn>,
    schema: u32,
}

fn declare_table(
    program: &mut Program,
    table: &sqlparser_064::ast::CreateTable,
    tokens: &DeclarationTokens,
) -> Result<BoundTable, Failure> {
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
    let columns = map_table_columns(table, false, |column, nullable| {
        if nullable && !column.options.is_empty() {
            return Err(unsupported("explicit NULL is outside the table slice"));
        }
        if column.options.iter().any(|option| option.name.is_some()) {
            return Err(unsupported(
                "named column constraints are outside the table slice",
            ));
        }
        let (kind, spelling) = table_type(&column.data_type, nullable)?;
        let ty = insert_type(
            &mut program.table.type_table,
            SqlType {
                nullable,
                origin: Origin::Absent {},
                kind,
            },
        );
        let nullability = if nullable { "" } else { " NOT NULL" };
        Ok(TableColumn {
            name: name(&column.name.value),
            ty,
            declaration: format!("{spelling}{nullability}"),
        })
    })?;
    let positions = tokens.column_ranges(table.columns.iter().map(|col| col.name.span))?;
    let table_source = render_table(tokens.range()?, &table_name, &columns);
    let table_columns = columns
        .iter()
        .zip(&positions)
        .enumerate()
        .map(|(index, (field, position))| {
            let text = format!("{} {}", field.name.name, field.declaration);
            column(
                field.name.clone(),
                index,
                field.ty,
                diagnostic_source(position, &text),
            )
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
        name_diagnostic: diagnostic_source(&coordinates(table_ident.span), &table_name.name),
        name: table_name.clone(),
        foreign_keys: Vec::new(),
    });

    Ok(BoundTable {
        name: table_name,
        columns,
        schema,
    })
}

fn bind_view(
    program: &mut Program,
    tables: &HashMap<String, BoundTable>,
    view: &sqlparser_064::ast::CreateView,
    position: Coordinates,
    sql: &str,
) -> Result<(), Failure> {
    validate_plain_view(view)?;
    let view_name = name(&local_name(&view.name)?.value);
    if view.materialized
        || view.local.is_some()
        || program
            .catalog
            .schemas
            .iter()
            .any(|schema| schema.name == view_name)
        || system_relation(&view_name.name)
    {
        return Err(unsupported("expected a standard view over a user table"));
    }
    let select = select_without_modifiers(&view.query, true)?;
    let GroupByExpr::Expressions(groups, _) = &select.group_by else {
        return Err(unsupported("expected ordinary grouping expressions"));
    };
    let input =
        projection_input(select, false)?.ok_or_else(|| unsupported("expected a table in FROM"))?;
    let BoundTable {
        name: table_name,
        columns,
        schema,
    } = tables.get(&input.name).ok_or_else(|| {
        unsupported(format!(
            "expected a declared base table in FROM, found {}",
            input.name
        ))
    })?;
    let columns = columns.as_slice();
    let schema = *schema;
    let report_expression_error = |failure: Failure| failure.with_compiler_diagnostic(sql, None);
    let mut aggregate = aggregate::BoundAggregate::bind(
        program,
        &select.projection,
        groups,
        columns,
        &input.qualifier,
        sql,
    )
    .map_err(report_expression_error)?;
    let mut projection = if let Some(aggregate) = &mut aggregate {
        std::mem::take(&mut aggregate.projection)
    } else if groups.is_empty() {
        bind_projection(program, &select.projection, columns, &input.qualifier, sql)
            .map_err(report_expression_error)?
    } else {
        return Err(unsupported("GROUP BY requires a supported aggregate"));
    };
    let predicate = select
        .selection
        .as_ref()
        .map(|expression| filter::bind(program, expression, columns, &input.qualifier, sql))
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
        position: coordinates(select.from[0].relation.span()),
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
            append_table_scan(
                program,
                schema,
                scan_row,
                table_name.name.clone(),
                scan_hint,
            )?;
        }
        root
    } else if matches!(predicate, Some(filter::Predicate::Constant(false))) {
        append_values(program, view_row, None)?
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
        let scan = append_table_scan(
            program,
            schema,
            scan_row,
            table_name.name.clone(),
            scan_hint,
        )?;
        project.or(filter).unwrap_or(scan)
    };
    let mut view_source = render_view(
        position,
        &view_name.name,
        &table_name.name,
        &input.qualifier,
        &projection,
    );
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
    append_view(program, view_source, view_name, fields, view_row, root)
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
    sql: &str,
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
                    compute::projection(program, expression, alias, columns, qualifier, sql, index)?
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
                if prefix.quote_style.is_none() && prefix.value.eq_ignore_ascii_case(qualifier) =>
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

fn table_type(data_type: &DataType, nullable: bool) -> Result<(TypeKind, String), Failure> {
    let integer =
        |width, signed, spelling: &str| (TypeKind::Integer { width, signed }, spelling.into());
    let kind = match data_type {
        DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::IntUnsigned(None)
        | DataType::IntegerUnsigned(None) => integer(IntegerWidth::I32, false, "INTEGER UNSIGNED"),
        DataType::Date => (TypeKind::Date {}, "DATE".into()),
        DataType::Varchar(size)
        | DataType::CharVarying(size)
        | DataType::CharacterVarying(size)
        | DataType::Char(size)
        | DataType::Character(size) => {
            use sqlparser_064::ast::CharacterLength;
            let fixed = matches!(data_type, DataType::Char(_) | DataType::Character(_));
            let name = if fixed { "CHAR" } else { "VARCHAR" };
            let (length, spelling) = match size {
                None => (
                    if fixed {
                        Precision::Specified(1)
                    } else {
                        Precision::Unspecified
                    },
                    name.to_string(),
                ),
                Some(CharacterLength::IntegerLength { length, unit: None })
                    if *length <= i32::MAX as u64 =>
                {
                    (
                        Precision::Specified(*length as u32),
                        format!("{name}({length})"),
                    )
                }
                _ => return Err(unsupported("unsupported character length")),
            };
            (
                TypeKind::Character {
                    fixed,
                    length,
                    metadata: CharacterMetadata::Present {
                        charset: "UTF-8".into(),
                        collation: "ISO-8859-1$en_US$primary".into(),
                        coercibility: Coercibility::Implicit,
                        identity: CollationIdentity::BuiltinImplicit,
                    },
                },
                spelling,
            )
        }
        _ => match checked_type(data_type, nullable)?.kind {
            Kind::Boolean => (TypeKind::Boolean {}, "BOOLEAN".into()),
            Kind::Decimal { precision, scale } => (
                TypeKind::Decimal {
                    precision: precision.into(),
                    scale: scale.into(),
                    explicit_scale: true,
                },
                format!("DECIMAL({precision}, {scale})"),
            ),
            Kind::Integer { bits: 8 } => integer(IntegerWidth::I8, true, "TINYINT"),
            Kind::Integer { bits: 16 } => integer(IntegerWidth::I16, true, "SMALLINT"),
            Kind::Integer { bits: 32 } => integer(IntegerWidth::I32, true, "INTEGER"),
            Kind::Integer { bits: 64 } => integer(IntegerWidth::I64, true, "BIGINT"),
            Kind::Double => (
                TypeKind::Float {
                    flavor: FloatFlavor::Double,
                },
                "DOUBLE".into(),
            ),
            Kind::Timestamp { precision: 6 }
                if matches!(data_type, DataType::Timestamp(None, _)) =>
            {
                (
                    TypeKind::Timestamp {
                        precision: Precision::Specified(6),
                        zone: TimeZone::None,
                    },
                    "TIMESTAMP".into(),
                )
            }
            _ => {
                return Err(unsupported(
                    "table type outside the direct projection slice",
                ));
            }
        },
    };
    Ok(kind)
}

fn name(ident: &str) -> Name {
    Name {
        name: ident.to_ascii_lowercase(),
        quoted: false,
    }
}

fn render_table(position: Coordinates, table: &Name, columns: &[TableColumn]) -> StatementText {
    let pretty = columns
        .iter()
        .map(|column| format!("`{}` {}", column.name.name, column.declaration))
        .collect::<Vec<_>>()
        .join(", ");
    let diagnostic = columns
        .iter()
        .map(|column| format!("{} {}", column.name.name, column.declaration))
        .collect::<Vec<_>>()
        .join(", ");
    StatementText {
        position,
        pretty: format!("CREATE TABLE `{}` ({pretty})", table.name),
        diagnostic: format!("CREATE TABLE {} ({diagnostic})", table.name),
    }
}

fn render_view(
    position: Coordinates,
    view: &str,
    table: &str,
    table_alias: &str,
    projection: &[Projection],
) -> StatementText {
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
    StatementText {
        position,
        pretty: format!(
            "CREATE VIEW `{view}` AS\nSELECT {pretty}\nFROM `schema`.`{table}` AS `{table_alias}`"
        ),
        diagnostic: format!(
            "CREATE VIEW {view} AS\nSELECT {diagnostic}\nFROM schema.{table} AS {table_alias}"
        ),
    }
}
