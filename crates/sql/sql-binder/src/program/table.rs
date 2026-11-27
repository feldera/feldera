//! Bind projections over one plain user table.

mod aggregate;
mod compute;
mod filter;

use super::{
    StatementText, ViewDeclaration, append_boundary, append_schema, append_view, check_prelude,
    column, diagnostic_source, finish,
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
    TypeId, TypeTable, ValidationLimits,
    catalog::Name,
    program::Program,
    relation_metadata::{Coordinates, Hint},
    source::Origin,
    statements::Statement,
    types::{
        Cardinality, CharacterMetadata, FloatFlavor, IntegerWidth, Precision, SqlType, TimeZone,
        TypeKind,
    },
};
use sqlparser_064::ast::{
    ArrayElemTypeDef, ColumnOption, GroupByExpr, Ident, MapBracketKind, TableConstraint,
    WildcardAdditionalOptions,
};
use std::collections::HashSet;

struct DeclaredColumn {
    name: Name,
    ty: TypeId,
    declaration: String,
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
    if !sql.is_ascii() {
        return Err(unsupported("table statement source must be ASCII"));
    }

    let declarations = DeclarationTokens::split(sql, starts)?;
    if declarations.len() != parsed.len() {
        return Err(unsupported(
            "source declaration count differs from parsed statements",
        ));
    }
    set_source(&mut program, id, sql, many);
    for (parsed_statement, tokens) in parsed.iter().zip(&declarations) {
        let ParsedStatement::Upstream(statement) = parsed_statement else {
            return Err(unsupported("expected CREATE TABLE or CREATE VIEW"));
        };
        match statement.as_ref() {
            Upstream::CreateTable(table) => {
                declare_table(&mut program, table, tokens)?;
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
            Upstream::CreateView(view) => bind_view(&mut program, view, tokens.range()?, sql)?,
            Upstream::Insert(insert) => super::insert::bind(&mut program, insert, tokens.range()?)?,
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
            &column.data_type,
            nullable,
            ValidationLimits::default().depth,
        )?;
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
            declaration: format!("{spelling}{nullability}{constraint}"),
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
            let text = format!("{} {}", field.name.name, field.declaration);
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
        name_diagnostic: diagnostic_source(&coordinates(table_ident.span), &table_name.name),
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
                let name = ident.value.to_ascii_lowercase();
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
    position: Coordinates,
    sql: &str,
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
    let select = select_without_modifiers(&view.query, true)?;
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
    let mut view_source = render_view(
        position,
        &declaration,
        &input.name,
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
    append_view(program, view_source, declaration, fields, view_row, root)
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

fn declare_type(
    types: &mut TypeTable,
    data_type: &DataType,
    nullable: bool,
    depth: usize,
) -> Result<(TypeId, String), Failure> {
    let child_depth = depth
        .checked_sub(1)
        .ok_or_else(|| unsupported("table type nesting exceeds the validation limit"))?;
    let (kind, spelling) = match data_type {
        DataType::Array(ArrayElemTypeDef::Qualified(element, None)) => {
            let (element, spelling) = declare_type(types, element, true, child_depth)?;
            (
                TypeKind::Array {
                    element,
                    cardinality: Cardinality::Unbounded,
                },
                format!("{spelling} ARRAY"),
            )
        }
        DataType::Map(key, value, MapBracketKind::AngleBrackets) => {
            let (key, key_spelling) = declare_type(types, key, false, child_depth)?;
            if !types.types[key.0 as usize].kind.can_be_map_key() {
                return Err(unsupported("MAP keys cannot have type MAP or NULL"));
            }
            let (value, value_spelling) = declare_type(types, value, true, child_depth)?;
            (
                TypeKind::Map { key, value },
                format!("MAP< {key_spelling}, {value_spelling} >"),
            )
        }
        _ => scalar_type(data_type, nullable)?,
    };
    let ty = insert_type(
        types,
        SqlType {
            kind,
            nullable,
            origin: Origin::Absent {},
        },
    );
    Ok((ty, spelling))
}

fn scalar_type(data_type: &DataType, nullable: bool) -> Result<(TypeKind, String), Failure> {
    use sqlparser_064::ast::{BinaryLength, TimezoneInfo};

    let integer =
        |width, signed, spelling: &str| (TypeKind::Integer { width, signed }, spelling.into());
    let kind = match data_type {
        DataType::TinyIntUnsigned(None) => integer(IntegerWidth::I8, false, "TINYINT UNSIGNED"),
        DataType::SmallIntUnsigned(None) => integer(IntegerWidth::I16, false, "SMALLINT UNSIGNED"),
        DataType::BigIntUnsigned(None) => integer(IntegerWidth::I64, false, "BIGINT UNSIGNED"),
        DataType::Unsigned
        | DataType::UnsignedInteger
        | DataType::IntUnsigned(None)
        | DataType::IntegerUnsigned(None) => integer(IntegerWidth::I32, false, "INTEGER UNSIGNED"),
        DataType::Date => (TypeKind::Date {}, "DATE".into()),
        DataType::Real => (
            TypeKind::Float {
                flavor: FloatFlavor::Real,
            },
            "REAL".into(),
        ),
        DataType::Binary(_) | DataType::Varbinary(_) => {
            let (fixed, width) = match data_type {
                DataType::Binary(width) => (true, *width),
                DataType::Varbinary(None) => (false, None),
                DataType::Varbinary(Some(BinaryLength::IntegerLength { length })) => {
                    (false, Some(*length))
                }
                _ => return Err(unsupported("unsupported binary length")),
            };
            let name = if fixed { "BINARY" } else { "VARBINARY" };
            let length = match width {
                Some(n) => Precision::Specified(
                    u32::try_from(n).map_err(|_| unsupported("binary length exceeds its range"))?,
                ),
                None if fixed => Precision::Specified(1),
                None => Precision::Unspecified,
            };
            (
                TypeKind::Binary { fixed, length },
                width.map_or_else(|| name.into(), |n| format!("{name}({n})")),
            )
        }
        DataType::Time(width, TimezoneInfo::None)
        | DataType::Timestamp(width, TimezoneInfo::None) => {
            let is_time = matches!(data_type, DataType::Time(..));
            let (name, maximum) = if is_time {
                ("TIME", 9)
            } else {
                ("TIMESTAMP", 6)
            };
            let precision = Precision::Specified(width.unwrap_or(maximum).min(maximum) as u32);
            let zone = TimeZone::None;
            let kind = if is_time {
                TypeKind::Time { precision, zone }
            } else {
                TypeKind::Timestamp { precision, zone }
            };
            (
                kind,
                width.map_or_else(|| name.into(), |n| format!("{name}({n})")),
            )
        }
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
                    metadata: CharacterMetadata::implicit(),
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

fn render_table(
    position: Coordinates,
    table: &Name,
    columns: &[DeclaredColumn],
    key_index: Option<usize>,
    key: &[String],
) -> StatementText {
    let mut pretty = columns
        .iter()
        .map(|column| format!("`{}` {}", column.name.name, column.declaration))
        .collect::<Vec<_>>();
    let mut diagnostic = columns
        .iter()
        .map(|column| format!("{} {}", column.name.name, column.declaration))
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

fn render_view(
    position: Coordinates,
    declaration: &ViewDeclaration,
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
            "{} AS\nSELECT {pretty}\nFROM `schema`.`{table}` AS `{table_alias}`",
            declaration.sql(true)
        ),
        diagnostic: format!(
            "{} AS\nSELECT {diagnostic}\nFROM schema.{table} AS {table_alias}",
            declaration.sql(false)
        ),
    }
}
