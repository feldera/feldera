//! Bind SQL syntax into a validated relational program.
//! The supported statement shapes are constant views and a table with a view.

mod constant;
mod frel;
mod program;
mod schema;

pub use schema::preview_schema;

pub use program::{bind_constant_program, bind_sql, bind_submissions, bind_table_program};

use feldera_sql_parser::{Statement, parse as parse_feldera};
use feldera_type_derivation_probe::{
    extras,
    model::{DerivedType, Kind, Outcome, SqlType},
    rewritten, sampled_arity,
};
use serde::{Deserialize, Serialize};
use sqlparser_064::{
    ast::{
        BinaryOperator, CastKind, CeilFloorKind, ColumnOption, CreateTableOptions, DataType,
        DateTimeField, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
        FunctionArguments, GroupByExpr, Ident, ObjectName, ObjectNamePart, Query, Select,
        SelectFlavor, SelectItem, SetExpr, Spanned, Statement as Upstream, TableFactor, Value,
        helpers::stmt_create_table::CreateTableBuilder,
    },
    keywords::Keyword,
    tokenizer::{Location, Span, Token, TokenWithSpan},
};
use std::{cell::OnceCell, collections::HashSet, fmt::Write};

#[derive(Debug, Serialize)]
pub struct Failure {
    category: &'static str,
    message: String,
    span: Option<[u64; 4]>,
    #[serde(skip)]
    diagnostic_span: Option<DiagnosticSpan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compiler_diagnostic: Option<Box<CompilerDiagnostic>>,
}

#[derive(Debug)]
enum DiagnosticSpan {
    Projection,
    Expression,
}

#[derive(Debug, Serialize)]
pub struct CompilerDiagnostic {
    warning: bool,
    error_type: &'static str,
    start_line_number: usize,
    start_column: usize,
    end_line_number: usize,
    end_column: usize,
    message: String,
    snippet: String,
}

impl std::fmt::Display for Failure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.category, self.message)?;
        if let Some([line, column, _, _]) = self.span {
            write!(f, " at line {line}, column {column}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Failure {}

impl Failure {
    pub fn is_parse_error(&self) -> bool {
        self.category == "parse_error"
    }

    /// A located SQL error, when the binder can diagnose this input.
    pub fn compiler_diagnostic(&self) -> Option<&CompilerDiagnostic> {
        self.compiler_diagnostic.as_deref()
    }

    fn with_compiler_diagnostic(mut self, sql: &str, projection: Option<Span>) -> Self {
        let range = match self.diagnostic_span {
            Some(DiagnosticSpan::Projection) => projection,
            Some(DiagnosticSpan::Expression) => self.span.map(from_span_array),
            None => None,
        };
        if let Some(range) = range {
            self.compiler_diagnostic =
                projection_diagnostic(sql, range, DiagnosticKind::CompilationError, &self.message)
                    .map(Box::new);
        }
        self
    }
}

#[derive(PartialEq, Eq)]
enum DiagnosticKind {
    CompilationError,
    ReservedName,
    Noop,
    UselessOperation,
    FloatingPointEquality,
}

impl DiagnosticKind {
    fn error_type(&self) -> &'static str {
        match self {
            Self::CompilationError => "Compilation error",
            Self::ReservedName => "Reserved name",
            Self::Noop => "Noop",
            Self::UselessOperation => "Useless operation",
            Self::FloatingPointEquality => "Floating point equality",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct BoundProjection {
    pub name: String,
    pub result: SqlType,
    #[serde(skip_serializing_if = "Option::is_none")]
    table: Option<String>,
    column_types: Vec<Column>,
    expression: BoundExpression,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    compiler_diagnostics: Vec<CompilerDiagnostic>,
    #[serde(skip)]
    output_quoted: bool,
    #[serde(skip)]
    materialized_view: bool,
    #[serde(skip)]
    view_name: String,
    #[serde(skip)]
    local_view: bool,
    #[serde(skip)]
    projection_span: Span,
}

#[derive(Debug, Serialize)]
struct Column {
    name: String,
    ty: SqlType,
    #[serde(skip)]
    quoted: bool,
}

struct TableInput {
    name: String,
    qualifier: String,
}

struct ColumnScope<'a> {
    columns: &'a [Column],
    qualifier: Option<&'a str>,
    sql: &'a str,
    preserve_parentheses: bool,
    tokens: OnceCell<Vec<TokenWithSpan>>,
}

impl ColumnScope<'_> {
    fn resolve(
        &self,
        column: &Ident,
        range: [u64; 4],
    ) -> Result<(SqlType, ExpressionKind), Failure> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, candidate)| candidate.name.eq_ignore_ascii_case(&column.value))
            .map(|(ordinal, candidate)| (candidate.ty, ExpressionKind::Column { ordinal }))
            .ok_or_else(|| Failure {
                category: "unresolved_column",
                message: format!("unknown column {column}"),
                span: Some(range),
                diagnostic_span: None,
                compiler_diagnostic: None,
            })
    }

    fn enclosing_parentheses(
        &self,
        [line, column, end_line, end_column]: [u64; 4],
    ) -> Option<Span> {
        if !self.preserve_parentheses {
            return None;
        }
        let tokens = self
            .tokens
            .get_or_init(|| feldera_sql_parser::source_tokens(self.sql).unwrap_or_default());
        let nonspace = || {
            tokens
                .iter()
                .filter(|item| !matches!(item.token, Token::Whitespace(_)))
        };
        let left = nonspace()
            .rev()
            .find(|item| (item.span.end.line, item.span.end.column) <= (line, column))?;
        let right = nonspace().find(|item| {
            (item.span.start.line, item.span.start.column) >= (end_line, end_column)
        })?;
        if left.token == Token::LParen && right.token == Token::RParen {
            Some(Span {
                start: left.span.start,
                end: right.span.end,
            })
        } else {
            None
        }
    }
}

impl BoundProjection {
    /// Encode the independently bound scalar root in the existing FRel
    /// expression contract. Unsupported literal forms remain explicit refusals.
    pub fn frel_expression(
        &self,
        sql: &str,
    ) -> Result<feldera_sql_rel_model::expressions::ExpressionTable, Failure> {
        frel::emit(
            sql,
            &self.expression,
            span(self.projection_span),
            self.table.as_ref().map(|_| self.column_types.as_slice()),
        )
    }

    /// Schema of the fixed table/view experiment under the S2 probe options.
    /// Usage flags are computed by later optimizer passes and excluded by C1a.
    pub fn generated_schema(&self) -> serde_json::Value {
        self.schema(true)
    }

    /// Observable schema of the scalar slice after the default optimizer removes
    /// unused inputs and the internal clock and error sources.
    pub fn scoped_schema(&self) -> serde_json::Value {
        self.schema(false)
    }

    fn schema(&self, include_internal_inputs: bool) -> serde_json::Value {
        use serde_json::json;

        let error_type = SqlType {
            kind: Kind::Character {
                fixed: false,
                length: None,
            },
            nullable: false,
        };
        let error_columns = ["table_or_view_name", "message", "metadata"]
            .map(|name| schema_column(name, false, error_type));
        let clock_type = SqlType {
            kind: Kind::Timestamp { precision: 6 },
            nullable: false,
        };
        let mut inputs = Vec::new();
        if include_internal_inputs {
            inputs.extend([
                json!({"name": "now", "case_sensitive": false, "materialized": false,
                    "foreign_keys": [], "fields": [schema_column("now", false, clock_type)]}),
                json!({"name": "feldera_error_table", "case_sensitive": false, "materialized": false,
                    "foreign_keys": [], "fields": error_columns}),
            ]);
        }
        if let Some(table) = self
            .table
            .as_ref()
            .filter(|_| include_internal_inputs || !self.local_view)
        {
            let fields: Vec<_> = self
                .column_types
                .iter()
                .map(|column| schema_column(&column.name, column.quoted, column.ty))
                .collect();
            inputs.push(
                json!({"name": table, "case_sensitive": false, "materialized": false,
                 "foreign_keys": [], "fields": fields}),
            );
        }
        let mut outputs = vec![json!({"name": "error_view", "case_sensitive": false,
                                      "materialized": false, "fields": error_columns})];
        if include_internal_inputs || !self.local_view {
            outputs.push(json!({"name": self.view_name, "case_sensitive": false,
                "materialized": self.materialized_view,
                "fields": [schema_column(&self.name, self.output_quoted, self.result)]}));
        }
        json!({"inputs": inputs, "outputs": outputs})
    }
}

fn schema_column(name: &str, quoted: bool, ty: SqlType) -> serde_json::Value {
    use serde_json::json;
    let name = if quoted {
        name.to_owned()
    } else {
        name.to_ascii_lowercase()
    };
    let mut descriptor = match ty.kind {
        Kind::Boolean => json!({"type": "BOOLEAN"}),
        Kind::Integer { bits: 8 } => json!({"type": "TINYINT"}),
        Kind::Integer { bits: 16 } => json!({"type": "SMALLINT"}),
        Kind::Integer { bits: 32 } => json!({"type": "INTEGER"}),
        Kind::Integer { bits: 64 } => json!({"type": "BIGINT"}),
        Kind::Integer { .. } => unreachable!("binder emits only supported integer widths"),
        Kind::Real => json!({"type": "REAL"}),
        Kind::Double => json!({"type": "DOUBLE"}),
        Kind::Decimal { precision, scale } => {
            json!({"type": "DECIMAL", "precision": precision, "scale": scale})
        }
        Kind::Character { fixed, length } => json!({
            "type": if fixed { "CHAR" } else { "VARCHAR" },
            "precision": length.map_or(-1, i64::from)
        }),
        Kind::Binary { fixed, length } => json!({
            "type": if fixed { "BINARY" } else { "VARBINARY" },
            "precision": length.map_or(-1, i64::from)
        }),
        Kind::Date => json!({"type": "DATE"}),
        Kind::Timestamp { precision } => json!({"type": "TIMESTAMP", "precision": precision}),
    };
    descriptor["nullable"] = ty.nullable.into();
    json!({"name": name, "case_sensitive": quoted, "columntype": descriptor})
}

#[derive(Debug, Serialize)]
struct BoundExpression {
    ty: SqlType,
    span: [u64; 4],
    #[serde(flatten)]
    kind: ExpressionKind,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ExpressionKind {
    Column {
        ordinal: usize,
    },
    TypedLiteral {
        literal: String,
        literal_span: [u64; 4],
    },
    FoldedLiteral {
        value: feldera_sql_rel_model::literals::LiteralValue,
    },
    Function {
        name: String,
        operands: Vec<BoundExpression>,
    },
    Binary {
        operator: &'static str,
        operands: Vec<BoundExpression>,
    },
    Coerce {
        inner: Box<BoundExpression>,
    },
}

fn span(span: Span) -> [u64; 4] {
    [
        span.start.line,
        span.start.column,
        span.end.line,
        span.end.column,
    ]
}

fn cover(left: [u64; 4], right: [u64; 4]) -> [u64; 4] {
    let start = [left[0], left[1]].min([right[0], right[1]]);
    let end = [left[2], left[3]].max([right[2], right[3]]);
    [start[0], start[1], end[0], end[1]]
}

fn from_span_array([line, column, end_line, end_column]: [u64; 4]) -> Span {
    Span {
        start: Location { line, column },
        end: Location {
            line: end_line,
            column: end_column,
        },
    }
}

fn unsupported(message: impl Into<String>) -> Failure {
    Failure {
        category: "outside_scope",
        message: message.into(),
        span: None,
        diagnostic_span: None,
        compiler_diagnostic: None,
    }
}

fn checked_type(ty: &DataType, nullable: bool) -> Result<SqlType, Failure> {
    let spelling = ty.to_string().replace(", ", ",").to_ascii_uppercase();
    let canonical = match spelling.as_str() {
        "INT" | "INT4" | "SIGNED" => "INTEGER",
        "INT2" => "SMALLINT",
        "INT8" | "INT64" => "BIGINT",
        "FLOAT4" | "FLOAT32" => "REAL",
        "FLOAT8" | "FLOAT64" | "DOUBLE PRECISION" => "DOUBLE",
        "BOOL" => "BOOLEAN",
        "STRING" | "TEXT" => "VARCHAR",
        "DATETIME" => "TIMESTAMP",
        _ => &spelling,
    };
    SqlType::parse(canonical, nullable).map_err(unsupported)
}

fn scalar_args(function: &Function) -> Result<(&str, Vec<&Expr>), Failure> {
    let Function {
        name,
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args,
        within_group,
        filter: None,
        null_treatment: None,
        over: None,
    } = function
    else {
        return Err(unsupported("expected an ordinary scalar function"));
    };
    let [ObjectNamePart::Identifier(name)] = name.0.as_slice() else {
        return Err(unsupported("expected an unqualified scalar function"));
    };
    let FunctionArguments::List(FunctionArgumentList {
        duplicate_treatment: None,
        args,
        clauses,
    }) = args
    else {
        return Err(unsupported("expected positional call arguments"));
    };
    if name.quote_style.is_some() || !within_group.is_empty() || !clauses.is_empty() {
        return Err(unsupported(
            "function clauses or quoted names are outside the probe",
        ));
    }
    let args = args
        .iter()
        .map(|arg| match arg {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(expression)) => Ok(expression),
            _ => Err(unsupported("expected a scalar argument")),
        })
        .collect::<Result<_, _>>()?;
    Ok((&name.value, args))
}

fn cast_literal(arg: &Expr, data_type: &DataType) -> Result<(SqlType, ExpressionKind), Failure> {
    let is_null = match arg {
        Expr::Value(value) => match value.value {
            Value::Null => true,
            Value::Boolean(_) | Value::Number(..) | Value::SingleQuotedString(_) => false,
            _ => return Err(unsupported("literal outside the generated scalar grammar")),
        },
        Expr::TypedString(_) => false,
        _ => {
            return Err(unsupported(
                "only explicit casts of probe literals are handled",
            ));
        }
    };
    let target = checked_type(data_type, is_null)?;
    Ok((
        target,
        ExpressionKind::TypedLiteral {
            literal: arg.to_string(),
            literal_span: span(arg.span()),
        },
    ))
}

fn plain_literal(node: &Expr, value: &Value) -> Result<(SqlType, ExpressionKind), Failure> {
    let (kind, literal) = match value {
        Value::Boolean(value) => (Kind::Boolean, value.to_string()),
        Value::Number(number, _) if number.parse::<i32>().is_ok() => {
            (Kind::Integer { bits: 32 }, number.clone())
        }
        Value::Number(number, _) if number.parse::<i64>().is_ok() => {
            (Kind::Integer { bits: 64 }, number.clone())
        }
        _ => return Err(unsupported("literal outside the scalar FRel scope")),
    };
    Ok((
        SqlType {
            kind,
            nullable: false,
        },
        ExpressionKind::TypedLiteral {
            literal,
            literal_span: span(node.span()),
        },
    ))
}

fn infer_call(
    name: &str,
    binary: bool,
    args: Vec<&Expr>,
    scope: &ColumnScope<'_>,
    range: [u64; 4],
    depth: usize,
) -> Result<(SqlType, Vec<BoundExpression>), Failure> {
    if !binary && sampled_arity(name) != Some(args.len()) {
        return Err(unsupported("scalar name or arity is outside the probe"));
    }
    let operands = args
        .into_iter()
        .map(|arg| bind(arg, scope, depth + 1))
        .collect::<Result<Vec<_>, _>>()?;
    let types = operands.iter().map(|arg| arg.ty).collect::<Vec<_>>();
    let outcome = if binary {
        extras::infer(name, &types)
    } else {
        rewritten::infer(name, &types)
    };
    match outcome {
        Outcome::Bound {
            result: DerivedType::Scalar(result),
            operands: targets,
        } => {
            let operands = operands
                .into_iter()
                .zip(targets)
                .map(|arg| {
                    let (inner, ty) = arg;
                    if inner.ty == ty {
                        inner
                    } else {
                        BoundExpression {
                            ty,
                            span: inner.span,
                            kind: ExpressionKind::Coerce {
                                inner: Box::new(inner),
                            },
                        }
                    }
                })
                .collect();
            Ok((result, operands))
        }
        Outcome::Rejected { .. } => Err(Failure {
            category: "type_rejection",
            message: format!("cannot apply {name} to {types:?}"),
            span: Some(range),
            diagnostic_span: (depth == 0).then_some(DiagnosticSpan::Projection),
            compiler_diagnostic: None,
        }),
        _ => Err(unsupported("outside the common type inference slice")),
    }
}

fn unsupported_niladic(node: &Expr) -> Option<Failure> {
    let name = match node {
        Expr::Identifier(name) => name,
        Expr::Function(function) if matches!(function.args, FunctionArguments::None) => {
            let [ObjectNamePart::Identifier(name)] = function.name.0.as_slice() else {
                return None;
            };
            name
        }
        _ => return None,
    };
    if name.quote_style.is_some() {
        return None;
    }
    let uppercase = name.value.to_ascii_uppercase();
    let replacement = match uppercase.as_str() {
        "USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER" => None,
        "CURRENT_TIMESTAMP" | "LOCALTIMESTAMP" => Some("NOW()"),
        "CURRENT_DATE" => Some("CAST(NOW() AS DATE)"),
        "CURRENT_TIME" | "LOCALTIME" => Some("CAST(NOW() AS TIME)"),
        _ => return None,
    };
    let mut message = format!("Function '{uppercase}' is not supported");
    if let Some(replacement) = replacement {
        let _ = write!(message, ". Use {replacement} instead");
    }
    Some(Failure {
        category: "unsupported_niladic_function",
        message,
        span: Some(span(name.span)),
        diagnostic_span: Some(DiagnosticSpan::Expression),
        compiler_diagnostic: None,
    })
}

fn bind(node: &Expr, scope: &ColumnScope<'_>, depth: usize) -> Result<BoundExpression, Failure> {
    if depth >= 32 {
        return Err(unsupported("expression exceeds 32 layers"));
    }
    if let Some(failure) = unsupported_niladic(node) {
        return Err(failure);
    }
    let mut range = span(node.span());
    let (ty, kind) = match node {
        Expr::Identifier(ident) if ident.quote_style.is_none() => scope.resolve(ident, range)?,
        Expr::CompoundIdentifier(parts) => {
            let [qualifier, column] = parts.as_slice() else {
                return Err(unsupported("expected a direct table or alias qualifier"));
            };
            if qualifier.quote_style.is_some()
                || column.quote_style.is_some()
                || !scope
                    .qualifier
                    .is_some_and(|name| name.eq_ignore_ascii_case(&qualifier.value))
            {
                return Err(unsupported(
                    "column qualifier does not match the visible table",
                ));
            }
            scope.resolve(column, range)?
        }
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
        } => cast_literal(expr, data_type)?,
        Expr::Value(value) => plain_literal(node, &value.value)?,
        Expr::Nested(inner) => {
            let mut bound = bind(inner, scope, depth + 1)?;
            if let Some(enclosing) = scope.enclosing_parentheses(bound.span) {
                bound.span = span(enclosing);
            }
            return Ok(bound);
        }
        Expr::BinaryOp { left, op, right } => {
            let operator = match op {
                BinaryOperator::Plus => "+",
                BinaryOperator::Minus => "-",
                BinaryOperator::Multiply => "*",
                BinaryOperator::Divide => "/",
                _ => return Err(unsupported("binary operator outside the arithmetic slice")),
            };
            let (ty, operands) =
                infer_call(operator, true, vec![left, right], scope, range, depth)?;
            if scope.preserve_parentheses {
                for operand in &operands {
                    range = cover(range, operand.span);
                }
            }
            (ty, ExpressionKind::Binary { operator, operands })
        }
        Expr::Function(function) => {
            let (name, args) = scalar_args(function)?;
            let name = name.to_ascii_uppercase();
            let (ty, operands) = infer_call(&name, false, args, scope, range, depth)?;
            (ty, ExpressionKind::Function { name, operands })
        }
        Expr::Trim {
            trim_where: None,
            trim_what: None,
            expr,
            trim_characters: None,
        }
        | Expr::Ceil {
            expr,
            field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
        }
        | Expr::Floor {
            expr,
            field: CeilFloorKind::DateTimeField(DateTimeField::NoDateTime),
        } => {
            let name = match node {
                Expr::Trim { .. } => "TRIM",
                Expr::Ceil { .. } => "CEIL",
                _ => "FLOOR",
            };
            let (ty, operands) = infer_call(name, false, vec![expr], scope, range, depth)?;
            (
                ty,
                ExpressionKind::Function {
                    name: name.into(),
                    operands,
                },
            )
        }
        _ => return Err(unsupported("expression outside the scalar binder")),
    };
    Ok(BoundExpression {
        ty,
        span: range,
        kind,
    })
}

fn plain_select(query: &Query) -> Result<&Select, Failure> {
    select_without_modifiers(query, false)
}

fn query_body(query: &Query) -> Result<&SetExpr, Failure> {
    let Query {
        with: None,
        body,
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks,
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators,
    } = query
    else {
        return Err(unsupported(
            "query modifiers are outside the generated projection",
        ));
    };
    if !locks.is_empty() || !pipe_operators.is_empty() {
        return Err(unsupported("query locks and pipes are outside the probe"));
    }
    Ok(body.as_ref())
}

fn select_without_modifiers(query: &Query, allow_table_clauses: bool) -> Result<&Select, Failure> {
    let SetExpr::Select(select) = query_body(query)? else {
        return Err(unsupported("expected one SELECT"));
    };
    let Select {
        select_token: _,
        optimizer_hints,
        distinct: None,
        select_modifiers: None,
        top: None,
        top_before_distinct: _,
        projection: _,
        exclude: None,
        into: None,
        from: _,
        lateral_views,
        prewhere: None,
        selection,
        connect_by,
        group_by,
        cluster_by,
        distribute_by,
        sort_by,
        having: None,
        named_window,
        qualify: None,
        window_before_qualify: _,
        value_table_mode: None,
        flavor: SelectFlavor::Standard,
    } = select.as_ref()
    else {
        return Err(unsupported("expected a plain SELECT projection"));
    };
    if !allow_table_clauses && selection.is_some() {
        return Err(unsupported("expected a plain SELECT projection"));
    }
    if !matches!(group_by, GroupByExpr::Expressions(items, modifiers)
        if modifiers.is_empty() && (items.is_empty() || allow_table_clauses))
        || !optimizer_hints.is_empty()
        || !lateral_views.is_empty()
        || !connect_by.is_empty()
        || !cluster_by.is_empty()
        || !distribute_by.is_empty()
        || !sort_by.is_empty()
        || !named_window.is_empty()
    {
        return Err(unsupported(
            "SELECT clauses are outside the generated projection",
        ));
    }
    Ok(select)
}

pub fn bind_generated_projection(sql: &str) -> Result<BoundProjection, Failure> {
    let statements = parse_feldera(sql).map_err(parse_failure)?;
    bind_projection(sql, &statements, true)
}

/// Bind a scalar view over one directly declared table in the fixed S1 profile.
pub fn bind_scalar_projection(sql: &str) -> Result<BoundProjection, Failure> {
    let statements = parse_feldera(sql).map_err(parse_failure)?;
    bind_projection(sql, &statements, false)
}

#[derive(Deserialize)]
pub struct Submission {
    sql: String,
    many: bool,
    batch: u64,
}

/// Keep API submission boundaries and their visible source coordinates.
pub fn bind_scalar_submissions(
    submissions: &[Submission],
) -> Result<(BoundProjection, String), Failure> {
    let (statements, source) = parse_submissions(submissions, |_| {})?;
    let result = bind_projection(&source, &statements, false)?;
    Ok((result, source))
}

fn parse_submissions(
    submissions: &[Submission],
    mut record_start: impl FnMut(Location),
) -> Result<(Vec<Statement>, String), Failure> {
    let mut statements = Vec::new();
    let mut lines = Vec::new();
    for submission in submissions {
        if submission.batch != 0 {
            return Err(unsupported("expected one compilation batch"));
        }
        let parsed = feldera_sql_parser::parse_at_with_starts(
            &submission.sql,
            lines.len() as u64,
            &mut record_start,
        )
        .map_err(parse_failure)?;
        if !submission.many && parsed.len() != 1 {
            return Err(parse_failure(
                "single-statement submission requires exactly one statement",
            ));
        }
        statements.extend(parsed);
        let trimmed = submission.sql.trim_end_matches('\n');
        if !trimmed.is_empty() || submission.sql.is_empty() {
            lines.extend(trimmed.split('\n'));
        }
    }
    Ok((statements, lines.join("\n")))
}

fn parse_failure(error: impl std::fmt::Display) -> Failure {
    Failure {
        category: "parse_error",
        message: error.to_string(),
        span: None,
        diagnostic_span: None,
        compiler_diagnostic: None,
    }
}

fn local_name(name: &ObjectName) -> Result<&Ident, Failure> {
    match name.0.as_slice() {
        [ObjectNamePart::Identifier(ident)]
            if ident.quote_style.is_none() && ident.value.is_ascii() =>
        {
            Ok(ident)
        }
        _ => Err(unsupported("expected an unquoted local relation name")),
    }
}

fn system_relation(name: &str) -> bool {
    matches!(name, "now" | "error_view" | "feldera_error_table")
}

fn validate_plain_view(view: &sqlparser_064::ast::CreateView) -> Result<(), Failure> {
    if view.or_alter
        || view.or_replace
        || view.secure
        || view.name_before_not_exists
        || !matches!(view.options, CreateTableOptions::None)
        || !view.cluster_by.is_empty()
        || view.comment.is_some()
        || view.with_no_schema_binding
        || view.if_not_exists
        || view.temporary
        || view.copy_grants
        || view.to.is_some()
        || view.params.is_some()
    {
        return Err(unsupported("expected a plain view declaration"));
    }
    Ok(())
}

fn bind_projection(
    sql: &str,
    statements: &[Statement],
    generated: bool,
) -> Result<BoundProjection, Failure> {
    if let Some(Statement::Upstream(last)) = statements.last()
        && matches!(last.as_ref(), Upstream::CreateView(view) if !view.columns.is_empty())
    {
        return Err(unsupported("explicit view columns require catalog binding"));
    }
    bind_query_projection(sql, statements, generated)
}

fn bind_query_projection(
    sql: &str,
    statements: &[Statement],
    generated: bool,
) -> Result<BoundProjection, Failure> {
    let [declarations @ .., Statement::Upstream(view)] = statements else {
        return Err(unsupported(
            "expected one table declaration followed by one view declaration",
        ));
    };
    if generated && declarations.len() != 1 {
        return Err(unsupported(
            "expected table declarations before the scalar view",
        ));
    }
    let Upstream::CreateView(view) = view.as_ref() else {
        return Err(unsupported("expected a trailing CREATE VIEW"));
    };
    let view_name = local_name(&view.name)?.value.to_ascii_lowercase();
    if system_relation(&view_name) {
        return Err(unsupported("view name conflicts with a system relation"));
    }
    if generated {
        if view.name.to_string() != "v" {
            return Err(unsupported("outside the generated table/view declarations"));
        }
    } else {
        validate_plain_view(view)?;
    }
    let select = plain_select(&view.query)?;
    let input = projection_input(select, generated)?;
    if generated && input.is_none() {
        return Err(unsupported("expected one table input"));
    }
    let mut tables = HashSet::new();
    let mut columns = input.is_none().then(Vec::new);
    for declaration in declarations {
        let Statement::Upstream(declaration) = declaration else {
            return Err(unsupported(
                "only plain table declarations can precede the scalar view",
            ));
        };
        let Upstream::CreateTable(table) = declaration.as_ref() else {
            return Err(unsupported(
                "only plain table declarations can precede the scalar view",
            ));
        };
        let declared = local_name(&table.name)?.value.to_ascii_lowercase();
        if system_relation(&declared) || declared == view_name || !tables.insert(declared.clone()) {
            return Err(unsupported("ambiguous scalar projection relation name"));
        }
        let bound = bind_table_columns(table, generated)?;
        if input.as_ref().is_some_and(|input| input.name == declared) {
            columns = Some(bound);
        }
    }
    let columns = columns.ok_or_else(|| unsupported("scalar view requires a declared table"))?;
    let (expr, alias, projection_span) = match select.projection.as_slice() {
        [SelectItem::ExprWithAlias { expr, alias }] => (
            expr,
            Some(alias),
            projection_token_span(sql, select.select_token.0.span, alias.span)
                .unwrap_or_else(|| expr.span()),
        ),
        [SelectItem::UnnamedExpr(expr @ Expr::Identifier(ident))] if !generated => {
            (expr, Some(ident), ident.span)
        }
        [SelectItem::UnnamedExpr(expr @ Expr::CompoundIdentifier(parts))] if !generated => {
            (expr, parts.last(), expr.span())
        }
        [SelectItem::UnnamedExpr(expr)] if !generated => (expr, None, expr.span()),
        _ => return Err(unsupported("expected one named projection")),
    };
    let scope = ColumnScope {
        columns: &columns,
        qualifier: input
            .as_ref()
            .filter(|_| !generated)
            .map(|input| input.qualifier.as_str()),
        sql,
        preserve_parentheses: !generated,
        tokens: OnceCell::new(),
    };
    let (expression, projection_span, warning) =
        bind_projection_value(&scope, expr, projection_span, input.is_none())?;
    Ok(BoundProjection {
        name: alias.map_or_else(|| "EXPR$0".into(), |ident| ident.value.clone()),
        result: expression.ty,
        table: input.map(|input| input.name),
        column_types: columns,
        expression,
        compiler_diagnostics: warning.into_iter().collect(),
        output_quoted: alias.is_none_or(|ident| ident.quote_style.is_some()),
        materialized_view: view.materialized,
        view_name,
        local_view: view.local.is_some(),
        projection_span,
    })
}

fn bind_projection_value(
    scope: &ColumnScope<'_>,
    expr: &Expr,
    projection_span: Span,
    constant: bool,
) -> Result<(BoundExpression, Span, Option<CompilerDiagnostic>), Failure> {
    let sql = scope.sql;
    let expression = bind(expr, scope, 0)
        .map_err(|failure| failure.with_compiler_diagnostic(sql, Some(projection_span)))?;
    let projection_span = if !scope.preserve_parentheses {
        projection_span
    } else {
        from_span_array(cover(span(projection_span), expression.span))
    };
    let warning = projection_warning(&expression)
        .and_then(|(kind, message)| projection_diagnostic(sql, projection_span, kind, &message));
    let expression = if constant && !matches!(expression.kind, ExpressionKind::TypedLiteral { .. })
    {
        constant::fold(expression)?
    } else {
        expression
    };
    Ok((expression, projection_span, warning))
}

fn projection_input(select: &Select, generated: bool) -> Result<Option<TableInput>, Failure> {
    let [input] = select.from.as_slice() else {
        return if select.from.is_empty() {
            Ok(None)
        } else {
            Err(unsupported("expected at most one direct table input"))
        };
    };
    let TableFactor::Table {
        name,
        alias,
        args: None,
        with_hints,
        optimizer_hints,
        version: None,
        with_ordinality: false,
        partitions,
        json_path: None,
        sample: None,
        index_directives,
        inheritance: None,
    } = &input.relation
    else {
        return Err(unsupported("expected a direct table input"));
    };
    if !input.joins.is_empty()
        || !with_hints.is_empty()
        || !optimizer_hints.is_empty()
        || !partitions.is_empty()
        || !index_directives.is_empty()
    {
        return Err(unsupported("expected an unfiltered table scan"));
    }
    let name = local_name(name)?.value.to_ascii_lowercase();
    let qualifier = match alias {
        None => name.clone(),
        Some(alias)
            if !generated
                && alias.columns.is_empty()
                && alias.at.is_none()
                && alias.name.quote_style.is_none() =>
        {
            alias.name.value.to_ascii_lowercase()
        }
        Some(_) => return Err(unsupported("expected a plain table alias")),
    };
    Ok(Some(TableInput { name, qualifier }))
}

fn bind_table_columns(
    table: &sqlparser_064::ast::CreateTable,
    generated: bool,
) -> Result<Vec<Column>, Failure> {
    if !table.constraints.is_empty()
        || table.columns.iter().any(|col| {
            col.options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::PrimaryKey(_)))
        })
    {
        return Err(unsupported("key constraints require catalog binding"));
    }
    map_table_columns(table, generated, |column, nullable| {
        Ok(Column {
            name: column.name.value.clone(),
            ty: checked_type(&column.data_type, nullable)?,
            quoted: column.name.quote_style.is_some(),
        })
    })
}

fn map_table_columns<T>(
    table: &sqlparser_064::ast::CreateTable,
    generated: bool,
    mut bind_column: impl FnMut(&sqlparser_064::ast::ColumnDef, bool) -> Result<T, Failure>,
) -> Result<Vec<T>, Failure> {
    if table.columns.is_empty() || table.query.is_some() {
        return Err(unsupported("expected a table with explicit scalar columns"));
    }
    if generated {
        if table.name.to_string() != "t" || table.columns.len() > 2 {
            return Err(unsupported("outside the generated table declaration"));
        }
    } else {
        let plain = CreateTableBuilder::new(table.name.clone())
            .columns(table.columns.clone())
            .constraints(table.constraints.clone())
            .build();
        if table != &plain {
            return Err(unsupported("expected a plain table declaration"));
        }
    }
    let mut identifiers = HashSet::new();
    table
        .columns
        .iter()
        .map(|column| {
            let nullable = match column.options.as_slice() {
                [] => true,
                [option] if matches!(option.option, ColumnOption::NotNull) => false,
                [option] if !generated && matches!(option.option, ColumnOption::Null) => true,
                [nullability, key]
                    if !generated
                        && matches!(nullability.option, ColumnOption::NotNull)
                        && matches!(key.option, ColumnOption::PrimaryKey(_)) =>
                {
                    false
                }
                _ => {
                    return Err(unsupported(
                        "column option outside the scalar table declaration",
                    ));
                }
            };
            let quoted = column.name.quote_style.is_some();
            if !generated && (quoted || !identifiers.insert(column.name.value.to_ascii_lowercase()))
            {
                return Err(unsupported("expected unique unquoted scalar column names"));
            }
            bind_column(column, nullable)
        })
        .collect()
}

fn projection_warning(expression: &BoundExpression) -> Option<(DiagnosticKind, String)> {
    use DiagnosticKind::{FloatingPointEquality, Noop, UselessOperation};
    let ExpressionKind::Function { name, operands } = &expression.kind else {
        return None;
    };
    let left = operands.first()?;
    let is_integer = matches!(left.ty.kind, Kind::Integer { .. });
    let is_null =
        matches!(&left.kind, ExpressionKind::TypedLiteral { literal, .. } if literal == "NULL");
    match name.as_str() {
        "CEIL" | "FLOOR" if is_integer && !is_null => Some((
            Noop,
            format!(
                "Function '{}' applied to integer arguments is useless.",
                name.to_ascii_lowercase()
            ),
        )),
        "ROUND" | "TRUNCATE" if is_integer => Some((
            UselessOperation,
            format!(
                "'{}' applied to integer value does nothing",
                name.to_ascii_lowercase()
            ),
        )),
        "NULLIF"
            if operands
                .iter()
                .any(|arg| matches!(arg.ty.kind, Kind::Real | Kind::Double)) =>
        {
            Some((
                FloatingPointEquality,
                "'NULLIF' compares floating point values for equality.".into(),
            ))
        }
        _ => None,
    }
}

/// Token boundaries retain punctuation absent from the AST expression span.
fn projection_token_span(sql: &str, select: Span, alias: Span) -> Option<Span> {
    let tokens = feldera_sql_parser::source_tokens(sql).ok()?;
    let mut projection = tokens.iter().filter(|token| {
        !matches!(token.token, Token::Whitespace(_))
            && token.span.start >= select.end
            && token.span.end <= alias.start
    });
    let first = projection.next()?;
    let mut last = projection.next_back().unwrap_or(first);
    if matches!(&last.token, Token::Word(word) if word.quote_style.is_none() && word.keyword == Keyword::AS)
    {
        last = projection.next_back().unwrap_or(first);
    }
    Some(Span {
        start: first.span.start,
        end: last.span.end,
    })
}

fn projection_diagnostic(
    sql: &str,
    Span { start, end }: Span,
    kind: DiagnosticKind,
    message: &str,
) -> Option<CompilerDiagnostic> {
    let lines: Vec<_> = sql.trim_end_matches('\n').split('\n').collect();
    let range = feldera_sql_source::SourceRange::from_character_span(
        &lines,
        (start.line, start.column),
        (end.line, end.column),
    )?;
    let snippet = range.snippet(&lines).ok()?;
    Some(CompilerDiagnostic {
        warning: !matches!(
            kind,
            DiagnosticKind::CompilationError | DiagnosticKind::ReservedName
        ),
        error_type: kind.error_type(),
        start_line_number: range.start_line_number as usize,
        start_column: range.start_column as usize,
        end_line_number: range.end_line_number as usize,
        end_column: range.end_column as usize,
        message: message.into(),
        snippet,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_conflicting_with_scalar_functions_have_a_product_diagnostic() {
        let error = bind_sql("test".into(), "CREATE TABLE LoG(x INT);").unwrap_err();
        assert_eq!(error.category, "reserved_name");
        let message = error.compiler_diagnostic.unwrap();
        assert!(!message.warning);
        assert_eq!(message.error_type, "Reserved name");
        assert_eq!((message.start_column, message.end_column), (14, 16));
        assert_eq!(
            message.message,
            "Table 'log' has the same name as a predefined function"
        );
        assert!(bind_sql("test".into(), "CREATE TABLE measurements(x INT);").is_ok());
    }

    #[test]
    fn resolves_column_and_preserves_literal_null_and_coercion() {
        let bound = bind_generated_projection(
            "CREATE TABLE t(a CHAR(5) NOT NULL);\nCREATE VIEW v AS SELECT NULLIF(a, CAST(NULL AS CHAR(1))) AS result FROM t",
        ).unwrap();
        assert!(bound.result.nullable);
        let ExpressionKind::Function { operands, .. } = bound.expression.kind else {
            panic!("expected a function")
        };
        assert!(matches!(
            operands[0].kind,
            ExpressionKind::Column { ordinal: 0 }
        ));
        assert!(
            matches!(operands[1].kind, ExpressionKind::TypedLiteral { ref literal, .. } if literal == "NULL")
        );
        assert_eq!(operands[0].span[0], 2);
    }

    #[test]
    fn rejected_expression_keeps_the_source_span() {
        let error = bind_generated_projection(
            "CREATE TABLE t(a BOOLEAN);\nCREATE VIEW v AS SELECT ABS(a) AS result FROM t",
        )
        .unwrap_err();
        assert_eq!(error.category, "type_rejection");
        assert_eq!(error.span.unwrap()[0], 2);
    }

    #[test]
    fn generated_type_errors_retain_the_full_projection_token_range() {
        for (projection, end_column) in [
            ("ABS(CAST(TRUE AS BOOLEAN))", 50),
            ("(a) + (CAST(TRUE AS BOOLEAN))", 53),
            ("CEIL(a)", 31),
        ] {
            let sql = format!(
                "CREATE TABLE t(a BOOLEAN NOT NULL);\nCREATE VIEW v AS SELECT {projection} AS result FROM t;"
            );
            let failure = bind_generated_projection(&sql).unwrap_err();
            let diagnostic = failure.compiler_diagnostic.unwrap();
            assert_eq!(diagnostic.error_type, "Compilation error");
            assert!(!diagnostic.warning);
            assert_eq!(
                (diagnostic.start_line_number, diagnostic.end_line_number),
                (2, 2)
            );
            assert_eq!(
                (diagnostic.start_column, diagnostic.end_column),
                (25, end_column)
            );
            assert_eq!(
                diagnostic.snippet,
                format!(
                    "    1|CREATE TABLE t(a BOOLEAN NOT NULL);\n    2|CREATE VIEW v AS SELECT {projection} AS result FROM t;\n{}{}\n",
                    " ".repeat(30),
                    "^".repeat(end_column - 24)
                )
            );
        }
    }

    #[test]
    fn generated_warning_rules_preserve_type_and_null_behavior() {
        for (projection, expected) in [
            ("CEIL(a)", Some("Noop")),
            ("CEIL(CAST(NULL AS INTEGER))", None),
            ("ROUND(CAST(NULL AS INTEGER))", Some("Useless operation")),
            ("NULLIF(a, b)", Some("Floating point equality")),
            ("NULLIF(a, a)", None),
        ] {
            let sql = format!(
                "CREATE TABLE t(a INTEGER, b REAL);\nCREATE VIEW v AS SELECT {projection} AS result FROM t;"
            );
            let bound = bind_generated_projection(&sql).unwrap();
            let warning = bound.compiler_diagnostics.first();
            assert_eq!(warning.map(|warning| warning.error_type), expected);
            if let Some(warning) = warning {
                assert!(warning.warning);
                assert_eq!(warning.start_column, 25);
                assert_eq!(warning.end_column, 24 + projection.len());
            }
        }
    }

    #[test]
    fn constant_projection_uses_checked_arithmetic() {
        let bound = bind_scalar_projection("CREATE VIEW v AS SELECT 1 + 2 * 3;").unwrap();
        assert!(matches!(
            bound.expression.kind,
            ExpressionKind::FoldedLiteral {
                value: feldera_sql_rel_model::literals::LiteralValue::Integer { value }
            } if value == "7"
        ));
        let overflow = bind_scalar_projection("CREATE VIEW v AS SELECT 2147483647 + 1;").unwrap();
        assert!(matches!(
            overflow.expression.kind,
            ExpressionKind::Binary { operator: "+", .. }
        ));
    }

    #[test]
    fn alias_replaces_the_table_qualifier() {
        let prefix = "CREATE TABLE t (id INTEGER); CREATE VIEW v AS ";
        let bound = bind_scalar_projection(&format!("{prefix}SELECT a.id FROM t AS a;"))
            .expect("alias resolves the column");
        assert_eq!(bound.name, "id");
        assert!(matches!(
            bound.expression.kind,
            ExpressionKind::Column { ordinal: 0 }
        ));
        let failure = bind_scalar_projection(&format!("{prefix}SELECT t.id FROM t AS a;"))
            .expect_err("alias hides the table qualifier");
        assert_eq!(failure.category, "outside_scope");
    }

    #[test]
    fn unqualified_niladic_function_takes_precedence_over_a_column() {
        let sql = "CREATE TABLE events(user INT);\nCREATE VIEW v AS SELECT user FROM events;";
        let failure = bind_scalar_projection(sql).unwrap_err();
        assert_eq!(failure.category, "unsupported_niladic_function");
        let diagnostic = failure.compiler_diagnostic.unwrap();
        assert_eq!(diagnostic.error_type, "Compilation error");
        assert!(!diagnostic.warning);
        assert_eq!(
            (diagnostic.start_line_number, diagnostic.start_column),
            (2, 25)
        );
        assert_eq!(diagnostic.end_column, 28);
        assert_eq!(
            diagnostic.snippet,
            "    1|CREATE TABLE events(user INT);\n    2|CREATE VIEW v AS SELECT user FROM events;\n                              ^^^^\n"
        );
    }

    #[test]
    fn refuses_query_modifiers_instead_of_dropping_them() {
        for query in [
            "SELECT DISTINCT a AS result FROM t",
            "SELECT a AS result FROM t LIMIT 1",
        ] {
            let error = bind_generated_projection(&format!(
                "CREATE TABLE t(a INTEGER); CREATE VIEW v AS {query}"
            ))
            .unwrap_err();
            assert_eq!(error.category, "outside_scope");
        }
    }
}
