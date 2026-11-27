//! Render the supported constant statements and recover their source positions.

use super::super::{BinaryOperator, CastKind, Expr, Failure, Value, parse_failure, unsupported};
use feldera_sql_rel_model::relation_metadata::Coordinates;
use sqlparser_064::{
    keywords::Keyword,
    tokenizer::{Span, Token, TokenWithSpan},
};

pub(super) struct SqlDisplay {
    pub(super) pretty: String,
    pub(super) diagnostic: String,
    precedence: u8,
}

impl SqlDisplay {
    pub(super) fn atom(text: String) -> Self {
        Self {
            diagnostic: text.clone(),
            pretty: text,
            precedence: u8::MAX,
        }
    }
}

pub(super) fn display_constant(expression: &Expr) -> Result<SqlDisplay, Failure> {
    match expression {
        Expr::Value(value) => match &value.value {
            Value::Boolean(value) => Ok(SqlDisplay::atom(
                if *value { "TRUE" } else { "FALSE" }.into(),
            )),
            Value::Null => Ok(SqlDisplay::atom("NULL".into())),
            _ => display_integer(&value.value).map(SqlDisplay::atom),
        },
        Expr::Nested(expression) => display_constant(expression),
        Expr::BinaryOp { left, op, right } => {
            let precedence = match op {
                BinaryOperator::Plus | BinaryOperator::Minus => 1,
                BinaryOperator::Multiply | BinaryOperator::Divide => 2,
                _ => return Err(unsupported("operator outside the constant program slice")),
            };
            let left = display_constant(left)?;
            let right = display_constant(right)?;
            let diagnostic = format!("({} {op} {})", left.diagnostic, right.diagnostic);
            let operand = |display: SqlDisplay, minimum| {
                if display.precedence < minimum {
                    format!("({})", display.pretty)
                } else {
                    display.pretty
                }
            };
            let pretty = format!(
                "{} {op} {}",
                operand(left, precedence),
                operand(right, precedence + 1)
            );
            Ok(SqlDisplay {
                pretty,
                diagnostic,
                precedence,
            })
        }
        Expr::Cast {
            kind: CastKind::Cast,
            expr,
            data_type,
            format: None,
        } => {
            let operand = display_constant(expr)?;
            let ty = super::super::checked_type(data_type, false)?;
            let name = match ty.kind {
                super::super::Kind::Integer { bits: 8 } => "TINYINT",
                super::super::Kind::Integer { bits: 16 } => "SMALLINT",
                super::super::Kind::Integer { bits: 32 } => "INTEGER",
                super::super::Kind::Integer { bits: 64 } => "BIGINT",
                super::super::Kind::Boolean => "BOOLEAN",
                _ => return Err(unsupported("cast type outside the constant program slice")),
            };
            Ok(SqlDisplay::atom(format!(
                "CAST({} AS {name})",
                operand.pretty
            )))
        }
        Expr::Function(function) => {
            let (name, operands) = super::super::scalar_args(function)?;
            let [Expr::Value(value)] = operands.as_slice() else {
                return Err(unsupported(
                    "expected one constant numeric function argument",
                ));
            };
            if !name.eq_ignore_ascii_case("TAN") {
                return Err(unsupported(
                    "constant function is outside the program slice",
                ));
            }
            Ok(SqlDisplay::atom(format!(
                "TAN({})",
                display_integer(&value.value)?
            )))
        }
        _ => Err(unsupported(
            "constant expression is outside the program slice",
        )),
    }
}

fn display_integer(value: &Value) -> Result<String, Failure> {
    let Value::Number(value, _) = value else {
        return Err(unsupported("expected an integer literal"));
    };
    value
        .parse::<i64>()
        .map(|value| value.to_string())
        .map_err(|_| unsupported("expected an exact integer literal"))
}

pub(super) fn statement_position(sql: &str) -> Result<Coordinates, Failure> {
    let tokens = significant_tokens(sql)?;
    token_range(&tokens)
}

fn significant_tokens(sql: &str) -> Result<Vec<TokenWithSpan>, Failure> {
    let tokens = feldera_sql_parser::source_tokens(sql).map_err(parse_failure)?;
    Ok(tokens
        .into_iter()
        .filter(|token| {
            !matches!(
                token.token,
                Token::Whitespace(_) | Token::SemiColon | Token::EOF
            )
        })
        .collect())
}

fn token_range(tokens: &[TokenWithSpan]) -> Result<Coordinates, Failure> {
    let first = tokens
        .first()
        .ok_or_else(|| unsupported("missing SQL source"))?;
    let last = tokens
        .last()
        .ok_or_else(|| unsupported("incomplete SQL source"))?;
    Ok(coordinates(Span {
        start: first.span.start,
        end: last.span.end,
    }))
}

pub(super) fn end_at_call_close(sql: &str, call: Span, last: Span) -> Result<Coordinates, Failure> {
    let closing = significant_tokens(sql)?
        .into_iter()
        .find(|token| token.span.start >= last.end)
        .filter(|token| token.token == Token::RParen)
        .ok_or_else(|| unsupported("missing closing parenthesis of scalar call"))?;
    Ok(coordinates(Span {
        start: call.start,
        end: closing.span.end,
    }))
}

pub(super) fn simple_aggregate_position(sql: &str, call: Span) -> Result<Coordinates, Failure> {
    let closing = significant_tokens(sql)?
        .into_iter()
        .find(|token| token.span.start > call.start && token.token == Token::RParen)
        .ok_or_else(|| unsupported("missing closing parenthesis of aggregate call"))?;
    Ok(coordinates(Span {
        start: call.start,
        end: closing.span.end,
    }))
}

pub(super) fn end_at_is_true(sql: &str, operand: Span) -> Result<Coordinates, Failure> {
    let tokens = significant_tokens(sql)?;
    let mut suffix = tokens
        .iter()
        .filter(|token| token.span.start >= operand.end);
    let (Some(is), Some(truth)) = (suffix.next(), suffix.next()) else {
        return Err(unsupported("missing IS TRUE predicate suffix"));
    };
    if !matches!(&is.token, Token::Word(word) if word.keyword == Keyword::IS)
        || !matches!(&truth.token, Token::Word(word) if word.keyword == Keyword::TRUE)
    {
        return Err(unsupported("missing IS TRUE predicate suffix"));
    }
    Ok(coordinates(Span {
        start: operand.start,
        end: truth.span.end,
    }))
}

pub(super) fn coordinates(span: Span) -> Coordinates {
    Coordinates {
        line: span.start.line as u32,
        column: span.start.column as u32,
        end_line: span.end.line as u32,
        end_column: span.end.column.saturating_sub(1) as u32,
        quoted: false,
    }
}

pub(super) struct DeclarationTokens(Vec<TokenWithSpan>);

impl DeclarationTokens {
    pub(super) fn split(sql: &str, starts: &[crate::Location]) -> Result<Vec<Self>, Failure> {
        let mut declarations: Vec<Self> = Vec::new();
        let mut starts = starts.iter().copied().peekable();
        for token in significant_tokens(sql)? {
            if starts.next_if_eq(&token.span.start).is_some() {
                declarations.push(Self(Vec::new()));
            }
            declarations
                .last_mut()
                .ok_or_else(|| unsupported("missing statement start"))?
                .0
                .push(token);
        }
        Ok(declarations)
    }

    pub(super) fn range(&self) -> Result<Coordinates, Failure> {
        token_range(&self.0)
    }

    pub(super) fn column_type_position(&self, name: Span) -> Result<Coordinates, Failure> {
        let next = self.0.partition_point(|token| token.span.start < name.end);
        self.0
            .get(next)
            .map(|token| coordinates(token.span))
            .ok_or_else(|| unsupported("missing column type after its name"))
    }

    pub(super) fn alias_type_position(&self) -> Result<Coordinates, Failure> {
        self.0
            .windows(2)
            .find_map(|pair| {
                matches!(&pair[0].token, Token::Word(w) if w.keyword == Keyword::AS)
                    .then(|| coordinates(pair[1].span))
            })
            .ok_or_else(|| unsupported("missing type after AS"))
    }

    /// Find the table constraint between columns. An inline PRIMARY KEY
    /// follows the column's type or nullability marker.
    pub(super) fn primary_key_position(&self) -> Option<sqlparser_064::tokenizer::Location> {
        self.0.windows(2).find_map(|tokens| {
            (matches!(tokens[0].token, Token::Comma | Token::LParen)
                && matches!(&tokens[1].token, Token::Word(w) if w.keyword == Keyword::PRIMARY))
            .then_some(tokens[1].span.start)
        })
    }

    pub(super) fn column_ranges(
        &self,
        column_spans: impl IntoIterator<Item = Span>,
    ) -> Result<Vec<Coordinates>, Failure> {
        let Some((closing, columns)) = self.0.split_last() else {
            return Err(unsupported("missing table column source"));
        };
        if closing.token != Token::RParen {
            return Err(unsupported("unexpected statement boundary in table source"));
        }
        let column_indices = column_spans
            .into_iter()
            .map(|span| {
                columns
                    .binary_search_by_key(&span.start, |token| token.span.start)
                    .map_err(|_| unsupported("missing position of the declared column"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut ranges = Vec::with_capacity(column_indices.len());
        let mut starts = column_indices.into_iter().peekable();
        while let Some(start) = starts.next() {
            let end = match starts.peek() {
                Some(&next) if columns[next - 1].token == Token::Comma => next - 1,
                Some(_) => return Err(unsupported("missing column separator")),
                None => columns.len(),
            };
            // The diagnostic origin covers the column name, type and
            // nullability. PRIMARY KEY contributes to the printed text only.
            let column = &columns[start..end];
            let key = column
                .iter()
                .position(|t| matches!(&t.token, Token::Word(w) if w.keyword == Keyword::PRIMARY));
            let mut end = key.unwrap_or(column.len());
            if end > 0 && column[end - 1].token == Token::Comma {
                end -= 1;
            }
            ranges.push(token_range(&column[..end])?);
        }
        Ok(ranges)
    }
}
