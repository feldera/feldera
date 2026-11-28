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
    pub(super) fn literal(text: &super::literal::CellText) -> Self {
        Self {
            pretty: text.render(true).into_owned(),
            diagnostic: text.render(false).into_owned(),
            precedence: u8::MAX,
        }
    }

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

pub(super) struct DeclarationTokens<'a> {
    pub(super) sql: &'a str,
    lines: &'a [&'a str],
    tokens: Vec<TokenWithSpan>,
}

impl<'a> DeclarationTokens<'a> {
    pub(super) fn single(sql: &'a str, lines: &'a [&'a str]) -> Result<Self, Failure> {
        Ok(Self {
            sql,
            lines,
            tokens: significant_tokens(sql)?,
        })
    }

    pub(super) fn split(
        sql: &'a str,
        lines: &'a [&'a str],
        starts: &[crate::Location],
    ) -> Result<Vec<Self>, Failure> {
        let mut declarations = Vec::new();
        let mut starts = starts.iter().copied().peekable();
        for token in significant_tokens(sql)? {
            if starts.next_if_eq(&token.span.start).is_some() {
                declarations.push(Self {
                    sql,
                    lines,
                    tokens: Vec::new(),
                });
            }
            declarations
                .last_mut()
                .ok_or_else(|| unsupported("missing statement start"))?
                .tokens
                .push(token);
        }
        Ok(declarations)
    }

    /// Parser columns count characters. The Java compiler publishes inclusive
    /// UTF-16 positions for statements, table metadata, and expression sites.
    pub(super) fn coordinates(&self, span: Span) -> Result<Coordinates, Failure> {
        let range = feldera_sql_source::SourceRange::from_character_span(
            self.lines,
            (span.start.line, span.start.column),
            (span.end.line, span.end.column),
        )
        .ok_or_else(|| unsupported("invalid SQL source range"))?;
        Ok(Coordinates {
            line: range.start_line_number as u32,
            column: range.start_column as u32,
            end_line: range.end_line_number as u32,
            end_column: range.end_column as u32,
            quoted: false,
        })
    }

    pub(super) fn distinct_position(&self, select: Span) -> Result<Coordinates, Failure> {
        let next = self.tokens.partition_point(|t| t.span.start < select.end);
        let token = self
            .tokens
            .get(next)
            .filter(|t| matches!(&t.token, Token::Word(w) if w.keyword == Keyword::DISTINCT))
            .ok_or_else(|| unsupported("missing DISTINCT modifier"))?;
        self.coordinates(token.span)
    }

    /// Calcite includes explicit parentheses in a literal's parser position.
    pub(super) fn literal_position(&self, expr: &Expr) -> Result<Coordinates, Failure> {
        use sqlparser_064::ast::Spanned;
        let span = expr.span();
        let mut first = self.tokens.partition_point(|t| t.span.start < span.start);
        let mut last = self.tokens.partition_point(|t| t.span.start < span.end);
        let mut inner = expr;
        while let Expr::Nested(child) = inner {
            if first == 0
                || self.tokens[first - 1].token != Token::LParen
                || self
                    .tokens
                    .get(last)
                    .is_none_or(|t| t.token != Token::RParen)
            {
                return Err(unsupported("missing literal parentheses"));
            }
            first -= 1;
            last += 1;
            inner = child;
        }
        self.token_range(&self.tokens[first..last])
    }

    /// sqlparser's ARRAY span covers its elements. Recover the constructor
    /// and closing bracket from the original tokens of this scalar array.
    pub(super) fn scalar_array_position(
        &self,
        elements: Span,
        enclosing: usize,
    ) -> Result<Coordinates, Failure> {
        let first = self
            .tokens
            .partition_point(|t| t.span.start < elements.start);
        let last = self.tokens.partition_point(|t| t.span.start < elements.end);
        let open = self.tokens[..first].iter().rposition(|t| t.token == Token::LBracket)
            .and_then(|i| i.checked_sub(1))
            .filter(|&i| matches!(&self.tokens[i].token, Token::Word(w) if w.keyword == sqlparser_064::keywords::Keyword::ARRAY))
            .ok_or_else(|| unsupported("missing ARRAY constructor"))?;
        let close = self.tokens[last..]
            .iter()
            .position(|t| t.token == Token::RBracket)
            .ok_or_else(|| unsupported("missing closing ARRAY bracket"))?;
        let close = last + close;
        let start = open
            .checked_sub(enclosing)
            .ok_or_else(|| unsupported("missing parentheses around ARRAY"))?;
        let end = close + 1 + enclosing;
        if self.tokens[start..open]
            .iter()
            .any(|t| t.token != Token::LParen)
            || self
                .tokens
                .get(close + 1..end)
                .is_none_or(|suffix| suffix.iter().any(|t| t.token != Token::RParen))
        {
            return Err(unsupported("missing parentheses around ARRAY"));
        }
        self.token_range(&self.tokens[start..end])
    }

    fn token_range(&self, tokens: &[TokenWithSpan]) -> Result<Coordinates, Failure> {
        let first = tokens
            .first()
            .ok_or_else(|| unsupported("missing SQL source"))?;
        let last = tokens
            .last()
            .ok_or_else(|| unsupported("incomplete SQL source"))?;
        self.coordinates(first.span.union(&last.span))
    }

    pub(super) fn cast_position(
        &self,
        child: Span,
        enclosing: usize,
    ) -> Result<Coordinates, Failure> {
        let end = self.tokens.partition_point(|t| t.span.start < child.start);
        let name = self.tokens[..end]
            .iter()
            .rev()
            .find(|t| matches!(&t.token, Token::Word(w) if w.keyword == Keyword::CAST))
            .ok_or_else(|| unsupported("missing CAST before its literal operand"))?;
        self.call_position(name.span, enclosing)
    }

    /// Function spans in sqlparser can omit their closing delimiters. Calcite
    /// includes the whole call as well as parentheses around the expression.
    pub(super) fn call_position(
        &self,
        name: Span,
        enclosing: usize,
    ) -> Result<Coordinates, Failure> {
        let name_index = self.tokens.partition_point(|t| t.span.start < name.start);
        let first = name_index
            .checked_sub(enclosing)
            .ok_or_else(|| unsupported("missing parentheses around scalar call"))?;
        if self
            .tokens
            .get(name_index + 1)
            .is_none_or(|t| t.token != Token::LParen)
            || self.tokens[first..name_index]
                .iter()
                .any(|t| t.token != Token::LParen)
        {
            return Err(unsupported("missing scalar call arguments"));
        }
        let mut depth = 0;
        for (index, token) in self.tokens[first..].iter().enumerate() {
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth -= 1;
                    if depth == 0 {
                        return self.token_range(&self.tokens[first..=first + index]);
                    }
                }
                _ => {}
            }
        }
        Err(unsupported("missing closing parenthesis of scalar call"))
    }

    /// sqlparser spans omit CAST syntax and wrapping parentheses. Calcite
    /// also starts an unparenthesized postfix cast at the WHERE token.
    pub(super) fn comparison_position(
        &self,
        left: &Expr,
        right: &Expr,
    ) -> Result<Coordinates, Failure> {
        use sqlparser_064::ast::Spanned;
        let left = left.span();
        let first = self.tokens.partition_point(|t| t.span.start < left.start);
        let operator = self.tokens.partition_point(|t| t.span.start < left.end);
        let rhs_end = right.span().end;
        let mut end = operator + 1;
        let mut depth = 0usize;
        let mut bare_postfix = false;
        while let Some(token) = self.tokens.get(end) {
            if token.token == Token::DoubleColon {
                bare_postfix |= depth == 0;
                // The binder accepts only scalar signed integer target names here.
                end += 2;
                continue;
            }
            if token.span.start >= rhs_end && depth == 0 {
                break;
            }
            match token.token {
                Token::LParen => depth += 1,
                Token::RParen => {
                    depth = depth.checked_sub(1).ok_or_else(|| {
                        unsupported("unexpected closing parenthesis in comparison")
                    })?
                }
                _ => {}
            }
            end += 1;
        }
        let start = if bare_postfix
            && first > 0
            && matches!(&self.tokens[first - 1].token, Token::Word(w) if w.keyword == Keyword::WHERE)
        {
            first - 1
        } else {
            first
        };
        self.token_range(
            self.tokens
                .get(start..end)
                .ok_or_else(|| unsupported("missing integer comparison operand"))?,
        )
    }

    pub(super) fn end_at_is_true(&self, operand: Span) -> Result<Coordinates, Failure> {
        let index = self
            .tokens
            .partition_point(|token| token.span.start < operand.end);
        let [is, truth, ..] = &self.tokens[index..] else {
            return Err(unsupported("missing IS TRUE predicate suffix"));
        };
        if !matches!(&is.token, Token::Word(w) if w.keyword == Keyword::IS)
            || !matches!(&truth.token, Token::Word(w) if w.keyword == Keyword::TRUE)
        {
            return Err(unsupported("missing IS TRUE predicate suffix"));
        }
        self.coordinates(Span {
            start: operand.start,
            end: truth.span.end,
        })
    }

    pub(super) fn range(&self) -> Result<Coordinates, Failure> {
        self.token_range(&self.tokens)
    }

    pub(super) fn column_type_position(&self, name: Span) -> Result<Coordinates, Failure> {
        let next = self
            .tokens
            .partition_point(|token| token.span.start < name.end);
        self.tokens
            .get(next)
            .map(|token| self.coordinates(token.span))
            .ok_or_else(|| unsupported("missing column type after its name"))?
    }

    pub(super) fn alias_type_position(&self) -> Result<Coordinates, Failure> {
        self.tokens
            .windows(2)
            .find_map(|pair| {
                matches!(&pair[0].token, Token::Word(w) if w.keyword == Keyword::AS)
                    .then(|| self.coordinates(pair[1].span))
            })
            .ok_or_else(|| unsupported("missing type after AS"))?
    }

    /// Find the table constraint between columns. An inline PRIMARY KEY
    /// follows the column's type or nullability marker.
    pub(super) fn primary_key_position(&self) -> Option<sqlparser_064::tokenizer::Location> {
        self.tokens.windows(2).find_map(|tokens| {
            (matches!(tokens[0].token, Token::Comma | Token::LParen)
                && matches!(&tokens[1].token, Token::Word(w) if w.keyword == Keyword::PRIMARY))
            .then_some(tokens[1].span.start)
        })
    }

    pub(super) fn column_ranges(
        &self,
        column_spans: impl IntoIterator<Item = Span>,
    ) -> Result<Vec<Coordinates>, Failure> {
        let Some((closing, columns)) = self.tokens.split_last() else {
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
            ranges.push(self.token_range(&column[..end])?);
        }
        Ok(ranges)
    }
}
