//! Locations come from original tokens, never from normalized SQL text.

use sqlparser_064::{
    ast::DataType,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Location, Span, Token, TokenWithSpan},
};

pub(super) fn at(message: impl std::fmt::Display, location: Location) -> ParserError {
    ParserError::ParserError(format!("{message}{location}"))
}

pub(super) fn located(message: impl Into<String>, span: Span) -> ParserError {
    ParserError::Located {
        message: message.into(),
        diagnostic_class: None,
        span,
    }
}

/// Upstream combines punctuation from several dialects into one token.
/// JavaCC reports the individual Feldera token at which the expression fails.
pub(super) fn invalid_operator(parser: &Parser, prefix: bool) -> Option<ParserError> {
    let token = parser.peek_token_ref();
    let (offset, width) = match token.token {
        Token::DoubleEq
        | Token::DoubleTilde
        | Token::DoubleTildeAsterisk
        | Token::HashArrow
        | Token::HashLongArrow
        | Token::AtArrow
        | Token::AtAt
        | Token::AtQuestion
        | Token::Overlap
        | Token::DoubleExclamationMark
        | Token::PGSquareRoot
        | Token::Pipe
        | Token::Assignment => (0, 1),
        Token::Colon if prefix => (0, 1),
        Token::Gt | Token::ShiftRight
            if matches!(
                parser.peek_nth_token_ref(1).token,
                Token::Gt | Token::ShiftRight
            ) =>
        {
            (0, 1)
        }
        Token::Tilde
            if matches!(
                parser.peek_nth_token_ref(1).token,
                Token::Tilde | Token::TildeAsterisk
            ) =>
        {
            (0, 1)
        }
        Token::Tilde
        | Token::TildeAsterisk
        | Token::ExclamationMarkTilde
        | Token::ExclamationMarkTildeAsterisk
            if prefix =>
        {
            (0, 1)
        }
        // After a left operand, !~ starts a legal regex comparison and the
        // next tilde fails as its right operand.
        Token::ExclamationMarkDoubleTilde | Token::ExclamationMarkDoubleTildeAsterisk => {
            (if prefix { 0 } else { 2 }, 1)
        }
        Token::Sharp | Token::AtSign | Token::ArrowAt | Token::CaretAt => {
            let next = parser.peek_nth_token_ref(1);
            if next.token == Token::EOF && next.span.start == token.span.end {
                return Some(located("unrecognized character at end of input", next.span));
            }
            (
                u64::from(matches!(token.token, Token::ArrowAt | Token::CaretAt)),
                1,
            )
        }
        Token::PGCubeRoot => (0, 2),
        _ => return None,
    };
    let start = token.span.start;
    let location = |n| Location::new(start.line, start.column + n);
    Some(located(
        format!("unexpected operator {}", token.token),
        Span::new(location(offset), location(offset + width)),
    ))
}

/// A syntax failure and its original token span. Some tokenizer failures
/// provide only a point, and some parser errors have no recorded location.
pub struct SyntaxDiagnostic {
    pub message: String,
    pub span: Option<Span>,
    pub error_type: &'static str,
}

/// Inspect syntax independently of name resolution. Recursion-limit failures
/// propagate to the caller so they cannot be reported as invalid SQL.
pub fn syntax_diagnostic(sql: &str) -> Result<Option<SyntaxDiagnostic>, ParserError> {
    let message = match crate::parse(sql) {
        Ok(_) => return Ok(None),
        Err(ParserError::ParserError(message) | ParserError::TokenizerError(message)) => message,
        Err(ParserError::Located {
            message,
            diagnostic_class,
            span,
        }) => {
            return Ok(Some(SyntaxDiagnostic {
                message,
                span: Some(span),
                error_type: diagnostic_class.unwrap_or("Error parsing SQL"),
            }));
        }
        Err(error) => return Err(error),
    };
    // sqlparser 0.64 exposes diagnostic locations only as a terminal suffix.
    let start = message.rsplit_once("Line: ").and_then(|(_, suffix)| {
        let (line, column) = suffix.split_once(", Column: ")?;
        Some(Location::new(line.parse().ok()?, column.parse().ok()?))
    });
    let span = start.map(|start| {
        crate::source_tokens(sql)
            .ok()
            .and_then(|tokens| {
                if tokens.last().is_some_and(|last| last.span.end == start) {
                    // JavaCC locates EOF at the last character it read, including
                    // a trailing newline. Token ends are exclusive in sqlparser.
                    let last = if start.column > 1 {
                        Location::new(start.line, start.column - 1)
                    } else {
                        let previous_line = sql.strip_suffix('\n')?.rsplit('\n').next()?;
                        Location::new(
                            start.line.checked_sub(1)?,
                            previous_line.chars().count() as u64 + 1,
                        )
                    };
                    return Some(Span::new(last, last));
                }
                tokens
                    .into_iter()
                    .find(|t| t.span.start == start)
                    .map(|t| t.span)
            })
            .unwrap_or(Span::new(start, start))
    });
    Ok(Some(SyntaxDiagnostic {
        message,
        span,
        error_type: "Error parsing SQL",
    }))
}

/// The AST stores the width and modifiers but discards their locations.
/// Diagnose the first unsupported part while the parser still has its tokens.
pub(super) fn validate_data_type(
    ty: &DataType,
    source: &[TokenWithSpan],
) -> Result<(), ParserError> {
    let custom_params = matches!(ty, DataType::Custom(_, modifiers) if !modifiers.is_empty());
    if !custom_params && super::type_policy::allows(ty) {
        return Ok(());
    }
    let mut tokens = source
        .iter()
        .filter(|t| !matches!(t.token, Token::Whitespace(_)));
    let Some(first) = tokens.next() else {
        return Err(ParserError::ParserError(
            "missing source of an unsupported type".into(),
        ));
    };
    let keyword = |token: &TokenWithSpan| match &token.token {
        Token::Word(w) => w.keyword,
        _ => Keyword::NoKeyword,
    };
    let offending = if custom_params {
        tokens.find(|t| t.token == Token::LParen)
    } else {
        match ty {
            DataType::Datetime(_) | DataType::Set(_) => Some(first),
            DataType::DecimalUnsigned(_) | DataType::DecUnsigned(_) | DataType::DoublePrecisionUnsigned => {
                tokens.find(|t| keyword(t) == Keyword::UNSIGNED)
            }
            _ => {
                // A failed precision applies at the literal. An unsupported width
                // on a type such as INTEGER fails at the opening parenthesis.
                let numeric_precision = matches!(ty, DataType::Binary(_) | DataType::Varbinary(_) |
                    DataType::Character(_) | DataType::CharacterVarying(_) | DataType::Char(_) |
                    DataType::CharVarying(_) | DataType::Varchar(_) | DataType::Decimal(_) |
                    DataType::Dec(_) | DataType::Numeric(_) | DataType::Time(_, _) | DataType::Timestamp(_, _));
                let precision_error = numeric_precision.then(|| tokens.clone().find(|t|
                    matches!(&t.token, Token::Number(n, _) if n.parse::<u64>().is_ok_and(|v| v > i32::MAX as u64))
                    || matches!(keyword(t), Keyword::OCTETS | Keyword::CHARACTERS)
                )).flatten();
                precision_error.or_else(|| tokens.find(|t| keyword(t) != Keyword::VARYING))
            }
        }
    }.unwrap_or(first);
    let message = if custom_params {
        "user-defined types cannot have parameters"
    } else {
        "type parameter or shape is outside the Feldera grammar"
    };
    Err(at(message, offending.span.start))
}

#[cfg(test)]
mod tests {
    use crate::parse;

    #[test]
    fn type_diagnostics_point_to_the_offending_token() {
        for (source, line, column) in [
            ("CREATE TABLE t(x ROW(y INT(10)))", 1, 27),
            ("SELECT CAST(NULL AS DECIMA(5,2))", 1, 27),
            ("CREATE TABLE t(\n  x VARCHAR(2147483648))", 2, 13),
            ("CREATE TABLE t(x INT(2147483648))", 1, 21),
            ("CREATE TABLE t(x ROW(y VARBIT(10 OCTETS)))", 1, 30),
            ("SELECT CAST(NULL AS BINARY(MAX))", 1, 27),
        ] {
            let error = parse(source).unwrap_err().to_string();
            assert!(
                error.ends_with(&format!("Line: {line}, Column: {column}")),
                "{source}: {error}"
            );
        }
    }
}

#[cfg(test)]
mod span_tests {
    use super::syntax_diagnostic;
    use sqlparser_064::tokenizer::{Location, Span};

    #[test]
    fn operator_and_clause_errors_preserve_the_original_token() {
        for (sql, line, column, width) in [
            ("SELECT @name", 1, 8, 1),
            ("SELECT :name", 1, 8, 1),
            ("SELECT a == b", 1, 10, 1),
            ("SELECT a !~~ b", 1, 12, 1),
            ("SELECT a >>> b", 1, 10, 1),
            ("SELECT a <@ b", 1, 11, 1),
            ("SELECT * FROM (SELECT 1\n WHERE TRUE) q", 2, 2, 5),
            ("SELECT (a + b) YEAR TO MONTH", 1, 8, 21),
            ("SELECT MAP[1, 2,]", 1, 16, 1),
        ] {
            let error = syntax_diagnostic(sql).unwrap().expect("invalid SQL");
            assert_eq!(error.error_type, "Error parsing SQL");
            assert_eq!(
                error.span,
                Some(Span::new(
                    Location::new(line, column),
                    Location::new(line, column + width)
                )),
                "{sql}"
            );
        }
        for sql in [
            "SELECT a ~* b",
            "SELECT a !~ b",
            "SELECT a >> -b",
            "SELECT ?",
        ] {
            assert!(syntax_diagnostic(sql).unwrap().is_none(), "{sql}");
        }
    }

    #[test]
    fn derived_query_errors_keep_the_offending_token() {
        for (sql, line, col, width) in [
            ("SELECT * FROM (SELECT FROM t) q", 1, 23, 4),
            ("SELECT * FROM (SELECT x IN s FROM t) q", 1, 25, 2),
            ("SELECT * FROM (SELECT x(+) FROM t) q", 1, 25, 1),
            (
                "SELECT * FROM (WITH t AS (\nSELECT FROM src) SELECT * FROM t)",
                2,
                8,
                4,
            ),
        ] {
            assert_eq!(
                syntax_diagnostic(sql).unwrap().unwrap().span,
                Some(Span::new(
                    Location::new(line, col),
                    Location::new(line, col + width)
                )),
                "{sql}",
            );
        }
    }

    #[test]
    fn diagnostics_select_original_delimiters_inside_multiline_hints() {
        for (sql, line, col, width) in [
            ("SELECT\n /*+ */ 1", 2, 2, 3),
            ("SELECT /*+ h(NULL) */ 1", 1, 13, 1),
            ("SELECT /*+ h(abc\n , 1) */ 1", 2, 2, 1),
            ("SELECT /*+ a, */ 1", 1, 13, 1),
            ("SELECT 1 /*+ h */", 1, 10, 3),
            ("SELECT _nosuch'abc'", 1, 8, 12),
        ] {
            let diagnostic = syntax_diagnostic(sql).unwrap().expect("invalid SQL");
            assert_eq!(
                diagnostic.span,
                Some(Span::new(
                    Location::new(line, col),
                    Location::new(line, col + width)
                )),
                "{sql}"
            );
        }
    }
}
