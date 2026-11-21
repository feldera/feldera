//! Locations come from original tokens, never from normalized SQL text.

use sqlparser_064::{
    ast::DataType,
    keywords::Keyword,
    parser::ParserError,
    tokenizer::{Location, Span, Token, TokenWithSpan},
};

pub(super) fn at(message: impl std::fmt::Display, location: Location) -> ParserError {
    ParserError::ParserError(format!("{message}{location}"))
}

/// A syntax failure and its original token span. Some tokenizer failures
/// provide only a point, and some parser errors have no recorded location.
pub struct SyntaxDiagnostic {
    pub message: String,
    pub span: Option<Span>,
}

/// Inspect syntax independently of name resolution. Recursion-limit failures
/// propagate to the caller so they cannot be reported as invalid SQL.
pub fn syntax_diagnostic(sql: &str) -> Result<Option<SyntaxDiagnostic>, ParserError> {
    let message = match crate::parse(sql) {
        Ok(_) => return Ok(None),
        Err(ParserError::ParserError(message) | ParserError::TokenizerError(message)) => message,
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
            .and_then(|tokens| tokens.into_iter().find(|t| t.span.start == start))
            .map_or(Span::new(start, start), |t| t.span)
    });
    Ok(Some(SyntaxDiagnostic { message, span }))
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
        ] {
            let error = parse(source).unwrap_err().to_string();
            assert!(
                error.ends_with(&format!("Line: {line}, Column: {column}")),
                "{source}: {error}"
            );
        }
    }
}
