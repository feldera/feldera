//! The interval qualifier on (end - start) determines the SQL result type.

use sqlparser_064::{
    ast::{BinaryOperator, DateTimeField, Expr, Interval},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Span, Token},
};

fn unit(token: &Token) -> Option<DateTimeField> {
    let Token::Word(word) = token else {
        return None;
    };
    if word.quote_style.is_some() {
        return None;
    }
    match word.value.to_ascii_uppercase().as_str() {
        "YEAR" | "YEARS" => Some(DateTimeField::Year),
        "QUARTER" | "QUARTERS" => Some(DateTimeField::Quarter),
        "MONTH" | "MONTHS" => Some(DateTimeField::Month),
        "WEEK" | "WEEKS" => Some(DateTimeField::Week(None)),
        "DAY" | "DAYS" => Some(DateTimeField::Day),
        "HOUR" | "HOURS" => Some(DateTimeField::Hour),
        "MINUTE" | "MINUTES" => Some(DateTimeField::Minute),
        "SECOND" | "SECONDS" => Some(DateTimeField::Second),
        _ => None,
    }
}

pub(super) fn precision(parser: &mut Parser) -> Result<u64, ParserError> {
    let start = parser.peek_token().span.start;
    let value = parser.parse_literal_uint()?;
    if value > i32::MAX as u64 {
        Err(super::diagnostic::at(
            "interval precision exceeds INTEGER range",
            start,
        ))
    } else {
        Ok(value)
    }
}

pub(super) fn parse(
    parser: &mut Parser,
    expr: Expr,
    opening_index: usize,
) -> Result<Expr, ParserError> {
    let Some(leading) = unit(&parser.peek_token_ref().token) else {
        return Ok(expr);
    };
    // INTERVAL (expr) DAY gives the unit to the surrounding interval constructor.
    let previous = (0..opening_index)
        .rev()
        .map(|i| &parser.token_at(i).token)
        .find(|t| !matches!(t, Token::Whitespace(_)));
    if matches!(previous, Some(Token::Word(w)) if w.keyword == Keyword::INTERVAL) {
        return Ok(expr);
    }
    let start = parser.token_at(opening_index).span.start;
    parser.next_token();
    let mut leading_precision = None;
    let mut fractional_seconds_precision = None;
    if parser.consume_token(&Token::LParen) {
        leading_precision = Some(precision(parser)?);
        if leading == DateTimeField::Second && parser.consume_token(&Token::Comma) {
            fractional_seconds_precision = Some(precision(parser)?);
        }
        parser.expect_token(&Token::RParen)?;
    }
    let mut last_field = None;
    if parser.parse_keyword(Keyword::TO) {
        let token = parser.next_token();
        let last = unit(&token.token);
        use DateTimeField::{Day, Hour, Minute, Month, Second, Year};
        if !matches!(
            (&leading, &last),
            (Year, Some(Month))
                | (Day, Some(Hour | Minute | Second))
                | (Hour, Some(Minute | Second))
                | (Minute, Some(Second))
        ) {
            return Err(super::diagnostic::at(
                "invalid interval qualifier range",
                token.span.start,
            ));
        }
        if last == Some(Second) && parser.consume_token(&Token::LParen) {
            fractional_seconds_precision = Some(precision(parser)?);
            parser.expect_token(&Token::RParen)?;
        }
        last_field = last;
    }
    let span = Span::new(start, parser.token_at(parser.index() - 1).span.end);
    let mut value = &expr;
    while let Expr::Nested(inner) = value {
        value = inner;
    }
    if !matches!(
        value,
        Expr::BinaryOp {
            op: BinaryOperator::Minus,
            ..
        }
    ) {
        return Err(super::diagnostic::located(
            "interval qualifier requires a subtraction",
            span,
        ));
    }
    Ok(Expr::QualifiedDateTimeDifference {
        interval: Box::new(Interval {
            value: Box::new(expr),
            leading_field: Some(leading),
            leading_precision,
            last_field,
            fractional_seconds_precision,
        }),
        span,
    })
}
