//! CAST interval targets, including leading and fractional-second precision.

use sqlparser_064::{
    ast::{CastKind, DataType, Expr, IntervalFields},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

fn unit(parser: &mut Parser) -> Result<IntervalFields, ParserError> {
    let token = parser.next_token();
    if let Token::Word(word) = &token.token {
        if word.quote_style.is_none() {
            let spelling = word.value.to_ascii_uppercase();
            match spelling.strip_suffix('S').unwrap_or(&spelling) {
                "YEAR" => return Ok(IntervalFields::Year),
                "QUARTER" => return Ok(IntervalFields::Quarter),
                "MONTH" => return Ok(IntervalFields::Month),
                "WEEK" => return Ok(IntervalFields::Week),
                "DAY" => return Ok(IntervalFields::Day),
                "HOUR" => return Ok(IntervalFields::Hour),
                "MINUTE" => return Ok(IntervalFields::Minute),
                "SECOND" => return Ok(IntervalFields::Second),
                _ => (),
            }
        }
    }
    Err(super::diagnostic::at(
        "expected an interval unit",
        token.span.start,
    ))
}

pub(super) fn parse_cast(parser: &mut Parser) -> Result<Expr, ParserError> {
    let kind = if parser.parse_keyword(Keyword::CAST) {
        CastKind::Cast
    } else if parser.parse_keyword(Keyword::TRY_CAST) {
        CastKind::TryCast
    } else {
        parser.expect_keyword_is(Keyword::SAFE_CAST)?;
        CastKind::SafeCast
    };
    parser.expect_token(&Token::LParen)?;
    let expr = parser.parse_expr()?;
    parser.expect_keyword_is(Keyword::AS)?;
    // Calcite's BuiltinFunctionCall has a special INTERVAL alternative here.
    // Its general DataType grammar does not admit intervals in columns, ROWs
    // or arrays. Keeping the branch at CAST avoids broadening those contexts.
    let data_type = if parser.parse_keyword(Keyword::INTERVAL) {
        parse_interval(parser)?
    } else {
        parser.parse_data_type()?
    };
    let format = parser.parse_optional_cast_format()?;
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Cast {
        kind,
        expr: Box::new(expr),
        data_type,
        format,
    })
}

fn parse_interval(parser: &mut Parser) -> Result<DataType, ParserError> {
    use IntervalFields::*;
    let first = unit(parser)?;
    let mut leading_precision = None;
    let mut fractional_precision = None;
    if parser.consume_token(&Token::LParen) {
        leading_precision = Some(super::datetime_difference::precision(parser)?);
        if first == Second && parser.consume_token(&Token::Comma) {
            fractional_precision = Some(super::datetime_difference::precision(parser)?);
        }
        parser.expect_token(&Token::RParen)?;
    }
    let to = parser.peek_token().span.start;
    let fields = if parser.parse_keyword(Keyword::TO) {
        let last = unit(parser)?;
        let fields = match (first, last) {
            (Year, Month) => YearToMonth,
            (Day, Hour) => DayToHour,
            (Day, Minute) => DayToMinute,
            (Day, Second) => DayToSecond,
            (Hour, Minute) => HourToMinute,
            (Hour, Second) => HourToSecond,
            (Minute, Second) => MinuteToSecond,
            _ => {
                return Err(super::diagnostic::at(
                    "invalid interval type unit range",
                    to,
                ));
            }
        };
        if last == Second && parser.consume_token(&Token::LParen) {
            fractional_precision = Some(super::datetime_difference::precision(parser)?);
            parser.expect_token(&Token::RParen)?;
        }
        fields
    } else {
        first
    };
    Ok(DataType::Interval {
        fields: Some(fields),
        leading_precision,
        precision: fractional_precision,
    })
}

/// Calcite accepts a named time frame as well as a built-in extraction unit.
pub(super) fn parse_extract(parser: &mut Parser) -> Result<Expr, ParserError> {
    use sqlparser_064::ast::{DateTimeField, ExtractSyntax};
    parser.expect_keyword_is(Keyword::EXTRACT)?;
    parser.expect_token(&Token::LParen)?;
    let field = match parser.maybe_parse(|p| p.parse_date_time_field())? {
        Some(field) => field,
        None if matches!(parser.peek_token_ref().token, Token::Word(_)) => {
            DateTimeField::Custom(super::statements::identifier(parser)?)
        }
        None => return parser.expected("time unit or identifier", parser.peek_token()),
    };
    parser.expect_keyword_is(Keyword::FROM)?;
    let expr = Box::new(parser.parse_expr()?);
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Extract {
        field,
        expr,
        syntax: ExtractSyntax::From,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FelderaDialect;
    use sqlparser_064::ast::{SelectItem, SetExpr, Statement};

    fn cast_type(sql: &str) -> DataType {
        let statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select")
        };
        let SelectItem::UnnamedExpr(Expr::Cast { data_type, .. }) = &select.projection[0] else {
            panic!("expected cast")
        };
        data_type.clone()
    }

    #[test]
    fn singular_and_plural_qualifiers_keep_their_typed_range() {
        use IntervalFields::*;
        for (text, fields) in [
            ("YEAR", Year),
            ("MONTH", Month),
            ("WEEK", Week),
            ("QUARTER", Quarter),
            ("DAY", Day),
            ("HOUR", Hour),
            ("MINUTE", Minute),
            ("SECOND", Second),
            ("YEAR TO MONTH", YearToMonth),
            ("DAY TO HOUR", DayToHour),
            ("DAY TO MINUTE", DayToMinute),
            ("DAY TO SECOND", DayToSecond),
            ("HOUR TO MINUTE", HourToMinute),
            ("HOUR TO SECOND", HourToSecond),
            ("MINUTE TO SECOND", MinuteToSecond),
        ] {
            let plural = text
                .split(' ')
                .map(|word| {
                    if word == "TO" {
                        word.into()
                    } else {
                        format!("{word}S")
                    }
                })
                .collect::<Vec<_>>()
                .join(" ");
            for qualifier in [text.to_owned(), plural.to_lowercase()] {
                let sql = format!("SELECT CAST('1' AS INTERVAL {qualifier})");
                assert_eq!(
                    cast_type(&sql),
                    DataType::Interval {
                        fields: Some(fields),
                        leading_precision: None,
                        precision: None
                    }
                );
            }
        }
    }

    #[test]
    fn interval_targets_do_not_leak_into_columns_or_nested_types() {
        for sql in [
            "CREATE TABLE t (v INTERVAL DAY)",
            "CREATE TABLE t (v ROW(a INTERVAL DAY))",
            "SELECT CAST(1 AS ROW(a INTERVAL DAY))",
            "SELECT CAST(1 AS INTERVAL DAY ARRAY)",
            "SELECT CAST(1 AS INTERVAL YEAR[])",
        ] {
            assert!(Parser::parse_sql(&FelderaDialect, sql).is_err(), "{sql}");
        }
        assert_eq!(
            cast_type("SELECT TRY_CAST(1 AS INTERVAL DAY)"),
            DataType::Interval {
                fields: Some(IntervalFields::Day),
                leading_precision: None,
                precision: None
            }
        );
        assert!(matches!(
            cast_type("SELECT SAFE_CAST(1 AS ROW(a INTEGER) ARRAY)"),
            DataType::Array(_)
        ));
    }

    #[test]
    fn formatting_preserves_both_interval_precisions() {
        for qualifier in [
            "YEAR(2)",
            "SECOND(2)",
            "SECOND(2, 3)",
            "DAY TO SECOND(3)",
            "DAY(4) TO SECOND(6)",
            "YEAR(2) TO MONTH",
            "WEEK(1)",
        ] {
            let sql = format!("SELECT CAST('1' AS INTERVAL {qualifier})");
            assert_eq!(cast_type(&sql).to_string(), format!("INTERVAL {qualifier}"));
        }
    }

    #[test]
    fn invalid_interval_targets_are_rejected() {
        for qualifier in [
            "",
            "YEAR(2, 3)",
            "SECOND(2, 3, 4)",
            "DAY TO MINUTE(3)",
            "YEAR(2147483648)",
            "YEAR TO DAY",
            "SECOND TO DAY",
            "DAY TO DAY",
            "\"DAY\"",
        ] {
            let sql = format!("SELECT CAST('1' AS INTERVAL {qualifier})");
            assert!(Parser::parse_sql(&FelderaDialect, &sql).is_err(), "{sql}");
        }
        // These words remain ordinary identifiers outside an interval type.
        assert!(Parser::parse_sql(&FelderaDialect, "SELECT years, days, minutes FROM t").is_ok());
    }
}
