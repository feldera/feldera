//! Interval type qualifiers that the upstream AST can represent without loss.

use sqlparser_064::{
    ast::{CastKind, DataType, Expr, IntervalFields},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

#[derive(Clone, Copy)]
enum Unit {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
}

fn unit(parser: &mut Parser) -> Result<Unit, ParserError> {
    let token = parser.next_token();
    if let Token::Word(word) = &token.token {
        if word.quote_style.is_none() {
            let unit = match word.value.to_ascii_uppercase().as_str() {
                "YEAR" | "YEARS" => Some(Unit::Year),
                "MONTH" | "MONTHS" => Some(Unit::Month),
                "DAY" | "DAYS" => Some(Unit::Day),
                "HOUR" | "HOURS" => Some(Unit::Hour),
                "MINUTE" | "MINUTES" => Some(Unit::Minute),
                "SECOND" | "SECONDS" => Some(Unit::Second),
                _ => None,
            };
            if let Some(unit) = unit {
                return Ok(unit);
            }
        }
    }
    Err(super::diagnostic::at(
        format!("unsupported interval type unit: {token}"),
        token.span.start,
    ))
}

fn reject_precision(parser: &Parser) -> Result<(), ParserError> {
    if parser.peek_token().token == Token::LParen {
        // Calcite distinguishes leading-field precision from fractional-second
        // precision. The upstream DataType holds only the latter. Refuse both
        // here until an extended type model can preserve their separate roles.
        return Err(super::diagnostic::at(
            "interval type precision requires an extended AST",
            parser.peek_token().span.start,
        ));
    }
    Ok(())
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
    use IntervalFields as Fields;
    use Unit::*;
    let location = parser.peek_token().span.start;
    let start = unit(parser)?;
    reject_precision(parser)?;
    let end = if parser.parse_keyword(Keyword::TO) {
        Some(unit(parser)?)
    } else {
        None
    };
    reject_precision(parser)?;
    let fields = match (start, end) {
        (Year, None) => Fields::Year,
        (Month, None) => Fields::Month,
        (Day, None) => Fields::Day,
        (Hour, None) => Fields::Hour,
        (Minute, None) => Fields::Minute,
        (Second, None) => Fields::Second,
        (Year, Some(Month)) => Fields::YearToMonth,
        (Day, Some(Hour)) => Fields::DayToHour,
        (Day, Some(Minute)) => Fields::DayToMinute,
        (Day, Some(Second)) => Fields::DayToSecond,
        (Hour, Some(Minute)) => Fields::HourToMinute,
        (Hour, Some(Second)) => Fields::HourToSecond,
        (Minute, Some(Second)) => Fields::MinuteToSecond,
        _ => {
            return Err(super::diagnostic::at(
                "invalid interval type unit range",
                location,
            ));
        }
    };
    Ok(DataType::Interval {
        fields: Some(fields),
        precision: None,
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
                precision: None
            }
        );
        assert!(matches!(
            cast_type("SELECT SAFE_CAST(1 AS ROW(a INTEGER) ARRAY)"),
            DataType::Array(_)
        ));
    }

    #[test]
    fn interval_precisions_and_unrepresentable_units_are_explicit_refusals() {
        for qualifier in [
            "",
            "YEAR(2)",
            "SECOND(2, 3)",
            "DAY TO SECOND(3)",
            "YEAR TO DAY",
            "SECOND TO DAY",
            "DAY TO DAY",
            "WEEK",
            "QUARTER",
            "\"DAY\"",
        ] {
            let sql = format!("SELECT CAST('1' AS INTERVAL {qualifier})");
            assert!(Parser::parse_sql(&FelderaDialect, &sql).is_err(), "{sql}");
        }
        // These words remain ordinary identifiers outside an interval type.
        assert!(Parser::parse_sql(&FelderaDialect, "SELECT years, days, minutes FROM t").is_ok());
    }
}
