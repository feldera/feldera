//! Calcite TRIM syntax without enabling unrelated comma-separated dialect forms.

use sqlparser_064::{
    ast::Expr,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser) -> Result<Expr, ParserError> {
    parser.expect_keyword_is(Keyword::TRIM)?;
    parser.expect_token(&Token::LParen)?;
    let trim_where = if [Keyword::BOTH, Keyword::LEADING, Keyword::TRAILING]
        .iter()
        .any(|keyword| parser.peek_keyword(*keyword))
    {
        Some(parser.parse_trim_where()?)
    } else {
        None
    };
    let (expr, trim_what) = if trim_where.is_some() && parser.parse_keyword(Keyword::FROM) {
        (parser.parse_expr()?, None)
    } else {
        let first = parser.parse_expr()?;
        if parser.parse_keyword(Keyword::FROM) {
            (parser.parse_expr()?, Some(Box::new(first)))
        } else {
            (first, None)
        }
    };
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Trim {
        expr: Box::new(expr),
        trim_where,
        trim_what,
        trim_characters: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FelderaDialect;
    use sqlparser_064::ast::{SelectItem, SetExpr, Statement, TrimWhereField};

    #[test]
    fn trim_preserves_direction_characters_and_source_expression() {
        for (sql, direction, characters) in [
            ("TRIM(BOTH FROM value)", Some(TrimWhereField::Both), false),
            (
                "TRIM(LEADING 'x' FROM value)",
                Some(TrimWhereField::Leading),
                true,
            ),
            (
                "TRIM(TRAILING value)",
                Some(TrimWhereField::Trailing),
                false,
            ),
            ("TRIM('x' FROM value)", None, true),
            ("TRIM(value)", None, false),
        ] {
            let statements = Parser::parse_sql(&FelderaDialect, &format!("SELECT {sql}")).unwrap();
            let Statement::Query(query) = &statements[0] else {
                panic!("expected query")
            };
            let SetExpr::Select(select) = query.body.as_ref() else {
                panic!("expected select")
            };
            let SelectItem::UnnamedExpr(Expr::Trim {
                expr,
                trim_where,
                trim_what,
                trim_characters,
            }) = &select.projection[0]
            else {
                panic!("expected trim")
            };
            assert!(matches!(expr.as_ref(), Expr::Identifier(name) if name.value == "value"));
            assert_eq!(*trim_where, direction);
            assert_eq!(trim_what.is_some(), characters);
            assert!(trim_characters.is_none());
        }
    }

    #[test]
    fn trim_rejects_other_dialects_and_incomplete_clauses() {
        for sql in [
            "TRIM(FROM value)",
            "TRIM(value, 'x')",
            "TRIM(BOTH FROM)",
            "TRIM(BOTH 'x' FROM)",
            "TRIM()",
            "TRIM(BOTH FROM value, 'x')",
        ] {
            assert!(
                Parser::parse_sql(&FelderaDialect, &format!("SELECT {sql}")).is_err(),
                "{sql}"
            );
        }
    }
}
