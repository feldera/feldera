//! VALUES rows, including Calcite's scalar and parenthesized ROW forms.

use sqlparser_064::{
    ast::{Expr, Parens, Values},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser, value_keyword: bool) -> Result<Values, ParserError> {
    let mut explicit_row = false;
    let rows = parser.parse_comma_separated(|parser| {
        // Calcite treats (ROW(...)) as one row, not one ROW-valued column.
        // ROW(ROW(...)) still has one column containing a ROW expression.
        let wrapped_row = parser.peek_token().token == Token::LParen
            && matches!(parser.peek_nth_token(1).token, Token::Word(word)
                if word.keyword == Keyword::ROW && word.quote_style.is_none())
            && parser.peek_nth_token(2).token == Token::LParen;
        let wrapper = wrapped_row.then(|| parser.next_token());
        let row = parser.parse_keyword(Keyword::ROW);
        explicit_row |= row;
        if row || parser.peek_token().token == Token::LParen {
            let opening = parser.expect_token(&Token::LParen)?;
            let content = if parser
                .peek_one_of_keywords(&[
                    Keyword::SELECT,
                    Keyword::WITH,
                    Keyword::VALUES,
                    Keyword::TABLE,
                ])
                .is_some()
            {
                vec![Expr::Subquery(parser.parse_query()?)]
            } else {
                parser.parse_comma_separated(|parser| field(parser, row, wrapped_row))?
            };
            let closing = parser.expect_token(&Token::RParen)?;
            let (opening, closing) = if let Some(wrapper) = wrapper {
                (wrapper, parser.expect_token(&Token::RParen)?)
            } else {
                (opening, closing)
            };
            Ok(Parens {
                opening_token: opening.into(),
                content,
                closing_token: closing.into(),
            })
        } else {
            // Absent parentheses have empty attached tokens; the expression
            // keeps its own source spans. Do not invent source coordinates.
            Ok(Parens::with_empty_span(vec![value_expression(parser)?]))
        }
    })?;
    Ok(Values {
        explicit_row,
        rows,
        value_keyword,
    })
}

fn value_expression(parser: &mut Parser) -> Result<Expr, ParserError> {
    // A qualified wildcard is a Calcite expression. An unqualified star is
    // available only inside an explicit ROW constructor.
    if matches!(parser.peek_token_ref().token, Token::Word(_))
        && parser.peek_nth_token(1).token == Token::Period
    {
        let mut expr = parser.parse_wildcard_expr()?;
        if let Expr::QualifiedWildcard(name, _) = &expr {
            for part in &name.0 {
                if let sqlparser_064::ast::ObjectNamePart::Identifier(ident) = part {
                    if let std::ops::ControlFlow::Break(error) = super::reserved::identifier(ident)
                    {
                        return Err(error);
                    }
                }
            }
            loop {
                let precedence = parser.get_next_precedence()?;
                if precedence == 0 || parser.peek_token_ref().token == Token::Period {
                    break;
                }
                expr = parser.parse_infix(expr, precedence)?;
            }
        }
        Ok(expr)
    } else {
        parser.parse_expr()
    }
}

fn field(parser: &mut Parser, explicit_row: bool, wrapped_row: bool) -> Result<Expr, ParserError> {
    if parser.peek_keyword(Keyword::DEFAULT) {
        return Ok(Expr::DefaultValue(parser.next_token().span));
    }
    let expr = if explicit_row && !wrapped_row {
        super::collections::row_operand(parser)?
    } else {
        value_expression(parser)?
    };
    if !wrapped_row && parser.parse_keyword(Keyword::AS) {
        Ok(Expr::Named {
            expr: Box::new(expr),
            name: super::statements::identifier(parser)?,
        })
    } else {
        Ok(expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FelderaDialect;
    use sqlparser_064::ast::{SetExpr, Statement};

    fn values(sql: &str) -> Values {
        let mut statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
        let Statement::Query(query) = statements.remove(0) else {
            panic!("expected query")
        };
        let SetExpr::Values(values) = *query.body else {
            panic!("expected VALUES")
        };
        values
    }

    #[test]
    fn scalar_values_keep_row_boundaries_expressions_and_spans() {
        let values = values("VALUES 1 + 2, (3, 4), NULL, ARRAY[5, 6]");
        assert_eq!(
            values.rows.iter().map(|row| row.len()).collect::<Vec<_>>(),
            [1, 2, 1, 1]
        );
        assert!(matches!(values.rows[0][0], Expr::BinaryOp { .. }));
        assert!(matches!(values.rows[3][0], Expr::Array(_)));
        let Expr::Value(value) = &values.rows[2][0] else {
            panic!("expected NULL literal")
        };
        assert_eq!(value.span.start.column, 23);
        assert_eq!(values.rows[1].opening_token.0.span.start.column, 15);
        assert_eq!(values.rows[1].closing_token.0.span.start.column, 20);
    }

    #[test]
    fn row_wrappers_do_not_create_an_extra_column_layer() {
        for sql in ["VALUES ROW(1, 2)", "VALUES (ROW(1, 2))"] {
            let values = values(sql);
            assert!(values.explicit_row);
            assert_eq!(values.rows[0].len(), 2);
            assert!(values.rows[0]
                .iter()
                .all(|expr| matches!(expr, Expr::Value(_))));
        }
        let values = values("VALUES ROW(ROW(1, 2))");
        assert_eq!(values.rows[0].len(), 1);
        assert!(matches!(values.rows[0][0], Expr::RowConstructor { .. }));
        let values = self::values("VALUES (SELECT 1)");
        assert!(matches!(values.rows[0][0], Expr::Subquery(_)));
        // The corpus's indexed ROW queries require two ROW-valued columns.
        let values = self::values("VALUES (ROW(ROW(3, 7), ROW(4, 8)))");
        assert_eq!(values.rows[0].len(), 2);
        assert!(values.rows[0]
            .iter()
            .all(|expr| matches!(expr, Expr::RowConstructor { .. })));
    }

    #[test]
    fn malformed_rows_and_expressions_after_row_closers_are_rejected() {
        for sql in [
            "VALUES",
            "VALUES ()",
            "VALUES ROW()",
            "VALUES 1,",
            "VALUES ROW 1",
            "VALUES (1)+2",
            "VALUES (1,2)+3",
            "VALUES 1 AS x",
        ] {
            assert!(Parser::parse_sql(&FelderaDialect, sql).is_err(), "{sql}");
        }
    }
}
