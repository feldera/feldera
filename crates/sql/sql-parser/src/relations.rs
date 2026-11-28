//! Relational syntax supplied through the small vendored parser hooks.

use sqlparser_064::{
    ast::{FunctionArg, FunctionArgExpr, FunctionArgOperator, Join, JoinConstraint, JoinOperator},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Span, Token},
};
use std::ops::ControlFlow;

pub(super) fn join(parser: &mut Parser) -> Option<Result<Join, ParserError>> {
    let left = parser.parse_keywords(&[Keyword::LEFT, Keyword::ASOF]);
    if !left && !parser.parse_keyword(Keyword::ASOF) {
        return None;
    }
    Some(asof(parser, left))
}

fn asof(parser: &mut Parser, left: bool) -> Result<Join, ParserError> {
    parser.expect_keyword_is(Keyword::JOIN)?;
    let relation = parser.parse_table_factor()?;
    parser.expect_keyword_is(Keyword::MATCH_CONDITION)?;
    let match_condition = parser.parse_expr()?;
    // Calcite requires ON for both ASOF forms; USING and an absent condition
    // are syntax errors even though upstream's generic ASOF accepts them.
    parser.expect_keyword_is(Keyword::ON)?;
    let constraint = JoinConstraint::On(parser.parse_expr()?);
    let join_operator = if left {
        JoinOperator::LeftAsOf {
            match_condition,
            constraint,
        }
    } else {
        JoinOperator::AsOf {
            match_condition,
            constraint,
        }
    };
    Ok(Join {
        relation,
        global: false,
        join_operator,
    })
}

pub(super) fn function_argument(parser: &mut Parser) -> Option<Result<FunctionArg, ParserError>> {
    let named = matches!(parser.peek_token().token, Token::Word(_))
        && parser.peek_nth_token(1).token == Token::RArrow;
    let offset = if named { 2 } else { 0 };
    if !matches!(parser.peek_nth_token(offset).token, Token::Word(word)
        if matches!(word.keyword, Keyword::TABLE | Keyword::DEFAULT) && word.quote_style.is_none())
    {
        return None;
    }
    Some(table_or_default_argument(parser, named))
}

fn table_or_default_argument(parser: &mut Parser, named: bool) -> Result<FunctionArg, ParserError> {
    let argument_name = if named {
        let name = parser.parse_identifier()?;
        if let ControlFlow::Break(error) = super::reserved::identifier(&name) {
            return Err(error);
        }
        parser.expect_token(&Token::RArrow)?;
        Some(name)
    } else {
        None
    };
    let token = parser.next_token();
    let arg = if matches!(token.token, Token::Word(w) if w.keyword == Keyword::DEFAULT) {
        FunctionArgExpr::Expr(sqlparser_064::ast::Expr::DefaultValue(token.span))
    } else {
        let name = super::statements::compound_name(parser)?;
        let span = Span::new(
            token.span.start,
            parser.token_at(parser.index() - 1).span.end,
        );
        FunctionArgExpr::Table { name, span }
    };
    Ok(match argument_name {
        Some(name) => FunctionArg::Named {
            name,
            arg,
            operator: FunctionArgOperator::RightArrow,
        },
        None => FunctionArg::Unnamed(arg),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser_064::ast::{Expr, FunctionArguments, SelectItem, SetExpr, Statement};

    #[test]
    fn left_asof_is_distinct_from_inner_and_keeps_both_conditions() {
        for (prefix, left) in [("LEFT ASOF", true), ("ASOF", false)] {
            let sql =
                format!("SELECT * FROM a {prefix} JOIN b MATCH_CONDITION a.t >= b.t ON a.x = b.x");
            let statements = Parser::parse_sql(&super::super::FelderaDialect, &sql).unwrap();
            let Statement::Query(query) = &statements[0] else {
                panic!("expected query")
            };
            let SetExpr::Select(select) = query.body.as_ref() else {
                panic!("expected select")
            };
            let operator = &select.from[0].joins[0].join_operator;
            assert_eq!(matches!(operator, JoinOperator::LeftAsOf { .. }), left);
            let (JoinOperator::AsOf {
                match_condition,
                constraint,
            }
            | JoinOperator::LeftAsOf {
                match_condition,
                constraint,
            }) = operator
            else {
                panic!("expected asof")
            };
            assert_eq!(match_condition.to_string(), "a.t >= b.t");
            assert!(
                matches!(constraint, JoinConstraint::On(expr) if expr.to_string() == "a.x = b.x")
            );
            assert!(super::super::parse(&sql).is_ok());
        }
    }

    #[test]
    fn table_arguments_are_relations_with_quoted_names_and_spans() {
        let statements = Parser::parse_sql(
            &super::super::FelderaDialect,
            "SELECT f(input => TABLE s.\"T\", TABLE t, 1)",
        )
        .unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select")
        };
        let SelectItem::UnnamedExpr(Expr::Function(function)) = &select.projection[0] else {
            panic!("expected function")
        };
        let FunctionArguments::List(args) = &function.args else {
            panic!("expected arguments")
        };
        let FunctionArg::Named {
            name,
            arg: FunctionArgExpr::Table { name: table, span },
            ..
        } = &args.args[0]
        else {
            panic!("expected named TABLE argument")
        };
        assert_eq!(name.value, "input");
        assert_eq!(table.to_string(), "s.\"T\"");
        assert_eq!(span.start.column, 19);
        assert_eq!(span.end.column, 30);
        assert!(matches!(
            args.args[1],
            FunctionArg::Unnamed(FunctionArgExpr::Table { .. })
        ));
        assert!(matches!(
            args.args[2],
            FunctionArg::Unnamed(FunctionArgExpr::Expr(_))
        ));
    }

    #[test]
    fn invalid_asof_and_table_argument_forms_are_rejected() {
        for sql in [
            "SELECT * FROM a LEFT ASOF JOIN b ON TRUE",
            "SELECT * FROM a LEFT ASOF JOIN b MATCH_CONDITION TRUE",
            "SELECT * FROM a ASOF JOIN b MATCH_CONDITION TRUE USING(x)",
            "SELECT * FROM a LEFT OUTER ASOF JOIN b MATCH_CONDITION TRUE ON TRUE",
            "SELECT f(TABLE)",
            "SELECT f(TABLE (SELECT 1))",
            "SELECT f(TABLE qualify)",
            "SELECT f(TABLE t + 1)",
            "SELECT TABLE t",
            "SELECT f(qualify => TABLE t)",
        ] {
            assert!(!super::super::parse(sql).is_ok(), "{sql}");
        }
    }
}
