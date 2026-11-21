//! Calcite SET accepts one literal or identifier after `=`, not session controls.

use sqlparser_064::{
    ast::{Expr, Set, Statement, UnaryOperator, Value},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
    parser
        .peek_keyword(Keyword::SET)
        .then(|| assignment(parser))
}

fn assignment(parser: &mut Parser) -> Result<Statement, ParserError> {
    parser.expect_keyword(Keyword::SET)?;
    let variable = parser.parse_object_name(false)?;
    parser.expect_token(&Token::Eq)?;
    let value_start = parser.peek_token().span.start;
    let value = parser.parse_expr()?;
    let literal_or_name = match &value {
        Expr::Value(_) | Expr::Identifier(_) | Expr::Interval(_) | Expr::TypedString(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => {
            matches!(expr.as_ref(), Expr::Value(value) if matches!(value.value, Value::Number(..)))
        }
        _ => false,
    };
    if !literal_or_name {
        return Err(super::diagnostic::at(
            "SET requires one literal or simple identifier",
            value_start,
        ));
    }
    Ok(Statement::Set(Set::SingleAssignment {
        scope: None,
        hivevar: false,
        variable,
        values: vec![value],
    }))
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn settings_do_not_admit_transaction_controls_or_expressions() {
        for sql in [
            "SET x = ON",
            "SET x = OFF",
            "SET x = true",
            "SET x = -1",
            "SET x = 'text'",
            "SET a.b = 2",
            "RESET x",
            "RESET ALL",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "SET TRANSACTION READ ONLY",
            "SET ROLE r",
            "SET TIME ZONE 'UTC'",
            "SET x TO 1",
            "SET x = 1 + 2",
            "SET x = a.b",
            "SET x = (1)",
            "SET x = 1, 2",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
    }
}
