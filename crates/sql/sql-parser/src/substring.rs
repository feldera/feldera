use sqlparser_064::{
    ast::Expr,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser) -> Result<Expr, ParserError> {
    let standard = parser.peek_keyword(Keyword::SUBSTRING);
    let name = parser.parse_identifier()?;
    if parser.peek_token_ref().token != Token::LParen {
        return Ok(Expr::Identifier(name));
    }
    if !standard {
        return parser.parse_function(name.into());
    }
    parser.expect_token(&Token::LParen)?;
    let expr = Box::new(parser.parse_expr()?);
    let comma = parser.consume_token(&Token::Comma);
    if !comma {
        parser.expect_keyword(Keyword::FROM)?;
    }
    let start = Box::new(parser.parse_expr()?);
    let length = if parser.parse_keyword(Keyword::FOR) || parser.consume_token(&Token::Comma) {
        Some(Box::new(parser.parse_expr()?))
    } else {
        None
    };
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Substring {
        expr,
        substring_from: Some(start),
        substring_for: length,
        special: comma,
        shorthand: false,
    })
}

#[cfg(test)]
mod tests {
    #[test]
    fn distinguishes_the_builtin_grammar_from_ordinary_calls() {
        for (expression, valid) in [
            ("SUBSTR", true),
            ("SUBSTRING.x", true),
            ("SUBSTR()", true),
            ("SUBSTR(s,1,2,3)", true),
            ("SUBSTR(s FROM 1)", false),
            ("SUBSTR(s,1 FOR 2)", false),
            ("SUBSTRING(s FROM 1,2)", true),
            ("SUBSTRING(s,1 FOR 2)", true),
            ("SUBSTRING(s FOR 2)", false),
            ("SUBSTRING(s)", false),
            ("SUBSTRING(s,1,2,3)", false),
            ("x.SUBSTRING(s)", true),
            ("x.SUBSTRING(s FROM 1)", false),
            ("\"substring\"(s,1,2,3)", true),
        ] {
            assert_eq!(
                crate::parse(&format!("SELECT {expression}")).is_ok(),
                valid,
                "{expression}"
            );
        }
    }
}
