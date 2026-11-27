//! SQL/JSON object constructors in the pinned Calcite grammar.

use sqlparser_064::{
    ast::{Expr, JsonObjectEntry},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

fn operand(parser: &mut Parser) -> Result<Expr, ParserError> {
    // Calcite starts each name and value in a non-query context. A query
    // inside a function argument or on the right of an operator is legal.
    let mut nested = false;
    for offset in 0.. {
        let token = parser.peek_nth_token_no_skip(offset);
        match token.token {
            Token::Whitespace(_) => continue,
            Token::LParen => nested = true,
            Token::Word(w)
                if nested
                    && matches!(
                        w.keyword,
                        Keyword::SELECT | Keyword::WITH | Keyword::VALUES | Keyword::TABLE
                    ) =>
            {
                return Err(super::diagnostic::at(
                    "expected a non-query expression",
                    token.span.start,
                ));
            }
            Token::Colon => {
                return Err(super::diagnostic::at(
                    "expected an expression",
                    token.span.start,
                ))
            }
            _ => break,
        }
    }
    parser.parse_expr()
}

pub(super) fn parse(parser: &mut Parser) -> Result<Expr, ParserError> {
    let start = parser.next_token().span;
    parser.expect_token(&Token::LParen)?;
    let mut entries = Vec::new();
    let mut null_clause = parser.parse_json_null_clause();
    if null_clause.is_none() && parser.peek_token_ref().token != Token::RParen {
        loop {
            let explicit_key = parser.parse_keyword(Keyword::KEY);
            // The optional first pair has two-token lookahead. An ordinary
            // function call there requires KEY or an enclosing parenthesis.
            if entries.is_empty()
                && !explicit_key
                && matches!(&parser.peek_token_ref().token,
                    Token::Word(w) if !super::grammar::dedicated_call(w))
                && parser.peek_nth_token_ref(1).token == Token::LParen
            {
                return parser.expected_ref("a JSON object member name", parser.peek_token_ref());
            }
            let key = operand(parser)?;
            if explicit_key
                || !(parser.consume_token(&Token::Colon) || parser.consume_token(&Token::Comma))
            {
                parser.expect_keyword_is(Keyword::VALUE)?;
            }
            let value = operand(parser)?;
            entries.push(JsonObjectEntry { key, value });
            if !parser.consume_token(&Token::Comma) {
                break;
            }
        }
        null_clause = parser.parse_json_null_clause();
    }
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::JsonObject {
        entries,
        null_clause,
        span: start.union(&parser.get_current_token().span),
    })
}
