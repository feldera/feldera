//! Calcite uses two tokens of lookahead to distinguish postfix predicates
//! from the implicit name of the preceding SELECT item.

use sqlparser_064::{
    ast::{Expr, MultisetPredicate},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, TokenWithSpan},
};

fn is_a(token: &Token) -> bool {
    matches!(token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("a"))
}

fn keyword(token: &TokenWithSpan) -> Keyword {
    match &token.token {
        Token::Word(word) => word.keyword,
        _ => Keyword::NoKeyword,
    }
}

pub(super) fn ends_expression(parser: &Parser) -> bool {
    let first = keyword(parser.peek_token_ref());
    let next = parser.peek_nth_token_ref(1);
    match first {
        Keyword::AT | Keyword::REGEXP => true,
        Keyword::NOT => keyword(next) == Keyword::REGEXP,
        Keyword::IS => {
            !matches!(
                keyword(next),
                Keyword::NOT
                    | Keyword::NULL
                    | Keyword::TRUE
                    | Keyword::FALSE
                    | Keyword::UNKNOWN
                    | Keyword::EMPTY
                    | Keyword::JSON
                    | Keyword::DISTINCT
            ) && !is_a(&next.token)
        }
        Keyword::LIKE | Keyword::ILIKE | Keyword::RLIKE => {
            matches!(
                next.token,
                Token::EOF | Token::SemiColon | Token::Comma | Token::RParen
            ) || matches!(keyword(next), Keyword::ANY | Keyword::FROM)
        }
        _ => false,
    }
}

pub(super) fn after_is(
    parser: &mut Parser,
    expr: Expr,
    precedence: u8,
) -> Result<Expr, ParserError> {
    let start = parser.get_current_token().span;
    let first = parser.peek_token_ref().span;
    let negated = parser.parse_keyword(Keyword::NOT);
    let next = parser.peek_token_ref();
    let predicate = if is_a(&next.token) {
        let article = parser.next_token();
        if !parser.parse_keyword(Keyword::SET) {
            return Err(super::diagnostic::located(
                "expected A SET after IS",
                article.span,
            ));
        }
        MultisetPredicate::ASet
    } else if parser.parse_keyword(Keyword::EMPTY) {
        MultisetPredicate::Empty
    } else if parser.parse_keyword(Keyword::JSON) {
        return parser.parse_is_json_predicate(expr, negated, false);
    } else {
        if parser.peek_keyword(Keyword::DISTINCT)
            && keyword(parser.peek_nth_token_ref(1)) != Keyword::FROM
        {
            if !negated {
                return Err(super::diagnostic::located(
                    "expected IS DISTINCT FROM",
                    start,
                ));
            }
            parser.next_token();
            return parser.expected_ref("FROM after IS NOT DISTINCT", parser.peek_token_ref());
        }
        if !matches!(
            keyword(parser.peek_token_ref()),
            Keyword::DISTINCT | Keyword::NULL | Keyword::TRUE | Keyword::FALSE | Keyword::UNKNOWN
        ) {
            return Err(super::diagnostic::located(
                "expected a predicate after IS",
                first,
            ));
        }
        if negated {
            parser.prev_token();
        }
        return parser.parse_is_predicate(expr, precedence);
    };
    let span = start.union(&parser.get_current_token().span);
    Ok(Expr::IsMultiset {
        expr: Box::new(expr),
        predicate,
        negated,
        span,
    })
}
