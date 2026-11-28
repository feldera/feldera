//! Calcite's TRANSLATE builtin has separate character-set and positional forms.

use sqlparser_064::{
    ast::{Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser) -> Result<Expr, ParserError> {
    let name = parser.parse_identifier()?;
    parser.expect_token(&Token::LParen)?;
    let first = parser.parse_expr()?;
    if parser.parse_keyword(Keyword::USING) {
        if !matches!(parser.peek_token_ref().token, Token::Word(_)) {
            return parser.expected("character set name", parser.peek_token());
        }
        let charset = super::statements::identifier(parser)?;
        parser.expect_token(&Token::RParen)?;
        return Ok(Expr::TranslateUsing {
            expr: Box::new(first),
            charset,
            span: name.span.union(&parser.get_current_token().span),
        });
    }

    let mut args = vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(first))];
    while parser.consume_token(&Token::Comma) {
        args.push(FunctionArg::Unnamed(FunctionArgExpr::Expr(
            parser.parse_expr()?,
        )));
    }
    parser.expect_token(&Token::RParen)?;
    Ok(Expr::Function(Function {
        name: name.into(),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_distinct: vec![],
        within_group: vec![],
    }))
}
