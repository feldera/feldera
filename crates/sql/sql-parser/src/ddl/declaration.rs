//! Parse the Feldera CREATE prefixes before sqlparser can discard a view modifier.

use crate::statements::compound_name;
use sqlparser_064::{
    ast::{CreateView, Statement, ViewColumnDef},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Span, Token},
};

pub(crate) fn parse(parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
    if !parser.peek_keyword(Keyword::CREATE) {
        return None;
    }
    let keyword = |i| match &parser.peek_nth_token_ref(i).token {
        Token::Word(word) => word.keyword,
        _ => Keyword::NoKeyword,
    };
    let kind = if keyword(1) == Keyword::OR && keyword(2) == Keyword::REPLACE {
        keyword(3)
    } else {
        keyword(1)
    };
    if !matches!(
        kind,
        Keyword::TABLE | Keyword::VIEW | Keyword::LOCAL | Keyword::MATERIALIZED
    ) {
        return None;
    }
    parser.next_token();
    let replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    let modifier = parser.next_token().span;
    Some(match kind {
        Keyword::TABLE => super::table::declaration(parser, replace),
        Keyword::VIEW => view(parser, replace, None, false),
        _ if parser.parse_keyword(Keyword::VIEW) => view(
            parser,
            replace,
            (kind == Keyword::LOCAL).then_some(modifier),
            kind == Keyword::MATERIALIZED,
        ),
        _ => Err(crate::diagnostic::at(
            "expected VIEW after CREATE modifier",
            modifier.start,
        )),
    })
}

fn view(
    parser: &mut Parser,
    or_replace: bool,
    local: Option<Span>,
    materialized: bool,
) -> Result<Statement, ParserError> {
    let name = compound_name(parser)?;
    let columns = if parser.peek_token_ref().token == Token::LParen {
        super::table::names(parser)?
            .into_iter()
            .map(|name| ViewColumnDef {
                name,
                data_type: None,
                options: None,
            })
            .collect()
    } else {
        Vec::new()
    };
    let options = super::properties::parse(parser)?;
    parser.expect_keyword(Keyword::AS)?;
    let query = parser.parse_query()?;
    Ok(Statement::CreateView(CreateView {
        name,
        or_replace,
        local,
        materialized,
        columns,
        query,
        options,
        or_alter: false,
        secure: false,
        name_before_not_exists: false,
        cluster_by: Vec::new(),
        comment: None,
        with_no_schema_binding: false,
        if_not_exists: false,
        temporary: false,
        copy_grants: false,
        to: None,
        params: None,
    }))
}
