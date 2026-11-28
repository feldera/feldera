//! Calcite OrderByLimitOpt permits one LIMIT with OFFSET in either order,
//! or FETCH alone or after OFFSET. Dynamic parameter order survives formatting.
use sqlparser_064::{
    ast::{Expr, Fetch, LimitClause, Offset, Query},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse(parser: &mut Parser, query: &mut Query) -> Result<(), ParserError> {
    let leading_offset = parse_offset(parser)?;
    if parser.parse_keyword(Keyword::LIMIT) {
        let first = &parser.peek_token_ref().token;
        if !matches!(first, Token::Number(_, false))
            && !matches!(first, Token::Placeholder(p) if p == "?")
            && parser
                .peek_one_of_keywords(&[Keyword::ALL, Keyword::DECIMAL])
                .is_none()
        {
            return Err(crate::diagnostic::located(
                "expected a row count after LIMIT",
                parser.get_current_token().span,
            ));
        }
        query.limit_clause = limit(parser, leading_offset)?;
        return Ok(());
    }
    query.limit_clause = leading_offset.map(|offset| LimitClause::LimitOffset {
        limit: None,
        offset: Some(offset),
        limit_by: Vec::new(),
    });
    if parser.parse_keyword(Keyword::FETCH) {
        if parser
            .parse_one_of_keywords(&[Keyword::FIRST, Keyword::NEXT])
            .is_none()
        {
            return Err(crate::diagnostic::located(
                "expected FETCH FIRST or FETCH NEXT",
                parser.get_current_token().span,
            ));
        }
        let count = if parser.consume_token(&Token::LParen) {
            if parser.peek_keyword(Keyword::SELECT) {
                return Err(crate::diagnostic::located(
                    "expected a row count expression",
                    parser.get_current_token().span,
                ));
            }
            let expression = parser.parse_expr()?;
            crate::reject::validate_non_query(&expression, Some(parser))?;
            parser.expect_token(&Token::RParen)?;
            Expr::Nested(Box::new(expression))
        } else {
            count(parser)?
        };
        parser.expect_one_of_keywords(&[Keyword::ROW, Keyword::ROWS])?;
        parser.expect_keyword(Keyword::ONLY)?;
        query.fetch = Some(Fetch {
            quantity: Some(count),
            percent: false,
            with_ties: false,
        });
    }
    Ok(())
}

fn parse_offset(parser: &mut Parser) -> Result<Option<Offset>, ParserError> {
    if !parser.parse_keyword(Keyword::OFFSET) {
        return Ok(None);
    }
    let offset = parser.parse_offset()?;
    crate::reject::validate_non_query(&offset.value, Some(parser))?;
    Ok(Some(offset))
}

fn limit(
    parser: &mut Parser,
    leading_offset: Option<Offset>,
) -> Result<Option<LimitClause>, ParserError> {
    // Calcite uses two tokens of lookahead for LIMIT start, count. DECIMAL '…'
    // alone consumes that lookahead, committing the production to a comma.
    let typed_start = parser.peek_keyword(Keyword::DECIMAL);
    let comma_form = typed_start || parser.peek_nth_token_ref(1).token == Token::Comma;
    let start = if comma_form {
        let start = count(parser)?;
        parser.expect_token(&Token::Comma)?;
        Some(start)
    } else {
        None
    };
    let count = if parser.parse_keyword(Keyword::ALL) {
        None
    } else {
        Some(count(parser)?)
    };
    let last_token = parser.get_current_token().clone();
    let offset_first = leading_offset.is_some();
    let offset = match leading_offset {
        Some(offset) => Some(offset),
        None => parse_offset(parser)?,
    };
    Ok(match (start, count, offset) {
        (start, count, Some(offset)) => Some(LimitClause::LimitWithOffset {
            offset,
            offset_first,
            start: start.map(Box::new),
            count: count.map(Box::new),
        }),
        (Some(offset), Some(limit), None) => Some(LimitClause::OffsetCommaLimit { offset, limit }),
        (Some(offset), None, None) => Some(LimitClause::OffsetCommaAll {
            offset,
            all: last_token.into(),
        }),
        (None, count, None) => count.map(|limit| LimitClause::LimitOffset {
            limit: Some(limit),
            offset: None,
            limit_by: Vec::new(),
        }),
    })
}

fn count(parser: &mut Parser) -> Result<Expr, ParserError> {
    if parser.peek_keyword(Keyword::DECIMAL)
        && matches!(
            parser.peek_nth_token_ref(1).token,
            Token::SingleQuotedString(_)
        )
    {
        return parser.parse_prefix();
    }
    match &parser.peek_token_ref().token {
        Token::Number(_, false) => (),
        Token::Placeholder(value) if value == "?" => (),
        _ => return parser.expected_ref("unsigned numeric literal or ?", parser.peek_token_ref()),
    }
    Ok(Expr::Value(parser.parse_value()?))
}
