//! Calcite's plan inspection statements. DESCRIBE STATEMENT has the same
//! options and query/DML grammar as the default EXPLAIN PLAN.

use super::{compound_name, identifier, Statement};
use sqlparser_064::{
    ast::Statement as UpstreamStatement,
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Location, Token},
};
use std::fmt;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainDetail {
    #[default]
    Attributes,
    All,
    None,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainDepth {
    #[default]
    Physical,
    Logical,
    Type,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ExplainFormat {
    #[default]
    Text,
    Xml,
    Json,
    Dot,
}

#[derive(Debug)]
pub struct ExplainPlan {
    pub detail: ExplainDetail,
    pub depth: ExplainDepth,
    pub format: ExplainFormat,
    pub statement: Box<UpstreamStatement>,
}

impl fmt::Display for ExplainPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EXPLAIN PLAN")?;
        f.write_str(match self.detail {
            ExplainDetail::Attributes => "",
            ExplainDetail::All => " INCLUDING ALL ATTRIBUTES",
            ExplainDetail::None => " EXCLUDING ATTRIBUTES",
        })?;
        f.write_str(match self.depth {
            ExplainDepth::Physical => "",
            ExplainDepth::Logical => " WITHOUT IMPLEMENTATION",
            ExplainDepth::Type => " WITH TYPE",
        })?;
        f.write_str(match self.format {
            ExplainFormat::Text => "",
            ExplainFormat::Xml => " AS XML",
            ExplainFormat::Json => " AS JSON",
            ExplainFormat::Dot => " AS DOT",
        })?;
        write!(f, " FOR {}", self.statement)
    }
}

// ATTRIBUTES, IMPLEMENTATION and DOT have no keyword in the upstream lexer.
fn expect_word(parser: &mut Parser, value: &str) -> Result<(), ParserError> {
    let token = parser.next_token();
    if matches!(&token.token, Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(value))
    {
        Ok(())
    } else {
        parser.expected(value, token)
    }
}

pub(super) fn plan(parser: &mut Parser) -> Result<Statement, ParserError> {
    let detail = if parser.parse_keyword(Keyword::EXCLUDING) {
        expect_word(parser, "ATTRIBUTES")?;
        ExplainDetail::None
    } else if parser.parse_keyword(Keyword::INCLUDING) {
        let all = parser.parse_keyword(Keyword::ALL);
        expect_word(parser, "ATTRIBUTES")?;
        if all {
            ExplainDetail::All
        } else {
            ExplainDetail::Attributes
        }
    } else {
        ExplainDetail::Attributes
    };
    let depth = if parser.parse_keyword(Keyword::WITHOUT) {
        expect_word(parser, "IMPLEMENTATION")?;
        ExplainDepth::Logical
    } else if parser.parse_keyword(Keyword::WITH) {
        if parser.parse_keyword(Keyword::TYPE) {
            ExplainDepth::Type
        } else {
            expect_word(parser, "IMPLEMENTATION")?;
            ExplainDepth::Physical
        }
    } else {
        ExplainDepth::Physical
    };
    let format = if !parser.parse_keyword(Keyword::AS) {
        ExplainFormat::Text
    } else if parser.parse_keyword(Keyword::XML) {
        ExplainFormat::Xml
    } else if parser.parse_keyword(Keyword::JSON) {
        ExplainFormat::Json
    } else {
        expect_word(parser, "DOT")?;
        ExplainFormat::Dot
    };
    parser.expect_keyword(Keyword::FOR)?;
    explain(parser, detail, depth, format)
}

fn explain(
    parser: &mut Parser,
    detail: ExplainDetail,
    depth: ExplainDepth,
    format: ExplainFormat,
) -> Result<Statement, ParserError> {
    let statement = if parser
        .peek_one_of_keywords(&[
            Keyword::INSERT,
            Keyword::UPDATE,
            Keyword::DELETE,
            Keyword::MERGE,
        ])
        .is_some()
    {
        parser.parse_statement()?
    } else {
        UpstreamStatement::Query(parser.parse_query()?)
    };
    Ok(Statement::ExplainPlan(ExplainPlan {
        detail,
        depth,
        format,
        statement: Box::new(statement),
    }))
}

pub(super) fn describe(
    parser: &mut Parser,
    unicode_names: &[Location],
) -> Result<Statement, ParserError> {
    if parser
        .parse_one_of_keywords(&[Keyword::DATABASE, Keyword::CATALOG, Keyword::SCHEMA])
        .is_some()
    {
        return compound_name(parser).map(Statement::DescribeSchema);
    }
    let token = parser.peek_token_ref();
    // Without TABLE, Calcite's lookahead admits only lexical identifiers. Even
    // nonreserved keyword tokens require the explicit TABLE introducer.
    let implicit_table = match &token.token {
        Token::Word(word) => match word.quote_style {
            Some('"') => unicode_names.binary_search(&token.span.start).is_err(),
            None => !crate::reserved::is_keyword(&word.value),
            _ => false,
        },
        _ => false,
    };
    if parser.parse_keyword(Keyword::TABLE) || implicit_table {
        let name = compound_name(parser)?;
        let column = matches!(parser.peek_token_ref().token, Token::Word(_))
            .then(|| identifier(parser))
            .transpose()?;
        return Ok(Statement::DescribeTable { name, column });
    }
    let _ = parser.parse_keyword(Keyword::STATEMENT);
    explain(
        parser,
        ExplainDetail::Attributes,
        ExplainDepth::Physical,
        ExplainFormat::Text,
    )
}
