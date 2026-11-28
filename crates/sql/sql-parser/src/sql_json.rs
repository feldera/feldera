//! SQL/JSON constructors and path functions in Feldera's Calcite grammar.

use sqlparser_064::{
    ast::{
        Expr, ExprWithAlias, Function, FunctionArg, FunctionArgExpr,
        FunctionArgumentClause as Clause, FunctionArgumentList, FunctionArguments, JsonObjectEntry,
        JsonResponse, JsonReturningClause, JsonWrapper,
    },
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Token, Word},
};

fn operand(parser: &mut Parser) -> Result<Expr, ParserError> {
    let start = parser.index();
    let expr = parser.parse_expr()?;
    super::reject::validate_non_query(&expr, Some(parser))?;
    if matches!(expr, Expr::Lambda(_)) {
        // A lambda may be an argument of a nested routine call, but SQL/JSON
        // consumes ordinary value expressions at this level.
        if let Some(arrow) = (start..parser.index())
            .map(|i| parser.token_at(i))
            .find(|t| t.token == Token::Arrow)
        {
            return parser.expected_ref("a JSON operand", arrow);
        }
    }
    Ok(expr)
}

/// Calcite looks one token beyond KEY to distinguish the optional keyword
/// from a member name. Infix punctuation continues the name expression.
fn starts_key_operand(token: &Token) -> bool {
    match token {
        Token::Word(w) => {
            super::reserved::starts_identifier(token)
                || super::grammar::dedicated_call(w)
                || matches!(
                    w.keyword,
                    Keyword::NULL
                        | Keyword::INTERVAL
                        | Keyword::DATE
                        | Keyword::CASE
                        | Keyword::NEW
                        | Keyword::CURSOR
                        | Keyword::NEXT
                        | Keyword::CURRENT
                        | Keyword::NOT
                        | Keyword::EXISTS
                        | Keyword::UNIQUE
                )
        }
        Token::LParen
        | Token::LBrace
        | Token::Plus
        | Token::Minus
        | Token::Number(..)
        | Token::Placeholder(_)
        | Token::SingleQuotedString(_)
        | Token::NationalStringLiteral(_)
        | Token::EscapedStringLiteral(_)
        | Token::UnicodeStringLiteral(_)
        | Token::HexStringLiteral(_) => true,
        _ => false,
    }
}

fn pair(parser: &mut Parser, first_optional: bool) -> Result<JsonObjectEntry, ParserError> {
    // KEY is non-reserved. For example, KEY.x and KEY, value both use it
    // as a name. KEY (expr) VALUE value introduces an explicit key.
    let key_as_name = parser.peek_keyword(Keyword::KEY)
        && !starts_key_operand(&parser.peek_nth_token_ref(1).token);
    let explicit_key = !key_as_name && parser.parse_keyword(Keyword::KEY);
    // The first JSON_OBJECT member must pass Calcite's optional-member
    // lookahead. Later members and the required JSON_OBJECTAGG member do not.
    if first_optional
        && !explicit_key
        && (key_as_name
            || (matches!(&parser.peek_token_ref().token, Token::Word(w) if !super::grammar::dedicated_call(w))
                && parser.peek_nth_token_ref(1).token == Token::LParen))
    {
        return parser.expected_ref("a JSON object member name", parser.peek_token_ref());
    }
    let key = operand(parser)?;
    if explicit_key || !(parser.consume_token(&Token::Colon) || parser.consume_token(&Token::Comma))
    {
        parser.expect_keyword_is(Keyword::VALUE)?;
    }
    Ok(JsonObjectEntry {
        key,
        value: operand(parser)?,
    })
}

fn object(parser: &mut Parser, aggregate: bool) -> Result<Expr, ParserError> {
    let start = parser.next_token().span;
    parser.expect_token(&Token::LParen)?;
    if aggregate {
        let entry = Box::new(pair(parser, false)?);
        let null_clause = parser.parse_json_null_clause();
        let end = parser.expect_token(&Token::RParen)?;
        return Ok(Expr::JsonObjectAgg {
            entry,
            null_clause,
            span: start.union(&end.span),
        });
    }
    let mut entries = Vec::new();
    let mut null_clause = parser.parse_json_null_clause();
    if null_clause.is_none() && parser.peek_token_ref().token != Token::RParen {
        entries.push(pair(parser, true)?);
        while parser.consume_token(&Token::Comma) {
            entries.push(pair(parser, false)?);
        }
        null_clause = parser.parse_json_null_clause();
    }
    let end = parser.expect_token(&Token::RParen)?;
    Ok(Expr::JsonObject {
        entries,
        null_clause,
        span: start.union(&end.span),
    })
}

fn passing_binding(parser: &mut Parser) -> Result<ExprWithAlias, ParserError> {
    let expr = operand(parser)?;
    parser.expect_keyword(Keyword::AS)?;
    let name = super::statements::identifier(parser)?;
    Ok(ExprWithAlias {
        expr,
        alias: Some(name),
    })
}

fn parse_word(parser: &mut Parser, expected: &str) -> bool {
    let matches = matches!(&parser.peek_token_ref().token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected));
    if matches {
        parser.next_token();
    }
    matches
}

fn wrapper(parser: &mut Parser) -> Result<Option<JsonWrapper>, ParserError> {
    let wrapper = if parser.parse_keyword(Keyword::WITHOUT) {
        JsonWrapper::Without
    } else if parser.parse_keyword(Keyword::WITH) {
        if parse_word(parser, "CONDITIONAL") {
            JsonWrapper::Conditional
        } else {
            parse_word(parser, "UNCONDITIONAL");
            JsonWrapper::Unconditional
        }
    } else {
        return Ok(None);
    };
    let _ = parser.parse_keyword(Keyword::ARRAY);
    parser.expect_keyword(Keyword::WRAPPER)?;
    Ok(Some(wrapper))
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum JsonFunction {
    Array,
    Object,
    ArrayAgg,
    ObjectAgg,
    Exists,
    Query,
    Value,
}

impl JsonFunction {
    fn named(word: &Word) -> Option<Self> {
        if word.quote_style.is_some() || !word.value.get(..5)?.eq_ignore_ascii_case("JSON_") {
            return None;
        }
        Some(match word.value[5..].to_ascii_uppercase().as_str() {
            "ARRAY" => Self::Array,
            "OBJECT" => Self::Object,
            "ARRAYAGG" => Self::ArrayAgg,
            "OBJECTAGG" => Self::ObjectAgg,
            "EXISTS" => Self::Exists,
            "QUERY" => Self::Query,
            "VALUE" => Self::Value,
            _ => return None,
        })
    }

    pub(super) fn peek(parser: &Parser) -> Option<Self> {
        let Token::Word(word) = &parser.peek_token_ref().token else {
            return None;
        };
        let function = Self::named(word)?;
        (parser.get_current_token().token != Token::Period
            && parser.peek_nth_token_ref(1).token == Token::LParen)
            .then_some(function)
    }

    fn path_clauses(
        self,
        parser: &mut Parser,
        clauses: &mut Vec<Clause>,
    ) -> Result<(), ParserError> {
        if parser.parse_keyword(Keyword::PASSING) {
            clauses.push(Clause::JsonPassing(
                parser.parse_comma_separated(passing_binding)?,
            ));
        }
        if self == Self::Exists {
            if let Some(action) = parser.parse_one_of_keywords(&[
                Keyword::TRUE,
                Keyword::FALSE,
                Keyword::UNKNOWN,
                Keyword::ERROR,
            ]) {
                let action = match action {
                    Keyword::TRUE => JsonResponse::Boolean(true),
                    Keyword::FALSE => JsonResponse::Boolean(false),
                    Keyword::UNKNOWN => JsonResponse::Unknown,
                    _ => JsonResponse::Error,
                };
                parser.expect_keyword(Keyword::ON)?;
                parser.expect_keyword(Keyword::ERROR)?;
                clauses.push(Clause::JsonOnError(action));
            }
            return Ok(());
        }
        if parser.parse_keyword(Keyword::RETURNING) {
            clauses.push(Clause::JsonReturningClause(JsonReturningClause {
                data_type: parser.parse_data_type()?,
            }));
        }
        if self == Self::Query {
            if let Some(wrapper) = wrapper(parser)? {
                clauses.push(Clause::JsonWrapper(wrapper));
            }
        }
        loop {
            let response = if parser.parse_keyword(Keyword::ERROR) {
                JsonResponse::Error
            } else if parser.parse_keyword(Keyword::NULL) {
                JsonResponse::Null
            } else if self == Self::Value && parser.parse_keyword(Keyword::DEFAULT) {
                JsonResponse::Default(Box::new(operand(parser)?))
            } else if self == Self::Query && parser.parse_keyword(Keyword::EMPTY) {
                match parser.expect_one_of_keywords(&[Keyword::ARRAY, Keyword::OBJECT])? {
                    Keyword::ARRAY => JsonResponse::EmptyArray,
                    _ => JsonResponse::EmptyObject,
                }
            } else {
                break;
            };
            parser.expect_keyword(Keyword::ON)?;
            clauses.push(
                match parser.expect_one_of_keywords(&[Keyword::EMPTY, Keyword::ERROR])? {
                    Keyword::EMPTY => Clause::JsonOnEmpty(response),
                    _ => Clause::JsonOnError(response),
                },
            );
        }
        Ok(())
    }

    pub(super) fn parse(self, parser: &mut Parser) -> Result<Expr, ParserError> {
        if matches!(self, Self::Object | Self::ObjectAgg) {
            return object(parser, self == Self::ObjectAgg);
        }
        let name = parser.parse_identifier()?;
        parser.expect_token(&Token::LParen)?;
        let mut clauses = Vec::new();
        let operands = match self {
            Self::Array if parser.peek_token_ref().token == Token::RParen => Vec::new(),
            Self::Array => parser.parse_comma_separated(operand)?,
            Self::ArrayAgg => {
                let value = operand(parser)?;
                if parser.parse_keywords(&[Keyword::ORDER, Keyword::BY]) {
                    clauses.push(Clause::OrderBy(
                        parser.parse_comma_separated(Parser::parse_order_by_expr)?,
                    ));
                }
                vec![value]
            }
            Self::Exists | Self::Query | Self::Value => {
                let document = operand(parser)?;
                parser.expect_token(&Token::Comma)?;
                let path = operand(parser)?;
                self.path_clauses(parser, &mut clauses)?;
                vec![document, path]
            }
            Self::Object | Self::ObjectAgg => {
                unreachable!("object constructors consume key/value pairs")
            }
        };
        let inline_order = self == Self::ArrayAgg && !clauses.is_empty();
        if matches!(self, Self::Array | Self::ArrayAgg) {
            if let Some(clause) = parser.parse_json_null_clause() {
                clauses.push(Clause::JsonNullClause(clause));
            }
        }
        parser.expect_token(&Token::RParen)?;
        let mut within_group = Vec::new();
        if self == Self::ArrayAgg && parser.parse_keywords(&[Keyword::WITHIN, Keyword::GROUP]) {
            parser.expect_token(&Token::LParen)?;
            parser.expect_keyword(Keyword::ORDER)?;
            parser.expect_keyword(Keyword::BY)?;
            within_group = parser.parse_comma_separated(Parser::parse_order_by_expr)?;
            let end = parser.expect_token(&Token::RParen)?;
            if inline_order {
                return Err(super::diagnostic::located(
                    "JSON_ARRAYAGG has two ORDER BY lists",
                    name.span.union(&end.span),
                ));
            }
        }
        Ok(Expr::Function(Function {
            name: name.into(),
            args: FunctionArguments::List(FunctionArgumentList {
                duplicate_treatment: None,
                args: operands
                    .into_iter()
                    .map(|expr| FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)))
                    .collect(),
                clauses,
            }),
            parameters: FunctionArguments::None,
            uses_odbc_syntax: false,
            null_treatment: None,
            over: None,
            filter: None,
            within_distinct: Vec::new(),
            within_group,
        }))
    }
}
