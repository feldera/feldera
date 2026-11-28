//! Feldera SQL function declarations, isolated from PostgreSQL/Hive UDF syntax.

use sqlparser_064::{
    ast::{
        CreateFunction, CreateFunctionBody, DataType, FunctionReturnType, ObjectName,
        OperateFunctionArg, Statement,
    },
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};
use std::ops::ControlFlow;

pub(super) fn parse(parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
    let starts_with = |keywords: &[Keyword]| {
        keywords.iter().enumerate().all(|(index, keyword)| {
            matches!(&parser.peek_nth_token(index).token, Token::Word(word)
            if word.quote_style.is_none() && word.keyword == *keyword)
        })
    };
    if !starts_with(&[Keyword::CREATE, Keyword::FUNCTION])
        && !starts_with(&[
            Keyword::CREATE,
            Keyword::OR,
            Keyword::REPLACE,
            Keyword::FUNCTION,
        ])
    {
        return None;
    }
    Some(declaration(parser))
}

fn name(parser: &mut Parser) -> Result<sqlparser_064::ast::Ident, ParserError> {
    let name = parser.parse_identifier()?;
    if let ControlFlow::Break(error) = super::reserved::identifier(&name) {
        return Err(error);
    }
    Ok(name)
}

fn nullable_type(parser: &mut Parser) -> Result<DataType, ParserError> {
    let ty = parser.parse_data_type()?;
    if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
        Ok(ty)
    } else {
        let _ = parser.parse_keyword(Keyword::NULL);
        // Upstream function arguments lack a nullability field. Use its typed
        // Nullable wrapper, also used for ROW fields, rather than dropping the
        // distinction. Its Display spelling is not canonical Feldera SQL.
        Ok(DataType::Nullable(Box::new(ty)))
    }
}

fn argument(parser: &mut Parser) -> Result<OperateFunctionArg, ParserError> {
    let name = name(parser)?;
    let data_type = nullable_type(parser)?;
    let default_expr = if parser.parse_keyword(Keyword::DEFAULT) {
        Some(parser.parse_expr()?)
    } else {
        None
    };
    Ok(OperateFunctionArg {
        mode: None,
        name: Some(name),
        data_type,
        default_expr,
    })
}

fn declaration(parser: &mut Parser) -> Result<Statement, ParserError> {
    parser.expect_keyword(Keyword::CREATE)?;
    let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
    parser.expect_keyword(Keyword::FUNCTION)?;
    let if_not_exists = parser.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);
    let name = ObjectName::from(vec![name(parser)?]);
    parser.expect_token(&Token::LParen)?;
    let args = if parser.peek_token().token == Token::RParen {
        Vec::new()
    } else {
        parser.parse_comma_separated(argument)?
    };
    parser.expect_token(&Token::RParen)?;
    parser.expect_keyword(Keyword::RETURNS)?;
    let return_type = nullable_type(parser)?;
    let function_body = if parser.parse_keyword(Keyword::AS) {
        let body = parser.parse_expr()?;
        super::reject::validate_non_query(&body, Some(parser))?;
        Some(CreateFunctionBody::AsBeforeOptions {
            body,
            link_symbol: None,
        })
    } else {
        None
    };
    Ok(Statement::CreateFunction(CreateFunction {
        or_alter: false,
        or_replace,
        temporary: false,
        if_not_exists,
        name,
        args: Some(args),
        return_type: Some(FunctionReturnType::DataType(return_type)),
        function_body,
        behavior: None,
        called_on_null: None,
        parallel: None,
        security: None,
        set_params: Vec::new(),
        using: None,
        language: None,
        determinism_specifier: None,
        options: None,
        remote_connection: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FelderaDialect;

    #[test]
    fn function_declarations_preserve_types_defaults_and_expression_bodies() {
        let statements = Parser::parse_sql(&FelderaDialect,
            "CREATE OR REPLACE FUNCTION IF NOT EXISTS f(x INT NOT NULL, y VARCHAR NULL DEFAULT 'a') RETURNS INT NOT NULL AS x + CHAR_LENGTH(y)").unwrap();
        let Statement::CreateFunction(function) = &statements[0] else {
            panic!("expected function");
        };
        assert!(function.or_replace && function.if_not_exists);
        let args = function.args.as_ref().unwrap();
        assert!(!matches!(args[0].data_type, DataType::Nullable(_)));
        assert!(matches!(args[1].data_type, DataType::Nullable(_)));
        assert_eq!(args[1].default_expr.as_ref().unwrap().to_string(), "'a'");
        let Some(CreateFunctionBody::AsBeforeOptions { body, .. }) = &function.function_body else {
            panic!("expected expression body");
        };
        assert_eq!(body.to_string(), "x + CHAR_LENGTH(y)");
    }

    #[test]
    fn function_declaration_requires_feldera_parameter_and_body_syntax() {
        for sql in [
            "CREATE FUNCTION f(x INT) RETURNS INT AS x + 1",
            "CREATE FUNCTION f() RETURNS INT",
            "CREATE FUNCTION \"qualify\"(\"select\" INT) RETURNS INT AS \"select\"",
        ] {
            assert!(super::super::parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "CREATE FUNCTION f(INT) RETURNS INT",
            "CREATE FUNCTION f(x INT) AS x",
            "CREATE FUNCTION f(x INT,) RETURNS INT",
            "CREATE FUNCTION schema.f(x INT) RETURNS INT",
            "CREATE FUNCTION f(qualify INT) RETURNS INT",
            "CREATE FUNCTION f(x INT) RETURNS INT LANGUAGE SQL",
            "CREATE FUNCTION f(x INT) RETURNS INT AS (SELECT 1)",
        ] {
            assert!(!super::super::parse(sql).is_ok(), "{sql}");
        }
    }
}
