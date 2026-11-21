//! Feldera statements without corresponding upstream AST variants.
//!
//! Keep these typed and distinct from CREATE VIEW and column options. They are
//! syntax nodes for the probe, not the bound statement model or S1 contract.

use sqlparser_064::{
    ast::{
        DataType, Expr, Ident, ObjectName, ObjectNamePart, Statement as UpstreamStatement, Visit,
        VisitMut,
    },
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::{Location, Span, Token, TokenWithSpan},
};
use std::{fmt, ops::ControlFlow};

#[derive(Debug)]
pub enum Statement {
    Upstream(Box<UpstreamStatement>),
    CreateType(Box<crate::CreateType>),
    DeclareRecursiveView {
        name: ObjectName,
        columns: Vec<ViewColumn>,
        span: Span,
    },
    Lateness {
        view: Ident,
        column: Ident,
        expression: Box<Expr>,
        span: Span,
    },
}

#[derive(Debug)]
pub struct ViewColumn {
    pub name: Ident,
    pub data_type: DataType,
    pub nullable: bool,
}

pub(crate) fn identifier(parser: &mut Parser) -> Result<Ident, ParserError> {
    let name = parser.parse_identifier()?;
    if let ControlFlow::Break(error) = super::reserved::identifier(&name) {
        return Err(error);
    }
    Ok(name)
}

fn view_column(parser: &mut Parser) -> Result<ViewColumn, ParserError> {
    let name = identifier(parser)?;
    let data_type = parser.parse_data_type()?;
    let nullable = if parser.parse_keywords(&[Keyword::NOT, Keyword::NULL]) {
        false
    } else {
        let _ = parser.parse_keyword(Keyword::NULL);
        true
    };
    Ok(ViewColumn {
        name,
        data_type,
        nullable,
    })
}

fn parse_statement(parser: &mut Parser) -> Result<Statement, ParserError> {
    let start = parser.peek_token().span.start;
    // Upstream consumes LOCAL/GLOBAL/TRANSIENT before CREATE VIEW but drops
    // those modifiers. Intercept the complete Feldera prefix before that loss.
    if parser.peek_keyword(Keyword::CREATE) {
        let keyword_at = |offset, keyword| {
            matches!(parser.peek_nth_token(offset).token,
            Token::Word(word) if word.quote_style.is_none() && word.keyword == keyword)
        };
        let offset = if keyword_at(1, Keyword::OR) && keyword_at(2, Keyword::REPLACE) {
            3
        } else {
            1
        };
        let target = parser.peek_nth_token(offset);
        if !matches!(&target.token, Token::Word(word) if matches!(word.keyword,
            Keyword::LOCAL | Keyword::MATERIALIZED | Keyword::TABLE
                | Keyword::VIEW | Keyword::INDEX | Keyword::TYPE | Keyword::FUNCTION
                | Keyword::AGGREGATE))
        {
            return Err(super::diagnostic::at(
                "CREATE modifier or target is outside the Feldera grammar",
                if offset == 1 {
                    start
                } else {
                    parser.peek_nth_token(1).span.start
                },
            ));
        }
        if keyword_at(offset, Keyword::TYPE) {
            return crate::CreateType::parse(parser).map(|ty| Statement::CreateType(Box::new(ty)));
        }
        if keyword_at(offset, Keyword::LOCAL) {
            parser.expect_keyword(Keyword::CREATE)?;
            let or_replace = parser.parse_keywords(&[Keyword::OR, Keyword::REPLACE]);
            let local = parser.next_token().span;
            if !parser.peek_keyword(Keyword::VIEW) {
                return Err(super::diagnostic::at(
                    "LOCAL is supported only before VIEW",
                    local.start,
                ));
            }
            let mut view = parser.parse_create_view(false, or_replace, false, None)?;
            view.local = Some(local);
            return Ok(Statement::Upstream(Box::new(
                UpstreamStatement::CreateView(view),
            )));
        }
    }
    if parser.parse_keywords(&[Keyword::DECLARE, Keyword::RECURSIVE, Keyword::VIEW]) {
        let mut parts = vec![ObjectNamePart::Identifier(identifier(parser)?)];
        while parser.consume_token(&Token::Period) {
            parts.push(ObjectNamePart::Identifier(identifier(parser)?));
        }
        parser.expect_token(&Token::LParen)?;
        let columns = parser.parse_comma_separated(view_column)?;
        parser.expect_token(&Token::RParen)?;
        return Ok(Statement::DeclareRecursiveView {
            name: ObjectName(parts),
            columns,
            span: Span::new(start, parser.token_at(parser.index() - 1).span.end),
        });
    }
    if matches!(&parser.peek_token().token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("LATENESS"))
    {
        parser.next_token();
        let view = identifier(parser)?;
        parser.expect_token(&Token::Period)?;
        let column = identifier(parser)?;
        let expression = parser.parse_expr()?;
        super::reject::validate_non_query(&expression)?;
        return Ok(Statement::Lateness {
            view,
            column,
            expression: Box::new(expression),
            span: Span::new(start, parser.token_at(parser.index() - 1).span.end),
        });
    }
    parser
        .parse_statement()
        .map(|statement| Statement::Upstream(Box::new(statement)))
}

pub(super) fn parse(mut tokens: Vec<TokenWithSpan>) -> Result<Vec<Statement>, ParserError> {
    // Upstream's synthetic EOF has an empty span. Retain the original input end
    // so truncated statements report a location, including trailing whitespace.
    let end = tokens
        .last()
        .map_or(Location::new(1, 1), |token| token.span.end);
    tokens.push(TokenWithSpan {
        token: Token::EOF,
        span: Span::new(end, end),
    });
    let mut hints = super::hints::Attachments::parse(&tokens)?;
    let mut scoped_checks = super::reject::ScopedChecks::new(&tokens);
    let mut parser = Parser::new(&super::FelderaDialect).with_tokens_with_locations(tokens);
    let mut statements = Vec::new();
    loop {
        while parser.consume_token(&Token::SemiColon) {}
        if parser.peek_token().token == Token::EOF {
            hints.finish()?;
            return Ok(statements);
        }
        scoped_checks.statement_start = parser.peek_token().span.start;
        let mut statement = parse_statement(&mut parser)?;
        if let Statement::Upstream(statement) = &mut statement {
            if let ControlFlow::Break(error) = VisitMut::visit(statement.as_mut(), &mut hints) {
                return Err(error);
            }
        }
        if let Statement::Upstream(statement) = &statement {
            super::lateness::validate(statement)?;
        }
        let mut identifiers = super::reserved::ReservedIdentifiers::default();
        let validation = (|| match &statement {
            Statement::Upstream(statement) => {
                statement.visit(&mut scoped_checks)?;
                statement.visit(&mut identifiers)
            }
            Statement::CreateType(ty) => {
                ty.visit_fields(&mut scoped_checks)?;
                ty.visit_fields(&mut identifiers)
            }
            Statement::DeclareRecursiveView { columns, .. } => {
                columns.iter().try_for_each(|column| {
                    column.data_type.visit(&mut scoped_checks)?;
                    column.data_type.visit(&mut identifiers)
                })
            }
            Statement::Lateness { expression, .. } => {
                expression.visit(&mut scoped_checks)?;
                expression.visit(&mut identifiers)
            }
        })();
        if let ControlFlow::Break(error) = validation {
            return Err(error);
        }
        statements.push(statement);
        // Every top-level statement must consume its full input. In particular,
        // END is not an implicit end of this outer script, unlike a block body.
        if !matches!(parser.peek_token().token, Token::EOF | Token::SemiColon) {
            return Err(super::diagnostic::at(
                format!("expected end of statement, found: {}", parser.peek_token()),
                parser.peek_token().span.start,
            ));
        }
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upstream(statement) => statement.fmt(f),
            Self::CreateType(ty) => ty.fmt(f),
            Self::DeclareRecursiveView { name, columns, .. } => {
                write!(f, "DECLARE RECURSIVE VIEW {name} (")?;
                for (index, column) in columns.iter().enumerate() {
                    if index > 0 {
                        f.write_str(", ")?;
                    }
                    write!(f, "{} {}", column.name, column.data_type)?;
                    if !column.nullable {
                        f.write_str(" NOT NULL")?;
                    }
                }
                f.write_str(")")
            }
            Self::Lateness {
                view,
                column,
                expression,
                ..
            } => {
                write!(f, "LATENESS {view}.{column} {expression}")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_view_kind_and_modifier_span_survive_parsing_and_display() {
        for (sql, column, replace) in [
            ("CREATE LOCAL VIEW v AS SELECT 1", 8, false),
            ("CREATE OR REPLACE LOCAL VIEW v AS SELECT 1", 19, true),
        ] {
            let nodes = parse(super::super::unicode::tokens(sql).unwrap()).unwrap();
            let Statement::Upstream(node) = &nodes[0] else {
                panic!("expected view")
            };
            let UpstreamStatement::CreateView(view) = node.as_ref() else {
                panic!("expected view")
            };
            assert_eq!(view.local.unwrap().start, Location::new(1, column));
            assert_eq!(view.or_replace, replace);
            assert!(!view.materialized);
            assert_eq!(nodes[0].to_string(), sql);
        }
        for sql in [
            "CREATE GLOBAL VIEW v AS SELECT 1",
            "CREATE TRANSIENT VIEW v AS SELECT 1",
            "CREATE LOCAL MATERIALIZED VIEW v AS SELECT 1",
            "CREATE OR ALTER TABLE t(x INT)",
        ] {
            assert!(
                parse(super::super::unicode::tokens(sql).unwrap()).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn extensions_retain_names_nullability_expressions_and_statement_boundaries() {
        let sql = "DECLARE RECURSIVE VIEW s.\"V\"(\"select\" INT NOT NULL, y VARCHAR NULL);\nLATENESS \"V\".\"select\" (2 + 3); SELECT ';';";
        let statements = parse(super::super::unicode::tokens(sql).unwrap()).unwrap();
        assert_eq!(statements.len(), 3);
        let Statement::DeclareRecursiveView {
            name,
            columns,
            span,
        } = &statements[0]
        else {
            panic!("expected declaration")
        };
        assert_eq!(name.to_string(), "s.\"V\"");
        assert!(!columns[0].nullable && columns[1].nullable);
        assert_eq!(columns[0].name.quote_style, Some('"'));
        assert_eq!(span.start.line, 1);
        let Statement::Lateness {
            view,
            column,
            expression,
            span,
        } = &statements[1]
        else {
            panic!("expected lateness")
        };
        assert_eq!(view.value, "V");
        assert_eq!(column.value, "select");
        assert_eq!(expression.to_string(), "(2 + 3)");
        assert_eq!(span.start.line, 2);
    }

    #[test]
    fn extensions_reject_truncated_extra_and_out_of_context_syntax() {
        for sql in [
            "DECLARE VIEW v(x INT)",
            "DECLARE RECURSIVE VIEW v()",
            "DECLARE RECURSIVE VIEW v(x)",
            "DECLARE RECURSIVE VIEW v(x INT,)",
            "DECLARE RECURSIVE VIEW v(x INT LATENESS 1)",
            "DECLARE RECURSIVE VIEW v(x INT DEFAULT 1)",
            "DECLARE RECURSIVE VIEW v(qualify INT)",
            "DECLARE RECURSIVE VIEW v(x INT) AS SELECT 1",
            "LATENESS v.x",
            "LATENESS v.x 1 +",
            "LATENESS v.x (SELECT 1)",
            "LATENESS v.x first_value(x IGNORE NULLS)",
            "LATENESS v.x qualify",
            "LATENESS qualify.x 1",
            "LATENESS v.x 1 SELECT 1",
            "SELECT 1 END SELECT 2",
        ] {
            assert!(!super::super::parse(sql).is_ok(), "{sql}");
        }
    }
}
