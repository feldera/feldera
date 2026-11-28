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

mod explain;
pub use explain::{ExplainDepth, ExplainDetail, ExplainFormat, ExplainPlan};

#[derive(Debug)]
pub enum Statement {
    Upstream(Box<UpstreamStatement>),
    CreateType(Box<crate::CreateType>),
    ExplainPlan(ExplainPlan),
    DescribeTable {
        name: ObjectName,
        column: Option<Ident>,
    },
    DescribeSchema(ObjectName),
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
    if !matches!(parser.peek_token_ref().token, Token::Word(_)) {
        return parser.expected_ref("identifier", parser.peek_token_ref());
    }
    let name = parser.parse_identifier()?;
    if name.value.is_empty() {
        return Err(crate::diagnostic::located(
            "empty quoted identifier",
            name.span,
        ));
    }
    // Calcite enforces its identifier limit after folding unquoted names.
    let length = if name.quote_style.is_some() || name.value.is_ascii() {
        name.value.encode_utf16().count()
    } else {
        name.value.to_lowercase().encode_utf16().count()
    };
    if length > 128 {
        return Err(crate::diagnostic::at(
            "identifier exceeds 128 UTF-16 units",
            name.span.start,
        ));
    }
    if let ControlFlow::Break(error) = super::reserved::identifier(&name) {
        return Err(error);
    }
    Ok(name)
}

pub(crate) fn compound_name(parser: &mut Parser) -> Result<ObjectName, ParserError> {
    let mut parts = vec![identifier(parser)?];
    while parser.consume_token(&Token::Period) {
        if parser.peek_token_ref().token == Token::Mul {
            let star = parser.next_token();
            parts.push(Ident::with_span(star.span, "*"));
            break;
        }
        parts.push(identifier(parser)?);
    }
    Ok(ObjectName::from(parts))
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

fn parse_statement(
    parser: &mut Parser,
    unicode_names: &[Location],
) -> Result<Statement, ParserError> {
    let start = parser.peek_token().span.start;
    if parser.peek_keyword(Keyword::EXPLAIN) {
        let keyword = parser.next_token();
        if !parser.parse_keyword(Keyword::PLAN) {
            return Err(crate::diagnostic::located(
                "expected EXPLAIN PLAN",
                keyword.span,
            ));
        }
        return explain::plan(parser);
    }
    if parser.parse_keyword(Keyword::DESCRIBE) {
        return explain::describe(parser, unicode_names);
    }
    if parser.peek_keyword(Keyword::TABLE) {
        let query = parser.parse_query()?;
        return Ok(Statement::Upstream(Box::new(UpstreamStatement::Query(
            query,
        ))));
    }
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
                if keyword_at(1, Keyword::OR) {
                    parser.peek_nth_token(1).span.start
                } else {
                    start
                },
            ));
        }
        if keyword_at(offset, Keyword::TYPE) {
            return crate::CreateType::parse(parser).map(|ty| Statement::CreateType(Box::new(ty)));
        }
    }
    if parser.parse_keywords(&[Keyword::DECLARE, Keyword::RECURSIVE]) {
        parser.expect_keyword(Keyword::VIEW)?;
        let mut parts = vec![ObjectNamePart::Identifier(identifier(parser)?)];
        while parser.consume_token(&Token::Period) {
            parts.push(ObjectNamePart::Identifier(identifier(parser)?));
        }
        parser.expect_token(&Token::LParen)?;
        let mut columns = vec![view_column(parser)?];
        while parser.consume_token(&Token::Comma) {
            if parser.peek_token_ref().token == Token::RParen {
                return Err(super::diagnostic::located(
                    "expected a view column after comma",
                    parser.get_current_token().span,
                ));
            }
            columns.push(view_column(parser)?);
        }
        parser.expect_token(&Token::RParen)?;
        return Ok(Statement::DeclareRecursiveView {
            name: ObjectName(parts),
            columns,
            span: Span::new(start, parser.token_at(parser.index() - 1).span.end),
        });
    }
    if parser.peek_keyword(Keyword::DECLARE) {
        return Err(super::diagnostic::at(
            "expected DECLARE RECURSIVE VIEW",
            start,
        ));
    }
    if matches!(&parser.peek_token().token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("LATENESS"))
    {
        parser.next_token();
        let view = identifier(parser)?;
        parser.expect_token(&Token::Period)?;
        let column = identifier(parser)?;
        let expression = parser.parse_expr()?;
        super::reject::validate_non_query(&expression, Some(parser))?;
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

pub(super) fn parse_with_starts(
    mut tokens: Vec<TokenWithSpan>,
    unicode_names: &[Location],
    mut record_start: impl FnMut(Location),
) -> Result<Vec<Statement>, ParserError> {
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
        let mut statement = parse_statement(&mut parser, unicode_names)?;
        if let ControlFlow::Break(error) = statement.validate(&mut hints, &mut scoped_checks) {
            return Err(error);
        }
        record_start(scoped_checks.statement_start);
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

impl Statement {
    fn validate(
        &mut self,
        hints: &mut super::hints::Attachments,
        checks: &mut super::reject::ScopedChecks,
    ) -> ControlFlow<ParserError> {
        let mut identifiers = super::reserved::ReservedIdentifiers::default();
        let node = match self {
            Self::Upstream(node) => node.as_mut(),
            Self::ExplainPlan(plan) => plan.statement.as_mut(),
            Self::CreateType(ty) => {
                ty.visit_fields(checks)?;
                return ty.visit_fields(&mut identifiers);
            }
            Self::DeclareRecursiveView { columns, .. } => {
                return columns.iter().try_for_each(|column| {
                    column.data_type.visit(checks)?;
                    column.data_type.visit(&mut identifiers)
                });
            }
            Self::Lateness { expression, .. } => {
                VisitMut::visit(expression.as_mut(), hints)?;
                Visit::visit(expression.as_ref(), checks)?;
                return Visit::visit(expression.as_ref(), &mut identifiers);
            }
            // DESCRIBE names are checked as they are parsed.
            Self::DescribeSchema(_) | Self::DescribeTable { .. } => {
                return ControlFlow::Continue(())
            }
        };
        VisitMut::visit(node, hints)?;
        if let Err(error) = super::lateness::validate(node) {
            return ControlFlow::Break(error);
        }
        Visit::visit(node, checks)?;
        Visit::visit(node, &mut identifiers)
    }
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Upstream(statement) => match statement.as_ref() {
                UpstreamStatement::CreateTable(table) => super::ddl::table::display(table, f),
                other => other.fmt(f),
            },
            Self::CreateType(ty) => ty.fmt(f),
            Self::ExplainPlan(plan) => plan.fmt(f),
            Self::DescribeSchema(name) => write!(f, "DESCRIBE SCHEMA {name}"),
            Self::DescribeTable { name, column } => {
                write!(f, "DESCRIBE TABLE {name}")?;
                if let Some(column) = column {
                    write!(f, " {column}")?;
                }
                Ok(())
            }
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
            let nodes = crate::parse(sql).unwrap();
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
                crate::parse(sql).is_err(),
                "{sql}"
            );
        }
    }

    #[test]
    fn extensions_retain_names_nullability_expressions_and_statement_boundaries() {
        let sql = "DECLARE RECURSIVE VIEW s.\"V\"(\"select\" INT NOT NULL, y VARCHAR NULL);\nLATENESS \"V\".\"select\" (2 + 3); SELECT ';';";
        let statements = crate::parse(sql).unwrap();
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
