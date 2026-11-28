//! Parse Feldera's MAP, MULTISET and ROW constructors.

use sqlparser_064::{
    ast::{Expr, Ident, Map, MapEntry},
    keywords::Keyword,
    parser::{Parser, ParserError},
    tokenizer::Token,
};

pub(super) fn parse_multiset(parser: &mut Parser) -> Result<Expr, ParserError> {
    let start = parser.next_token().span;
    if parser.consume_token(&Token::LParen) {
        let query = Box::new(parser.parse_query_primary()?);
        let end = parser.expect_token(&Token::RParen)?.span;
        return Ok(Expr::MultisetQuery {
            query,
            span: start.union(&end),
        });
    }
    parser.expect_token(&Token::LBracket)?;
    let elements = parser.parse_comma_separated(non_query_element)?;
    let end = parser.expect_token(&Token::RBracket)?.span;
    Ok(Expr::MultisetValue {
        elements,
        span: start.union(&end),
    })
}

fn non_query_element(parser: &mut Parser) -> Result<Expr, ParserError> {
    let expr = parser.parse_expr()?;
    crate::reject::validate_non_query(&expr, Some(parser))?;
    Ok(expr)
}

pub(super) fn parse_map(parser: &mut Parser) -> Result<Expr, ParserError> {
    parser.expect_keyword_is(Keyword::MAP)?;
    parser.expect_token(&Token::LBracket)?;
    let mut entries = Vec::new();
    let mut trailing_key = None;
    if !parser.consume_token(&Token::RBracket) {
        loop {
            let key = non_query_element(parser)?;
            if parser.consume_token(&Token::RBracket) {
                trailing_key = Some(Box::new(key));
                break;
            }
            map_separator(parser)?;
            let value = non_query_element(parser)?;
            entries.push(MapEntry {
                key: Box::new(key),
                value: Box::new(value),
            });
            if parser.consume_token(&Token::RBracket) {
                break;
            }
            map_separator(parser)?;
        }
    }
    Ok(Expr::Map(Map {
        entries,
        trailing_key,
    }))
}

fn map_separator(parser: &mut Parser) -> Result<(), ParserError> {
    let comma = parser.expect_token(&Token::Comma)?;
    if parser.peek_token_ref().token == Token::RBracket {
        return Err(crate::diagnostic::located(
            "expected a MAP element after comma",
            comma.span,
        ));
    }
    Ok(())
}

/// The row grammar consumes AS only when an identifier follows.
pub(super) fn field_alias(parser: &mut Parser) -> Result<Option<Ident>, ParserError> {
    if !parser.parse_keyword(Keyword::AS) {
        return Ok(None);
    }
    if !super::reserved::starts_identifier(&parser.peek_token_ref().token) {
        return Err(super::diagnostic::located(
            "expected a field name after AS",
            parser.get_current_token().span,
        ));
    }
    super::statements::identifier(parser).map(Some)
}

/// ROW field aliases require an explicit AS. Each field keeps its identifier span.
pub(super) fn parse_row(parser: &mut Parser) -> Result<Expr, ParserError> {
    use sqlparser_064::ast::ExprWithAlias;
    let start = parser.next_token().span;
    parser.expect_token(&Token::LParen)?;
    let mut first = true;
    let fields = parser.parse_comma_separated(|parser| {
        if parser.peek_keyword(Keyword::DEFAULT) {
            first = false;
            return Ok(ExprWithAlias { expr: Expr::DefaultValue(parser.next_token().span), alias: None });
        }
        let expr = if first && matches!(parser.peek_token().token, Token::Word(w) if matches!(w.keyword, Keyword::SELECT | Keyword::WITH | Keyword::VALUES | Keyword::TABLE)) {
            Expr::Subquery(parser.parse_query()?)
        } else {
            row_operand(parser)?
        };
        first = false;
        let alias = field_alias(parser)?;
        Ok(ExprWithAlias { expr, alias })
    })?;
    let end = parser.expect_token(&Token::RParen)?.span;
    Ok(Expr::RowConstructor {
        fields,
        span: start.union(&end),
    })
}

pub(super) fn row_operand(parser: &mut Parser) -> Result<Expr, ParserError> {
    use sqlparser_064::ast::Spanned;
    let mut expr = if parser.peek_token_ref().token == Token::LParen
        && parser.peek_nth_token_ref(1).token == Token::Mul
        && parser.peek_nth_token_ref(2).token == Token::RParen
    {
        parser.next_token();
        let wildcard = parser.parse_wildcard_expr()?;
        parser.next_token();
        wildcard
    } else {
        parser.parse_wildcard_expr()?
    };
    if matches!(&expr, Expr::QualifiedWildcard(_, _) | Expr::Wildcard(_))
        && parser
            .parse_one_of_keywords(&[Keyword::EXCEPT, Keyword::EXCLUDE])
            .is_some()
    {
        parser.expect_token(&Token::LParen)?;
        let columns = parser.parse_comma_separated(|parser| parser.parse_object_name(false))?;
        let end = parser.expect_token(&Token::RParen)?.span;
        let span = expr.span().union(&end);
        expr = Expr::WildcardExclusion {
            wildcard: Box::new(expr),
            columns,
            span,
        };
    }
    Ok(expr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FelderaDialect;
    use sqlparser_064::ast::{ArrayElemTypeDef, DataType, SelectItem, SetExpr, Statement, Value};

    fn projection(sql: &str) -> Expr {
        let statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
        let Statement::Query(query) = &statements[0] else {
            panic!("expected query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected select")
        };
        let SelectItem::UnnamedExpr(expr) = &select.projection[0] else {
            panic!("expected projection")
        };
        expr.clone()
    }

    #[test]
    fn map_entries_preserve_order_duplicates_nulls_and_nested_expressions() {
        let Expr::Map(map) = projection("SELECT MAP[1, 'a', 1, 'b', NULL, MAP[2 + 3, NULL]]")
        else {
            panic!("constructor lost its map shape")
        };
        assert_eq!(map.entries.len(), 3);
        assert_eq!(map.entries[0].key.to_string(), "1");
        assert_eq!(map.entries[1].key.to_string(), "1");
        assert_eq!(map.entries[0].value.to_string(), "'a'");
        assert_eq!(map.entries[1].value.to_string(), "'b'");
        assert!(matches!(map.entries[2].key.as_ref(), Expr::Value(v) if v.value == Value::Null));
        let Expr::Map(nested) = map.entries[2].value.as_ref() else {
            panic!("nested map missing")
        };
        assert_eq!(nested.entries[0].key.to_string(), "2 + 3");
        assert!(
            matches!(nested.entries[0].value.as_ref(), Expr::Value(v) if v.value == Value::Null)
        );
        assert!(matches!(projection("SELECT MAP[]"), Expr::Map(m) if m.entries.is_empty()));
    }

    #[test]
    fn map_type_preserves_nested_keys_values_and_array_layers() {
        let statements = Parser::parse_sql(
            &FelderaDialect,
            "CREATE TABLE t (m MAP<INT, MAP<VARCHAR, ROW(x INT NULL) ARRAY>> ARRAY)",
        )
        .unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected table")
        };
        let DataType::Array(ArrayElemTypeDef::Qualified(map, _)) = &table.columns[0].data_type
        else {
            panic!("outer array missing")
        };
        let DataType::Map(key, value, _) = map.as_ref() else {
            panic!("outer map missing")
        };
        assert!(matches!(
            key.as_ref(),
            DataType::Int(_) | DataType::Integer(_)
        ));
        let DataType::Map(key, value, _) = value.as_ref() else {
            panic!("nested map missing")
        };
        assert!(matches!(key.as_ref(), DataType::Varchar(_)));
        let DataType::Array(ArrayElemTypeDef::Qualified(row, _)) = value.as_ref() else {
            panic!("row array missing")
        };
        let DataType::Struct(fields, _) = row.as_ref() else {
            panic!("row missing")
        };
        assert_eq!(fields[0].field_name.as_ref().unwrap().value, "x");
        assert_eq!(fields[0].nullable, Some(true));
    }

    #[test]
    fn map_hook_preserves_access_and_other_expression_contexts() {
        assert!(matches!(
            projection("SELECT MAP[1, 2][1]"),
            Expr::CompoundFieldAccess { .. }
        ));
        assert!(matches!(
            projection("SELECT \"MAP\"[1]"),
            Expr::CompoundFieldAccess { .. }
        ));
        assert!(matches!(projection("SELECT map(1, 2)"), Expr::Function(_)));
        assert!(matches!(projection("SELECT 8 > 1"), Expr::BinaryOp { .. }));
        assert!(matches!(
            projection("SELECT ARRAY[MAP[1, 2]]"),
            Expr::Array(_)
        ));
    }

    #[test]
    fn malformed_maps_fail_without_losing_an_operand() {
        for (sql, rendered) in [
            ("SELECT MAP[1]", "MAP[1]"),
            ("SELECT MAP[1,2,3]", "MAP[1, 2, 3]"),
        ] {
            assert_eq!(projection(sql).to_string(), rendered);
        }
        for sql in [
            "SELECT MAP[1,]",
            "SELECT MAP[1,2,]",
            "SELECT MAP[,1]",
            "SELECT MAP[1:2]",
            "CREATE TABLE t (m MAP<INT>)",
            "CREATE TABLE t (m MAP<INT, INT, INT>)",
            "CREATE TABLE t (m MAP<INT, MAP<INT, INT>)",
        ] {
            assert!(Parser::parse_sql(&FelderaDialect, sql).is_err(), "{sql}");
        }
    }
}
