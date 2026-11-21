//! Column LATENESS syntax for the coverage experiment. The upstream AST stores
//! dialect options as tokens; validate the expression before retaining them.
//! This representation is not the typed S1 model.

use sqlparser_064::{
    ast::{AlterTableOperation, ColumnOption, Statement, Visit},
    parser::{Parser, ParserError},
    tokenizer::Token,
};
use std::ops::ControlFlow;

pub(super) fn column_option(
    parser: &mut Parser,
) -> Result<Option<Result<Option<ColumnOption>, ParserError>>, ParserError> {
    if !matches!(&parser.peek_token().token, Token::Word(word)
        if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("LATENESS"))
    {
        return Ok(None);
    }
    let start = parser.index();
    parser.next_token();
    let expression = parser.parse_expr()?;
    super::reject::validate_non_query(&expression)?;
    if let ControlFlow::Break(error) =
        expression.visit(&mut super::reserved::ReservedIdentifiers::default())
    {
        return Err(error);
    }
    let tokens: Vec<_> = (start..parser.index())
        .map(|index| parser.token_at(index).clone())
        .collect();
    // Expressions inside dialect options are opaque to the outer AST visitor.
    // Apply its scoped syntax checks here as well, before discarding the tree.
    if let ControlFlow::Break(error) =
        expression.visit(&mut super::reject::ScopedChecks::new(&tokens))
    {
        return Err(error);
    }
    Ok(Some(Ok(Some(ColumnOption::DialectSpecific(
        tokens
            .into_iter()
            .filter_map(|token| {
                (!matches!(token.token, Token::Whitespace(_))).then_some(token.token)
            })
            .collect(),
    )))))
}

fn is_lateness(option: &ColumnOption) -> bool {
    matches!(option, ColumnOption::DialectSpecific(tokens)
        if matches!(tokens.first(), Some(Token::Word(word))
            if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("LATENESS")))
}

pub(super) fn validate(statement: &Statement) -> Result<(), ParserError> {
    let invalid = match statement {
        Statement::CreateTable(table) => table.columns.iter().find_map(|column| {
            let mut count = 0;
            column
                .options
                .iter()
                .any(|option| {
                    if !is_lateness(&option.option) {
                        return false;
                    }
                    count += 1;
                    count > 1 || option.name.is_some()
                })
                .then_some(column.name.span.start)
        }),
        Statement::CreateView(view) => view.columns.iter().find_map(|column| {
            column
                .options
                .as_ref()
                .is_some_and(|options| {
                    let count = options
                        .as_slice()
                        .iter()
                        .filter(|option| is_lateness(option))
                        .count();
                    count > 1 || (count > 0 && column.data_type.is_none())
                })
                .then_some(column.name.span.start)
        }),
        Statement::AlterTable(table) => table.operations.iter().find_map(|operation| {
            if let AlterTableOperation::AddColumn { column_def, .. } = operation {
                column_def
                    .options
                    .iter()
                    .any(|option| is_lateness(&option.option))
                    .then_some(column_def.name.span.start)
            } else {
                None
            }
        }),
        _ => None,
    };
    if let Some(column_start) = invalid {
        // Upstream drops option spans. Locate the invalid column declaration;
        // this does not claim to identify the offending attribute's token.
        Err(super::diagnostic::at(
            "LATENESS must be a single unnamed CREATE column attribute",
            column_start,
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::{parse, FelderaDialect};
    use sqlparser_064::{
        ast::{ColumnOption, Statement},
        parser::Parser,
    };

    #[test]
    fn lateness_retains_the_expression_and_adjacent_attributes() {
        let sql = "CREATE TABLE t (ts TIMESTAMP LATENESS INTERVAL '1' HOUR DEFAULT TIMESTAMP '2024-01-01 00:00:00', n INT NOT NULL LATENESS (2 + 3) PRIMARY KEY)";
        let statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
        let Statement::CreateTable(table) = &statements[0] else {
            panic!("expected table");
        };
        let ColumnOption::DialectSpecific(tokens) = &table.columns[0].options[0].option else {
            panic!("expected LATENESS");
        };
        assert_eq!(
            tokens.iter().map(ToString::to_string).collect::<Vec<_>>(),
            ["LATENESS", "INTERVAL", "'1'", "HOUR"]
        );
        assert!(matches!(
            table.columns[0].options[1].option,
            ColumnOption::Default(_)
        ));
        assert_eq!(table.columns[1].options.len(), 3);
        assert!(parse(sql).is_ok());
    }

    #[test]
    fn lateness_does_not_hide_invalid_expressions_or_reserved_names() {
        for sql in [
            "CREATE TABLE t (n INT LATENESS)",
            "CREATE TABLE t (n INT LATENESS 1 +)",
            "CREATE TABLE t (n INT LATENESS ARRAY[1,])",
            "CREATE TABLE t (n INT LATENESS qualify)",
            "CREATE TABLE t (n INT LATENESS first_value(n IGNORE NULLS))",
            "CREATE TABLE t (n INT LATENESS (SELECT 1))",
            "CREATE TABLE t (n INT LATENESS 1 LATENESS 2)",
            "CREATE TABLE t (n INT CONSTRAINT c LATENESS 1)",
            "ALTER TABLE t ADD COLUMN n INT LATENESS 1",
            "CREATE VIEW v(n LATENESS 1) AS SELECT 1",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "CREATE TABLE t (n INT LATENESS 0)",
            "CREATE TABLE t (n INT LATENESS -1)",
            "CREATE TABLE t (n INT LATENESS CASE WHEN TRUE THEN 1 ELSE 2 END)",
            "CREATE TABLE t (n INT LATENESS \"qualify\")",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
    }
}
