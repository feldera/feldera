//! Reserved identifiers from the pinned Feldera/Calcite grammar inventory.
//! See evidence/parser-keywords.json for the FMPP/template source digests.
//! Only declaration, relation, reference and projection-alias contexts are
//! checked here; function names and type names follow their own grammar.

use sqlparser_064::{
    ast::{
        Expr, Ident, ObjectName, ObjectNamePart, Select, SelectItem, Set, Statement, TableFactor,
        Visitor,
    },
    parser::ParserError,
    tokenizer::{Token, TokenWithSpan},
};
use std::{collections::HashSet, ops::ControlFlow};

pub(crate) const RESERVED: &[&str] = &[
    "ANY",
    "ASOF",
    "CALL",
    "CASE",
    "CREATE",
    "CROSS",
    "CURRENT",
    "CURSOR",
    "DATE",
    "DATEADD",
    "DATEDIFF",
    "DATEPART",
    "DATETIME",
    "DATE_PART",
    "DEFAULT",
    "DEFINE",
    "DELETE",
    "DESCRIBE",
    "DISTINCT",
    "DROP",
    "END-EXEC",
    "EXCEPT",
    "EXPLAIN",
    "FETCH",
    "FOREIGN",
    "FRIDAY",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPING",
    "HAVING",
    "IN",
    "INDEX",
    "INNER",
    "INSERT",
    "INTERSECT",
    "INTERVAL",
    "INTO",
    "JOIN",
    "LATENESS",
    "LATERAL",
    "LEFT",
    "LIMIT",
    "MATCH_CONDITION",
    "MATCH_RECOGNIZE",
    "MATERIALIZED",
    "MEASURE",
    "MERGE",
    "MINUS",
    "MONDAY",
    "NATURAL",
    "NEW",
    "NEXT",
    "NULL",
    "OFFSET",
    "ON",
    "ORDER",
    "ORDINAL",
    "OUTER",
    "OVER",
    "PARTITION",
    "PATTERN",
    "PRIMARY",
    "QUALIFY",
    "RANGE",
    "REMOVE",
    "RIGHT",
    "ROLLUP",
    "ROW",
    "ROWS",
    "SAFE_CAST",
    "SAFE_OFFSET",
    "SAFE_ORDINAL",
    "SATURDAY",
    "SELECT",
    "SEMI",
    "SET",
    "SKIP",
    "SOME",
    "STREAM",
    "SUNDAY",
    "TABLE",
    "TABLESAMPLE",
    "THEN",
    "THURSDAY",
    "TIME",
    "TIMESTAMP",
    "TRY_CAST",
    "TUESDAY",
    "UNION",
    "UNNEST",
    "UNSIGNED",
    "UPDATE",
    "USING",
    "UUID",
    "VALUES",
    "VARIANT",
    "WEDNESDAY",
    "WHEN",
    "WHERE",
    "WINDOW",
    "WITH",
];

pub(super) fn identifier(name: &Ident) -> ControlFlow<ParserError> {
    if name.quote_style.is_none()
        && RESERVED
            .binary_search(&name.value.to_ascii_uppercase().as_str())
            .is_ok()
    {
        ControlFlow::Break(ParserError::ParserError(format!(
            "reserved word {} requires quoting at Line: {}, Column: {}",
            name.value, name.span.start.line, name.span.start.column,
        )))
    } else {
        ControlFlow::Continue(())
    }
}

/// Calcite lexes END-EXEC as one reserved token. Upstream otherwise reads it
/// as subtraction; whitespace or quoted words must retain that interpretation.
pub(super) fn validate_tokens(tokens: &[TokenWithSpan]) -> Result<(), ParserError> {
    for triple in tokens.windows(3) {
        if matches!(&triple[0].token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("END"))
            && triple[1].token == Token::Minus
            && matches!(&triple[2].token, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case("EXEC"))
            && triple[0].span.end == triple[1].span.start
            && triple[1].span.end == triple[2].span.start
        {
            return Err(ParserError::ParserError(format!(
                "reserved token END-EXEC at Line: {}, Column: {}",
                triple[0].span.start.line, triple[0].span.start.column,
            )));
        }
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct ReservedIdentifiers {
    keyword_uses: HashSet<(u64, u64)>,
}

impl ReservedIdentifiers {
    fn keyword(&mut self, name: &Ident) {
        self.keyword_uses
            .insert((name.span.start.line, name.span.start.column));
    }

    fn identifier(&self, name: &Ident) -> ControlFlow<ParserError> {
        if self
            .keyword_uses
            .contains(&(name.span.start.line, name.span.start.column))
        {
            ControlFlow::Continue(())
        } else {
            identifier(name)
        }
    }
}

impl Visitor for ReservedIdentifiers {
    type Break = ParserError;

    fn pre_visit_relation(&mut self, name: &ObjectName) -> ControlFlow<Self::Break> {
        for part in &name.0 {
            if let ObjectNamePart::Identifier(name) = part {
                self.identifier(name)?;
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
        // This dialect's upstream AST represents UNNEST as a named table call.
        if let TableFactor::Table {
            name,
            args: Some(_),
            ..
        } = table
        {
            if let [ObjectNamePart::Identifier(name)] = name.0.as_slice() {
                if name.value.eq_ignore_ascii_case("UNNEST") {
                    self.keyword(name);
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<Self::Break> {
        match statement {
            Statement::Set(Set::SingleAssignment { values, .. }) => {
                // ON is a legal SET value, but not a general SQL identifier.
                if let [Expr::Identifier(name)] = values.as_slice() {
                    if name.value.eq_ignore_ascii_case("ON") {
                        self.keyword(name);
                    }
                }
            }
            Statement::CreateTable(table) => {
                for column in &table.columns {
                    identifier(&column.name)?;
                }
            }
            Statement::CreateView(view) => {
                for column in &view.columns {
                    identifier(&column.name)?;
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expression: &Expr) -> ControlFlow<Self::Break> {
        match expression {
            Expr::Identifier(name) => self.identifier(name)?,
            Expr::CompoundIdentifier(names) => {
                for name in names {
                    identifier(name)?;
                }
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<Self::Break> {
        for item in &select.projection {
            if let SelectItem::ExprWithAlias { alias, .. } = item {
                identifier(alias)?;
            }
        }
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::parse;
    use super::RESERVED;

    #[test]
    fn reserved_words_require_quotes_in_identifier_contexts() {
        assert!(RESERVED.windows(2).all(|pair| pair[0] < pair[1]));
        for word in RESERVED {
            for sql in [
                format!("CREATE TABLE {word} (i INTEGER)"),
                format!("CREATE TABLE t ({word} INTEGER)"),
                format!("CREATE VIEW {word} AS SELECT 1 AS i"),
                format!("SELECT t.{word} FROM t"),
                format!("SELECT 1 AS {word}"),
            ] {
                assert!(!parse(&sql).is_ok(), "{sql}");
            }
            let sql = format!(
                "CREATE TABLE \"{word}\" (\"{word}\" INT); SELECT t.\"{word}\" AS \"{word}\" FROM \"{word}\" t"
            );
            assert!(parse(&sql).is_ok(), "{sql}");
        }
    }

    #[test]
    fn builtin_functions_and_types_are_not_identifier_uses() {
        for sql in [
            "SELECT DATE '2024-01-01', TRY_CAST(x AS INT), ROW(a,b) FROM t",
            "CREATE TABLE t(d DATE, v VARIANT, u UUID)",
            "SELECT DATEADD(DAY, 1, d), DATE_PART(YEAR, d) FROM t",
            "SELECT a.x FROM t CROSS JOIN UNNEST(t.a) AS a(x)",
            "SET FELDERA_AVOID_STAR_JOINS = ON; SELECT 1",
            "SELECT t.END - EXEC FROM t",
            "SELECT t.END-/* comment */EXEC FROM t",
            "SELECT * FROM (SELECT 1 AS \"SELECT\") t",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
        assert!(!parse("SELECT * FROM (SELECT 1 AS SELECT) t").is_ok());
        for sql in [
            "SELECT t.ON FROM t",
            "SELECT * FROM UNNEST",
            "SET x = ON; SELECT t.ON FROM t",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
    }
}
