//! Syntax-coverage measurements, not a production parser or a semantic oracle.
//!
//! Keep each upstream AST inside its adapter. The experiment records both a
//! permissive stock dialect and a minimal dialect with its own fixed identity.

mod v062;
mod v064;

#[cfg(feature = "binder-probe")]
pub mod binder_probe {
    pub use feldera_sql_binder::{
        bind_constant_program, bind_generated_projection, bind_scalar_projection,
        bind_scalar_submissions, bind_table_program, BoundProjection, Failure, Submission,
    };
}

use serde::Serialize;
use std::collections::BTreeMap;

/// Every keyword known to each pinned upstream parser, before dialect policy.
pub fn keywords() -> BTreeMap<&'static str, &'static [&'static str]> {
    BTreeMap::from([
        ("0.62.0", sqlparser_062::keywords::ALL_KEYWORDS),
        ("0.64.0", sqlparser_064::keywords::ALL_KEYWORDS),
    ])
}

/// Reserved-word policy exposed for the pinned grammar drift check.
pub fn reserved_keywords() -> &'static [&'static str] {
    feldera_sql_parser::reserved_keywords()
}

/// An explicitly incomplete Feldera dialect. Version 0.64 normalizes Unicode
/// identifiers and rejects a known interval ambiguity; neither adapter has an
/// exhaustive AST rejection pass.
#[derive(Debug)]
struct FelderaDialect;

#[derive(Debug, Serialize)]
pub struct Parsed {
    pub statements: Vec<String>,
    /// Selected typed fields for differential AST checks, independent of Display.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub views: Vec<ParsedView>,
    /// AST-derived SQL for the paired Java semantic experiment, not original text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feldera_sql: Option<Emission>,
    /// Debug ASTs are optional because the full corpus produces large trees.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ast: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum Emission {
    Emitted { statements: Vec<String> },
    Unsupported { reason: String },
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ViewKind {
    Standard,
    Local,
    Materialized,
}

#[derive(Debug, Serialize)]
pub struct ParsedView {
    pub statement_index: usize,
    pub kind: ViewKind,
    pub replace: bool,
}

#[derive(Debug, Serialize)]
#[serde(tag = "verdict", rename_all = "snake_case")]
pub enum Verdict {
    Accepted(Parsed),
    Rejected { error: String },
}

impl Verdict {
    pub fn is_accepted(&self) -> bool {
        matches!(self, Self::Accepted(_))
    }
}

#[derive(Debug, Serialize)]
pub struct Measurement {
    pub version: &'static str,
    pub dialect: &'static str,
    #[serde(flatten)]
    pub result: Verdict,
}

/// Exercise the vendored Feldera parser without invoking comparison dialects.
pub fn parse_feldera_prototype(sql: &str, include_ast: bool) -> Verdict {
    v064::parse(sql, true, include_ast)
}

/// Parse the same SQL through each version's explicitly scoped adapter.
pub fn measure(sql: &str, include_ast: bool) -> [Measurement; 4] {
    [
        Measurement {
            version: "0.62.0",
            dialect: "generic",
            result: v062::parse(sql, false, include_ast),
        },
        Measurement {
            version: "0.62.0",
            dialect: "feldera_prototype",
            result: v062::parse(sql, true, include_ast),
        },
        Measurement {
            version: "0.64.0",
            dialect: "generic",
            result: v064::parse(sql, false, include_ast),
        },
        Measurement {
            version: "0.64.0",
            dialect: "feldera_prototype",
            result: parse_feldera_prototype(sql, include_ast),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stock_comparator_does_not_receive_the_vendor_extensions() {
        assert!(sqlparser_stock_064::parser::Parser::parse_sql(
            &sqlparser_stock_064::dialect::GenericDialect,
            "VALUES 1, 2"
        )
        .is_err());
        for sql in [
            "SELECT * FROM a LEFT ASOF JOIN b MATCH_CONDITION TRUE ON TRUE",
            "SELECT f(TABLE t)",
            "CREATE TABLE t(x INT ARRAY ARRAY)",
        ] {
            let measurements = crate::measure(sql, false);
            assert!(!measurements[2].result.is_accepted(), "stock parser: {sql}");
            assert!(
                measurements[3].result.is_accepted(),
                "patched parser: {sql}"
            );
        }
    }

    #[test]
    fn parser_owns_statement_boundaries() {
        for measurement in measure("SELECT ';' AS s; SELECT 2; -- ;\n", true) {
            let Verdict::Accepted(parsed) = measurement.result else {
                panic!("valid SQL rejected: {measurement:?}");
            };
            assert_eq!(parsed.statements.len(), 2);
            assert!(parsed.ast.unwrap().contains("SingleQuotedString"));
        }
    }

    #[test]
    fn malformed_suffix_is_not_a_successful_prefix() {
        for measurement in measure("SELECT 1; SELECT (", false) {
            assert!(!measurement.result.is_accepted(), "{measurement:?}");
        }
    }

    #[test]
    fn dialect_identity_is_explicit_in_each_version() {
        let old: &dyn sqlparser_062::dialect::Dialect = &FelderaDialect;
        let new: &dyn sqlparser_064::dialect::Dialect = &feldera_sql_parser::FelderaDialect;
        assert!(!old.is::<sqlparser_062::dialect::GenericDialect>());
        assert!(!new.is::<sqlparser_064::dialect::GenericDialect>());
        // Quoting differences must be observable, not hidden by stock identity.
        let measurements = measure("SELECT `value` FROM t", false);
        assert!(measurements[0].result.is_accepted());
        assert!(!measurements[1].result.is_accepted());
        assert!(measurements[2].result.is_accepted());
        assert!(!measurements[3].result.is_accepted());
    }
}
