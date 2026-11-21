//! Adapter kept separate so upstream API changes remain local to one version.

use super::{FelderaDialect, Parsed, Verdict};
use sqlparser_062::{
    dialect::{Dialect, GenericDialect},
    parser::Parser,
};

impl Dialect for FelderaDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        ch.is_alphabetic() || ch == '_'
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        ch.is_alphanumeric() || matches!(ch, '_' | '$')
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        ch == '"'
    }

    fn supports_unicode_string_literal(&self) -> bool {
        true
    }
    fn supports_lambda_functions(&self) -> bool {
        true
    }
    fn supports_numeric_prefix(&self) -> bool {
        true
    }
    fn supports_projection_trailing_commas(&self) -> bool {
        true
    }
    fn supports_select_wildcard_exclude(&self) -> bool {
        true
    }
    fn supports_select_wildcard_replace(&self) -> bool {
        true
    }
}

pub(super) fn parse(sql: &str, custom: bool, include_ast: bool) -> Verdict {
    let dialect: &dyn Dialect = if custom {
        &FelderaDialect
    } else {
        &GenericDialect
    };
    match Parser::parse_sql(dialect, sql) {
        Ok(statements) => Verdict::Accepted(Parsed {
            views: Vec::new(),
            feldera_sql: None,
            ast: include_ast.then(|| format!("{statements:#?}")),
            statements: statements.iter().map(ToString::to_string).collect(),
        }),
        Err(error) => Verdict::Rejected {
            error: error.to_string(),
        },
    }
}
