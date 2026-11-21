//! Comparison adapter for the pinned upstream and Feldera parsers.

#[cfg(test)]
mod unparse;

use super::{Parsed, Verdict};
use feldera_sql_parser::{parse as parse_feldera, Statement};

pub(super) fn parse(sql: &str, custom: bool, include_ast: bool) -> Verdict {
    if !custom {
        return match sqlparser_stock_064::parser::Parser::parse_sql(
            &sqlparser_stock_064::dialect::GenericDialect,
            sql,
        ) {
            Ok(statements) => Verdict::Accepted(Parsed {
                views: Vec::new(),
                feldera_sql: None,
                ast: include_ast.then(|| format!("{statements:#?}")),
                statements: statements.iter().map(ToString::to_string).collect(),
            }),
            Err(error) => Verdict::Rejected {
                error: error.to_string(),
            },
        };
    }
    match parse_feldera(sql) {
        Ok(statements) => Verdict::Accepted(Parsed {
            views: views(&statements),
            feldera_sql: Some(match feldera_sql_parser::emit_statements(&statements) {
                Ok(statements) => crate::Emission::Emitted { statements },
                Err(reason) => crate::Emission::Unsupported { reason },
            }),
            ast: include_ast.then(|| format!("{statements:#?}")),
            statements: statements.iter().map(ToString::to_string).collect(),
        }),
        Err(error) => Verdict::Rejected {
            error: error.to_string(),
        },
    }
}

fn views(statements: &[Statement]) -> Vec<crate::ParsedView> {
    statements
        .iter()
        .enumerate()
        .filter_map(|(statement_index, statement)| {
            let Statement::Upstream(statement) = statement else {
                return None;
            };
            let sqlparser_064::ast::Statement::CreateView(view) = statement.as_ref() else {
                return None;
            };
            Some(crate::ParsedView {
                statement_index,
                kind: if view.local.is_some() {
                    crate::ViewKind::Local
                } else if view.materialized {
                    crate::ViewKind::Materialized
                } else {
                    crate::ViewKind::Standard
                },
                replace: view.or_replace,
            })
        })
        .collect()
}

