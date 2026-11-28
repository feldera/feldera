//! Raw-parser checks for column attributes and connector property lists.
//! These do not validate connector names or configuration values.

use sqlparser_064::{
    ast::{ColumnOption, CreateTableOptions, ObjectNamePart, Spanned, SqlOption, Statement},
    keywords::Keyword,
    parser::ParserError,
    tokenizer::{Token, TokenWithSpan},
};

pub(super) fn validate_tokens(tokens: &mut [TokenWithSpan]) -> Result<(), ParserError> {
    // Calcite's StringLiteral production trims every x, X and quote from
    // both ends, not just the literal prefix. Preserve that legacy behavior:
    // X'Ax' parses as X'A'; its odd length is rejected later by validation.
    for token in tokens.iter_mut() {
        if let Token::HexStringLiteral(value) = &mut token.token {
            *value = value.trim_matches(['x', 'X', '\'']).to_owned();
        }
    }
    let tokens: Vec<_> = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .collect();
    for token in &tokens {
        if let Token::HexStringLiteral(value) = &token.token {
            if !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err(super::diagnostic::at(
                    "invalid hexadecimal literal",
                    token.span.start,
                ));
            }
        }
    }
    // Upstream drops an empty WITH list from the AST. Refuse it before that
    // evidence is lost. Feldera requires at least one key/value pair.
    for window in tokens.windows(3) {
        if matches!(&window[0].token, Token::Word(word) if word.keyword == Keyword::WITH)
            && window[1].token == Token::LParen
            && window[2].token == Token::RParen
        {
            return Err(super::diagnostic::at(
                "empty property list",
                window[2].span.start,
            ));
        }
    }
    Ok(())
}

fn properties(options: &CreateTableOptions) -> Result<(), ParserError> {
    let CreateTableOptions::With(options) = options else {
        return Ok(());
    };
    for option in options {
        let value = match option {
            SqlOption::KeyValue { key, value } if matches!(key.0.as_slice(), [ObjectNamePart::Identifier(name)] if name.quote_style == Some('\'')) => {
                value
            }
            SqlOption::LiteralKeyValue { key, value } if super::ddl::properties::is_string(key) => {
                value
            }
            _ => {
                return Err(super::diagnostic::at(
                    "properties require string key/value pairs",
                    option.span().start,
                ))
            }
        };
        if !super::ddl::properties::is_string(value) {
            return Err(super::diagnostic::at(
                "property values require string literals",
                value.span().start,
            ));
        }
    }
    Ok(())
}

pub(super) fn validate(statement: &Statement) -> Result<(), ParserError> {
    match statement {
        Statement::CreateTable(table) => {
            properties(&table.table_options)?;
            for column in &table.columns {
                let mut default_seen = false;
                let mut primary_key_seen = false;
                for option in &column.options {
                    let seen = match option.option {
                        ColumnOption::Default(_) => &mut default_seen,
                        ColumnOption::PrimaryKey(_) => &mut primary_key_seen,
                        _ => continue,
                    };
                    if std::mem::replace(seen, true) {
                        // Column options lack their own spans in this upstream
                        // AST. Identify the column; do not invent a token span.
                        return Err(super::diagnostic::at(
                            "duplicate DEFAULT or PRIMARY KEY column attribute",
                            column.name.span.start,
                        ));
                    }
                }
            }
        }
        Statement::CreateView(view) => properties(&view.options)?,
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::parse;

    #[test]
    fn raw_parser_rejects_invalid_literals_and_column_attributes() {
        for sql in [
            "CREATE TABLE t(id BIGINT DEFAULT NULL DEFAULT 1)",
            "CREATE TABLE t(id BIGINT PRIMARY KEY PRIMARY KEY)",
            "CREATE TABLE t(id INT) WITH (a = b)",
            "CREATE TABLE t(id INT) WITH (NULL = 'NULL')",
            "CREATE TABLE t(id INT) WITH ()",
            "CREATE VIEW v WITH ('a' = 1) AS SELECT 1",
            "SELECT X'hello'",
            "SELECT X'A b'",
            "SELECT UUID NULL",
        ] {
            assert!(!parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "CREATE TABLE t(id BIGINT DEFAULT 1 PRIMARY KEY, other INT DEFAULT 2)",
            "CREATE TABLE t(id INT) WITH ('a' = 'b')",
            "CREATE VIEW v WITH ('materialized' = 'true') AS SELECT 1",
            "SELECT X''",
            "SELECT X'ABC'",
            "SELECT X'Ax'",
            "SELECT 'WITH ()'",
            "SELECT UUID '00000000-0000-0000-0000-000000000000'",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
        let Ok(parsed) = parse("SELECT X'Ax'") else {
            panic!("expected raw parser acceptance")
        };
        assert_eq!(parsed[0].to_string(), "SELECT X'A'");
    }
}
