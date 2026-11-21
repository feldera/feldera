#[cfg(test)]
mod tests {
    use crate::{Emission, Verdict};

    fn emitted(sql: &str) -> Vec<String> {
        let Verdict::Accepted(parsed) = crate::parse_feldera_prototype(sql, false) else {
            panic!("input rejected")
        };
        let Some(Emission::Emitted { statements }) = parsed.feldera_sql else {
            panic!("input not emitted")
        };
        statements
    }

    #[test]
    fn row_map_and_function_nullability_are_emitted_from_ast_fields() {
        assert_eq!(
            emitted("CREATE TABLE t(r ROW(\"A\" INT NULL, b ROW(c VARCHAR)) ARRAY)"),
            ["CREATE TABLE t (r ROW(\"A\" INT NULL, b ROW(c VARCHAR)) ARRAY)"]
        );
        assert_eq!(
            emitted("SELECT MAP [1,'a',1,'b',NULL,MAP[2,'c']]"),
            ["SELECT MAP[1, 'a', 1, 'b', NULL, MAP[2, 'c']]"]
        );
        assert_eq!(emitted("CREATE FUNCTION f(x INT NOT NULL, y VARCHAR DEFAULT 'a') RETURNS INT NOT NULL AS x"),
            ["CREATE FUNCTION f(x INT NOT NULL, y VARCHAR NULL DEFAULT 'a') RETURNS INT NOT NULL AS x"]);
        assert_eq!(
            emitted("CREATE LOCAL VIEW v AS SELECT MAP[1,'a']"),
            ["CREATE LOCAL VIEW v AS SELECT MAP[1, 'a']"]
        );
    }

    #[test]
    fn quoted_names_and_values_are_escaped_without_source_recovery() {
        let original = "CREATE FUNCTION \"f\"(\"a\"\"b\" VARCHAR NULL DEFAULT 'x''y') RETURNS VARCHAR NULL AS \"a\"\"b\"";
        let sql = emitted(original);
        assert_eq!(sql, emitted(&sql[0]));
        assert!(sql[0].contains("\"a\"\"b\" VARCHAR NULL DEFAULT 'x''y'"));
    }

    #[test]
    fn decoded_quotes_are_all_reescaped_in_identifiers_and_literals() {
        for sql in [
            r#"CREATE INDEX "a""""b" ON t("c""d")"#,
            r#"SELECT 'a\b''''cd', 'ab\''cd', '''''', 'a\bcd'"#,
        ] {
            let first = emitted(sql);
            assert_eq!(first, emitted(&first[0]));
            assert_eq!(first[0], sql);
        }
    }
}
