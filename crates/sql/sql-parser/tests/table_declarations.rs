use feldera_sql_parser::{emit_statements, parse};

#[test]
fn table_elements_keep_their_declaration_order() {
    let sql = "CREATE OR REPLACE TABLE IF NOT EXISTS t (PRIMARY KEY (a, b), a INT, FOREIGN KEY (b) REFERENCES r (id), b INT, label,)";
    let expected = "CREATE OR REPLACE TABLE IF NOT EXISTS t (PRIMARY KEY (a, b), a INT, FOREIGN KEY (b) REFERENCES r(id), b INT, label)";
    let parsed = parse(sql).unwrap();
    assert_eq!(emit_statements(&parsed).unwrap(), [expected]);
    assert_eq!(parsed[0].to_string(), expected);
}

#[test]
fn properties_retain_introducers_and_literal_chain_boundaries() {
    for (declaration, tail) in [
        ("CREATE TABLE t (x INT)", ""),
        ("CREATE MATERIALIZED VIEW v (x)", " AS SELECT 1"),
    ] {
        let properties = "WITH ('k' = 'a'\n'b', X'41' '42' = N'v', _UTF8'k' = U&'v')";
        let sql = format!("{declaration} {properties}{tail}");
        let formatted = emit_statements(&parse(&sql).unwrap()).unwrap();
        assert_eq!(formatted, [sql]);
    }
}

#[test]
fn table_grammar_reports_the_token_that_ends_the_declaration() {
    for (sql, unexpected) in [
        ("CREATE TABLE t(x INT) INHERITS(parent)", "INHERITS"),
        ("CREATE TABLE t(x INT NULL NOT NULL)", "NOT"),
        ("CREATE TABLE t(x INT DEFAULT 1 NOT NULL)", "NOT"),
        ("CREATE TABLE t(x INT, PRIMARY KEY(x DESC))", "DESC"),
        ("CREATE VIEW v(x COMMENT 'name') AS SELECT 1", "COMMENT"),
        ("CREATE TABLE t(x INT) WITH (k='v')", "k"),
    ] {
        let error = feldera_sql_parser::syntax_diagnostic(sql).unwrap().unwrap();
        assert_eq!(
            error.span.unwrap().start.column as usize,
            sql.find(unexpected).unwrap() + 1,
            "{sql}"
        );
    }
}
