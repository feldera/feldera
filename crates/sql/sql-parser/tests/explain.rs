use feldera_sql_parser::{emit_statements, parse, parse_at_with_starts};

#[test]
fn preserves_plan_options_describe_targets_and_nested_sql() {
    for (sql, expected) in [
        (
            "EXPLAIN PLAN FOR TABLE foo.bar.baz.*",
            "EXPLAIN PLAN FOR TABLE foo.bar.baz.*",
        ),
        ("TABLE \"foo\".bar", "TABLE \"foo\".bar"),
        ("SELECT f(TABLE t.*)", "SELECT f(TABLE t.*)"),
        (
            "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH TYPE AS DOT FOR SELECT ?",
            "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES WITH TYPE AS DOT FOR SELECT ?",
        ),
        (
            "EXPLAIN PLAN EXCLUDING ATTRIBUTES WITHOUT IMPLEMENTATION AS XML FOR DELETE FROM t",
            "EXPLAIN PLAN EXCLUDING ATTRIBUTES WITHOUT IMPLEMENTATION AS XML FOR DELETE FROM t",
        ),
        (
            "EXPLAIN PLAN INCLUDING ATTRIBUTES WITH IMPLEMENTATION AS JSON FOR SELECT /*+ h */ 1",
            "EXPLAIN PLAN AS JSON FOR SELECT /*+ h */ 1",
        ),
        (
            "DESCRIBE STATEMENT UPDATE t SET i = 1",
            "EXPLAIN PLAN FOR UPDATE t SET i = 1",
        ),
        ("DESCRIBE SELECT 1", "EXPLAIN PLAN FOR SELECT 1"),
        ("DESCRIBE EXTENDED t", "DESCRIBE TABLE EXTENDED t"),
        ("DESCRIBE schema s.t.*", "DESCRIBE SCHEMA s.t.*"),
        ("DESCRIBE DATABASE \"X\"", "DESCRIBE SCHEMA \"X\""),
        ("DESCRIBE catalog t", "DESCRIBE SCHEMA t"),
        (
            "DESCRIBE \"TABLE\".x.* \"select\"",
            "DESCRIBE TABLE \"TABLE\".x.* \"select\"",
        ),
        ("DESCRIBE TABLE name", "DESCRIBE TABLE name"),
        (
            "DESCRIBE TABLE U&\"na!006de\" UESCAPE '!'",
            "DESCRIBE TABLE \"name\"",
        ),
    ] {
        let mut starts = Vec::new();
        let statements =
            parse_at_with_starts(&format!("VALUES 1;\n{sql}"), 5, |pos| starts.push(pos)).unwrap();
        let emitted = emit_statements(&statements).unwrap();
        assert_eq!(emitted[1], expected, "{sql}");
        assert_eq!(
            emit_statements(&parse(&emitted[1]).unwrap()).unwrap(),
            &emitted[1..]
        );
        assert_eq!(
            starts.iter().map(|pos| pos.line).collect::<Vec<_>>(),
            [6, 7]
        );
    }
}

#[test]
fn rejects_foreign_modifiers_invalid_targets_and_invalid_nested_sql() {
    for sql in [
        "DESC t",
        "EXPLAIN SELECT 1",
        "EXPLAIN PLAN AS TEXT FOR SELECT 1",
        "EXPLAIN PLAN INCLUDING ATTRIBUTES EXCLUDING ATTRIBUTES FOR SELECT 1",
        "EXPLAIN PLAN WITH TYPE INCLUDING ATTRIBUTES FOR SELECT 1",
        "EXPLAIN PLAN WITH TYPE AS \"JSON\" FOR SELECT 1",
        "EXPLAIN PLAN FOR CREATE TABLE t(i INT)",
        "DESCRIBE STATEMENT",
        "EXPLAIN PLAN FOR REMOVE FROM t VALUES (1)",
        "EXPLAIN PLAN FOR NULL",
        "EXPLAIN PLAN FOR SELECT * FROM qualify",
        "EXPLAIN PLAN FOR SELECT 1 WHERE TRUE",
        "EXPLAIN PLAN FOR TABLE select",
        "EXPLAIN PLAN FOR (DELETE FROM t)",
        "EXPLAIN PLAN FOR WITH c AS (SELECT 1) DELETE FROM t",
        "DESCRIBE SET x = 1",
        "DESCRIBE name",
        "DESCRIBE U&\"x\"",
        "DESCRIBE SCHEMA",
        "DESCRIBE TABLE 'x'",
        "DESCRIBE TABLE t.*.n",
        "DESCRIBE TABLE t col.another",
        "DESCRIBE TABLE t select",
    ] {
        assert!(parse(&format!("SELECT 1;\n{sql}")).is_err(), "{sql}");
    }
}

#[test]
fn describe_checks_name_length_after_sql_case_folding() {
    for (length, accepted) in [(64, true), (65, false)] {
        for name in ["İ".repeat(length), format!("\"{}\"", "🦀".repeat(length))] {
            assert_eq!(parse(&format!("DESCRIBE TABLE {name}")).is_ok(), accepted);
        }
    }
    assert!(parse("DESCRIBE TABLE \"\"").is_err());
    assert!(parse("DESCRIBE TABLE \"İİ\"").is_ok());
}
