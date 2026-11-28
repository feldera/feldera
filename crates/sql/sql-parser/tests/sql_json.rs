use feldera_sql_parser::{emit_statements, parse};

#[test]
fn formats_path_clauses_with_their_operands_and_policies() {
    for (source, expected) in [
        ("JSON_EXISTS(d, p PASSING a + 1 AS x, f((SELECT 1)) AS \"v\" UNKNOWN ON ERROR)",
         "JSON_EXISTS(d, p PASSING a + 1 AS x, f((SELECT 1)) AS \"v\" UNKNOWN ON ERROR)"),
        ("JSON_QUERY(d, p RETURNING VARCHAR(7) WITH WRAPPER EMPTY OBJECT ON EMPTY ERROR ON ERROR)",
         "JSON_QUERY(d, p RETURNING VARCHAR(7) WITH UNCONDITIONAL ARRAY WRAPPER EMPTY OBJECT ON EMPTY ERROR ON ERROR)"),
        ("JSON_VALUE(d, p DEFAULT f(x) ON ERROR NULL ON EMPTY)",
         "JSON_VALUE(d, p DEFAULT f(x) ON ERROR NULL ON EMPTY)"),
        ("JSON_OBJECTAGG(KEY::INT, x ABSENT ON NULL)", "JSON_OBJECTAGG(KEY KEY::INT VALUE x ABSENT ON NULL)"),
        ("JSON_OBJECT('a': 1, KEY, v)", "JSON_OBJECT(KEY 'a' VALUE 1, KEY KEY VALUE v)"),
        ("JSON_ARRAYAGG(x ORDER BY a DESC, b NULLS LAST NULL ON NULL)",
         "JSON_ARRAYAGG(x ORDER BY a DESC, b NULLS LAST NULL ON NULL)"),
        ("JSON_ARRAYAGG(x) WITHIN GROUP (ORDER BY z DESC)", "JSON_ARRAYAGG(x) WITHIN GROUP (ORDER BY z DESC)"),
    ] {
        let formatted = emit_statements(&parse(&format!("SELECT {source}")).unwrap()).unwrap();
        assert_eq!(formatted, [format!("SELECT {expected}")]);
        assert_eq!(emit_statements(&parse(&formatted[0]).unwrap()).unwrap(), formatted);
    }
}

#[test]
fn enforces_each_builtin_grammar_only_on_an_unqualified_unquoted_call() {
    for expression in [
        "JSON_ARRAY(1 ORDER BY v)",
        "JSON_ARRAY(x) OVER ()",
        "JSON_ARRAYAGG(x, y)",
        "JSON_ARRAYAGG(x ORDER BY y) WITHIN GROUP (ORDER BY z)",
        "JSON_OBJECTAGG(KEY x: y)",
        "JSON_OBJECTAGG(x, y -> y)",
        "JSON_VALUE(d, p EMPTY ARRAY ON EMPTY)",
        "JSON_QUERY(d, p DEFAULT 1 ON ERROR)",
        "JSON_EXISTS(d, p FALSE ON EMPTY)",
        "JSON_EXISTS(d, p RETURNING INT)",
        "JSON_VALUE(d, p PASSING (SELECT 1) AS x)",
        "JSON_ARRAY(ROW((SELECT 1)))",
        "s.JSON_VALUE(d, p RETURNING INT)",
        "\"JSON_EXISTS\"(d, p TRUE ON ERROR)",
        "f(x NULL ON NULL)",
    ] {
        assert!(
            parse(&format!("SELECT {expression} FROM t")).is_err(),
            "{expression}"
        );
    }
    for expression in [
        "s.JSON_ARRAY(x) FILTER (WHERE TRUE) OVER ()",
        "\"JSON_VALUE\"(x)",
        "JSON_ARRAY(f(x -> x), 1 + (SELECT 1))",
        "JSON_QUERY(d, p WITHOUT WRAPPER)",
        "JSON_OBJECTAGG(KEY.key, v)",
        "f(RETURNING)",
        "f(ABSENT, NULL)",
    ] {
        assert!(
            parse(&format!("SELECT {expression} FROM t")).is_ok(),
            "{expression}"
        );
    }
}
