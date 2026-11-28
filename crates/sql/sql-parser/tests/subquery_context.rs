use feldera_sql_parser::{emit_statements, parse};

#[test]
fn scalar_subqueries_follow_the_grammar_of_each_operand() {
    for (operand, allowed) in [
        ("(SELECT 1) + x", false),
        ("x + (SELECT 1)", true),
        ("COALESCE((SELECT 1), x)", true),
        ("EXISTS(SELECT 1)", false),
        ("f(EXISTS(SELECT 1))", true),
        ("(SELECT 1)::INT", false),
        ("CAST((SELECT 1) AS INT)", true),
        ("ROW(x, (SELECT 1))", false),
        ("ROW(x, f((SELECT 1)))", true),
        ("MAP[(SELECT 1), x]", false),
        ("MAP[x + (SELECT 1), x]", true),
        ("MULTISET[(SELECT 1)]", false),
        ("MULTISET[COALESCE((SELECT 1), x)]", true),
        ("MULTISET(SELECT /*+ h */ 1)", true),
        ("MULTISET(SELECT 1 UNION SELECT 2)", false),
        ("MULTISET((SELECT 1 UNION SELECT 2))", true),
        ("MULTISET()", false),
        ("MULTISET[]", false),
    ] {
        for sql in [
            format!("CREATE FUNCTION f(x INT) RETURNS INT AS {operand}"),
            format!("LATENESS t.x {operand}"),
            format!("CREATE TABLE t(x INT LATENESS {operand})"),
            format!("SELECT * FROM t FOR SYSTEM_TIME AS OF {operand}"),
        ] {
            let result = parse(&sql);
            assert_eq!(result.is_ok(), allowed, "{sql}: {result:?}");
            if let Ok(nodes) = result {
                let formatted = emit_statements(&nodes).unwrap();
                assert_eq!(formatted[0].contains("/*+ h */"), sql.contains("/*+ h */"));
                assert_eq!(
                    emit_statements(&parse(&formatted[0]).unwrap()).unwrap(),
                    formatted
                );
            }
        }
    }
}

#[test]
fn implicit_table_calls_keep_calcite_lookahead() {
    for (arg, allowed) in [
        ("CASE WHEN x THEN 1 END", false),
        ("1, CASE WHEN x THEN 1 END", true),
        ("CASE x WHEN 1 THEN 2 END", true),
        ("(CASE WHEN x THEN 1 END)", true),
        ("EXISTS(SELECT 1)", true),
        ("MULTISET(SELECT 1)", true),
    ] {
        assert_eq!(
            parse(&format!("SELECT * FROM f({arg})")).is_ok(),
            allowed,
            "{arg}"
        );
    }
}
