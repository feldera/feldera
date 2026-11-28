use feldera_sql_parser::{emit_statements, parse};

#[test]
fn cycle_marks_belong_to_their_cte() {
    let sql = "WITH RECURSIVE x AS (SELECT 1) CYCLE a, b SET mark TO 1 + 1 DEFAULT 0 USING path, y AS (SELECT 2) CYCLE c SET m USING p SELECT * FROM x";
    assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [sql]);
    for clause in [
        "a.b SET m USING p",
        "a SET m TO 1 USING p",
        "a SET m TO (SELECT 1) DEFAULT 0 USING p",
    ] {
        assert!(
            parse(&format!(
                "WITH x AS (SELECT 1) CYCLE {clause} SELECT * FROM x"
            ))
            .is_err(),
            "{clause}"
        );
    }
}

#[test]
fn natural_joins_keep_their_explicit_conditions() {
    for join in [
        "NATURAL LEFT ASOF JOIN b MATCH_CONDITION (a.t >= b.t) ON a.k = b.k",
        "NATURAL CROSS JOIN b USING(k, v)",
        "NATURAL LEFT JOIN b ON a.k = b.k",
        "CROSS JOIN b USING(k)",
    ] {
        let sql = format!("SELECT * FROM a {join}");
        let emitted = emit_statements(&parse(&sql).unwrap()).unwrap();
        assert_eq!(emitted, [sql]);
        assert_eq!(
            emit_statements(&parse(&emitted[0]).unwrap()).unwrap(),
            emitted
        );
    }
    for join in [
        "JOIN b USING(b.x)",
        "NATURAL JOIN b USING(SELECT)",
        "CROSS JOIN b USING(k) AS c",
    ] {
        assert!(parse(&format!("SELECT * FROM a {join}")).is_err(), "{join}");
    }
}

#[test]
fn preserves_between_symmetry_and_grouping_set_deduplication() {
    for clause in [
        "WHERE x NOT BETWEEN SYMMETRIC 4 AND 2",
        "WHERE x BETWEEN ASYMMETRIC 4 AND 2",
        "GROUP BY DISTINCT CUBE (x), ROLLUP (x)",
    ] {
        let sql = format!("SELECT x FROM t {clause}");
        let rendered = emit_statements(&parse(&sql).unwrap()).unwrap();
        let expected = clause
            .split_whitespace()
            .find(|word| matches!(*word, "SYMMETRIC" | "ASYMMETRIC" | "DISTINCT"))
            .unwrap();
        assert!(rendered[0].contains(expected));
        assert_eq!(
            emit_statements(&parse(&rendered[0]).unwrap()).unwrap(),
            rendered
        );
    }
}

#[test]
fn pagination_preserves_clause_and_parameter_order() {
    for sql in [
        "SELECT 1 LIMIT 2, 7",
        "SELECT 1 LIMIT ?, ALL OFFSET 3",
        "SELECT 1 OFFSET ? ROWS LIMIT ?, ?",
        "SELECT 1 FETCH FIRST (? + 1) ROWS ONLY",
        "SELECT 1 LIMIT DECIMAL '01.0', DECIMAL '-2'",
        "SELECT 1 OFFSET 1 + 2 FETCH FIRST DECIMAL '３' ROWS ONLY",
    ] {
        let rendered = emit_statements(&parse(sql).unwrap()).unwrap();
        assert_eq!(rendered, [sql]);
    }
    for sql in [
        "SELECT 1 LIMIT x, 1",
        "SELECT 1 LIMIT ALL, 3",
        "SELECT 1 LIMIT 3, -2",
        "SELECT 1 LIMIT 1, x + 3",
        "SELECT 1 LIMIT 1+2",
        "SELECT 1 LIMIT DECIMAL '1'",
        "SELECT 1 LIMIT ALL FETCH FIRST 1 ROW ONLY",
        "SELECT 1 FETCH FIRST ROW ONLY",
        "SELECT 1 FETCH 1 ROW ONLY",
        "SELECT 1 FETCH FIRST 1 ROWS",
        "SELECT 1 FETCH FIRST DECIMAL '1e' ROW ONLY",
        "SELECT 1 OFFSET (SELECT 1)",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn unicode_escape_applies_to_all_fragments_and_roundtrips_supplementary_characters() {
    for (source, expected) in [
        (r"SELECT U&'d!0061t!0061' UESCAPE '!'", "SELECT U&'data'"),
        (
            r"SELECT U&'!D83D!DC0D' UESCAPE '!'",
            r"SELECT U&'\D83D\DC0D'",
        ),
        (
            "SELECT U&'one'\n'!!two' UESCAPE '!'",
            "SELECT U&'one'\n'!two'",
        ),
        (r"SELECT U&'x\y' UESCAPE '!'", r"SELECT U&'x\\y'"),
    ] {
        let formatted = emit_statements(&parse(source).unwrap()).unwrap();
        assert_eq!(formatted, [expected]);
        assert_eq!(
            emit_statements(&parse(expected).unwrap()).unwrap(),
            formatted
        );
    }
    for invalid in [r"SELECT U&'\D83D'", "SELECT U&'x!' UESCAPE '!'"] {
        assert!(parse(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn map_query_and_inline_foreign_key_preserve_their_syntax() {
    let sql = "CREATE TABLE bids (a INT FOREIGN KEY REFERENCES auctions(id));\nCREATE VIEW v AS SELECT MAP(SELECT a, a FROM bids)";
    let emitted = emit_statements(&parse(sql).unwrap()).unwrap();
    assert!(emitted[0].contains("FOREIGN KEY REFERENCES auctions (id)"));
    assert!(emitted[1].contains("MAP(SELECT a, a FROM bids)"));
    assert_eq!(
        emit_statements(&parse(&emitted.join(";")).unwrap()).unwrap(),
        emitted
    );
    for invalid in [
        "CREATE TABLE t(a INT FOREIGN KEY REFERENCES db.s(id))",
        "CREATE TABLE t(a INT FOREIGN KEY REFERENCES s(a, b))",
        "SELECT MAP(SELECT FROM)",
    ] {
        assert!(parse(invalid).is_err(), "{invalid}");
    }
}

#[test]
fn retains_named_rows_grouped_unpivot_and_nested_grouping_sets() {
    for sql in [
        "SELECT ROW(x AS a, ROW(y AS b) AS c) FROM t",
        "SELECT ROW(t.* EXCLUDE (x)) FROM t",
        "SELECT t.* EXCEPT (t.a, t.b) FROM t",
        "SELECT * FROM t UNPIVOT INCLUDE NULLS ((m, n) FOR (x, y) IN ((a, b) AS ('x', 2), (c, d) AS ('y', 3)))",
        "SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((a, b), GROUPING SETS ((a), (b)))",
        "SELECT * FROM t EXCEPT (SELECT * FROM u)",
    ] {
        let emitted = emit_statements(&parse(sql).unwrap()).unwrap();
        assert_eq!(emit_statements(&parse(&emitted[0]).unwrap()).unwrap(), emitted, "{sql}");
    }
    for sql in [
        "SELECT ROW()",
        "SELECT ROW(x alias) FROM t",
        "SELECT * FROM t UNPIVOT ((m,) FOR (x) IN ((a)))",
        "SELECT * FROM t UNPIVOT ((m) FOR () IN ((a)))",
        "SELECT * FROM t UNPIVOT ((m) FOR (x) IN (()))",
    ] {
        assert!(parse(sql).is_err(), "{sql}");
    }
}
