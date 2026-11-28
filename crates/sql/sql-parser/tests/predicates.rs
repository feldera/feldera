use feldera_sql_parser::{emit_statements, parse};

#[test]
fn rlike_distinguishes_an_escape_operand_from_an_alias() {
    for (sql, expected) in [
        (
            "SELECT x NOT RLIKE p ESCAPE f(y) FROM t",
            "SELECT x NOT RLIKE p ESCAPE f(y) FROM t",
        ),
        (
            "SELECT x RLIKE p ESCAPE FROM t",
            "SELECT x RLIKE p AS ESCAPE FROM t",
        ),
    ] {
        assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [expected]);
    }
}

#[test]
fn unique_binds_addition_before_testing_the_result_for_null() {
    use feldera_sql_parser::FelderaDialect;
    use sqlparser_064::{
        ast::{BinaryOperator, Expr, Spanned},
        parser::Parser,
    };
    let sql = "UNIQUE x + y IS NULL";
    let mut parser = Parser::new(&FelderaDialect).try_with_sql(sql).unwrap();
    let Expr::IsNull(predicate) = parser.parse_expr().unwrap() else {
        panic!("expected IS NULL applied to UNIQUE");
    };
    assert_eq!(predicate.span().start.column, 1);
    assert_eq!(predicate.span().end.column, 13);
    assert!(matches!(*predicate, Expr::Unique { expr, .. }
        if matches!(*expr, Expr::BinaryOp { op: BinaryOperator::Plus, .. })));
    for sql in [
        "SELECT f(UNIQUE x)",
        "SELECT UNIQUE (SELECT x FROM t)",
        "SELECT UNIQUE t.* FROM t",
    ] {
        assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [sql]);
    }
    assert!(parse("SELECT UNIQUE FROM t").is_err());
}

#[test]
fn translate_distinguishes_positional_arguments_from_a_character_set() {
    for sql in [
        "SELECT TRANSLATE(a)",
        "SELECT TRANSLATE(a, b, c, d)",
        "SELECT TRANSLATE((SELECT x FROM t) USING \"UTF8\")",
        "SELECT TRANSLATE(t.* USING utf8) FROM t",
    ] {
        assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [sql]);
    }
    for args in [
        "",
        "* USING cs",
        "a USING t.cs",
        "a USING 'utf8'",
        "a USING cs, b",
        "a USING NULL",
        "DISTINCT a",
    ] {
        assert!(
            parse(&format!("SELECT TRANSLATE({args})")).is_err(),
            "{args}"
        );
    }
}

#[test]
fn json_object_preserves_name_value_pairs_and_the_null_policy() {
    let sql = "SELECT JSON_OBJECT('k':x, y, z FORMAT JSON ENCODING UTF16, KEY f(a) VALUE (v + 1) ABSENT ON NULL)";
    let emitted = emit_statements(&parse(sql).unwrap()).unwrap();
    assert_eq!(emitted, ["SELECT JSON_OBJECT(KEY 'k' VALUE x, KEY y VALUE z FORMAT JSON ENCODING UTF16, KEY f(a) VALUE (v + 1) ABSENT ON NULL)"]);
    assert_eq!(
        emit_statements(&parse(&emitted[0]).unwrap()).unwrap(),
        emitted
    );
    for args in [
        "",
        "NULL ON NULL",
        "NULL:x",
        "k:f((SELECT x FROM t))",
        "(f(x)):t.*",
        "a:b, f(x):y",
    ] {
        assert!(
            parse(&format!("SELECT JSON_OBJECT({args})")).is_ok(),
            "{args}"
        );
    }
    for args in [
        "KEY k:x",
        "f(a):v",
        "k:(SELECT x FROM t)",
        "k:v,",
        "k:v WITH UNIQUE KEYS",
        "k:v FORMAT JSON ENCODING UTF9",
    ] {
        assert!(
            parse(&format!("SELECT JSON_OBJECT({args})")).is_err(),
            "{args}"
        );
    }
}

#[test]
fn slash_comments_preserve_the_next_line_and_leave_quoted_slashes_alone() {
    let input = "// First comment\nSELECT 'https://example/x'; // don't consume the next statement\nSELECT 6 / 3;";
    let sql = emit_statements(&parse(input).unwrap()).unwrap();
    assert_eq!(sql, ["SELECT 'https://example/x'", "SELECT 6 / 3"]);
}

#[test]
fn null_is_the_pattern_operand_of_rlike() {
    for sql in ["SELECT 'a' RLIKE NULL", "SELECT NULL NOT RLIKE NULL"] {
        assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [sql]);
    }
    assert!(parse("SELECT 'a' RLIKE NULL NULL").is_err());
}

#[test]
fn keeps_datetime_difference_qualifiers_through_nested_expressions() {
    use feldera_sql_parser::Statement;
    use sqlparser_064::ast::{DateTimeField, Expr, SelectItem, SetExpr, Statement as Ast};
    let sql = "SELECT ((a-b)) DAYS(4) TO SECOND(2) AS delta FROM t";
    let statements = parse(sql).unwrap();
    let [Statement::Upstream(node)] = statements.as_slice() else {
        panic!()
    };
    let Ast::Query(query) = node.as_ref() else {
        panic!()
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        panic!()
    };
    let SelectItem::ExprWithAlias {
        expr: Expr::QualifiedDateTimeDifference { interval, span },
        ..
    } = &select.projection[0]
    else {
        panic!("qualifier lost")
    };
    assert_eq!(span.start.column, 8);
    assert_eq!(interval.leading_field, Some(DateTimeField::Day));
    assert_eq!(interval.leading_precision, Some(4));
    assert_eq!(interval.last_field, Some(DateTimeField::Second));
    assert_eq!(interval.fractional_seconds_precision, Some(2));
    let emitted = emit_statements(&statements).unwrap();
    assert_eq!(
        emit_statements(&parse(&emitted[0]).unwrap()).unwrap(),
        emitted
    );

    for expr in [
        "(a+b) MONTH",
        "(x) YEAR",
        "(a-b) DAY TO MONTH",
        "(a-b) MINUTE TO DAY",
        "(a-b) SECOND(2147483648)",
        "(a-b) YEAR(1,2)",
    ] {
        assert!(parse(&format!("SELECT {expr} FROM t")).is_err(), "{expr}");
    }
}

#[test]
fn multiset_predicates_preserve_the_operand_and_negation() {
    use sqlparser_064::ast::{Expr, MultisetPredicate, SelectItem, SetExpr, Statement};

    for (suffix, expected, negated) in [
        ("IS EMPTY", MultisetPredicate::Empty, false),
        ("IS NOT EMPTY", MultisetPredicate::Empty, true),
        ("IS A SET", MultisetPredicate::ASet, false),
        ("IS NOT A SET", MultisetPredicate::ASet, true),
    ] {
        let sql = format!("SELECT MULTISET[1, 2] {suffix} AND true");
        let statements = parse(&sql).unwrap();
        assert_eq!(emit_statements(&statements).unwrap(), [sql]);
        let feldera_sql_parser::Statement::Upstream(statement) = &statements[0] else {
            panic!("expected an ordinary SQL statement");
        };
        let Statement::Query(query) = statement.as_ref() else {
            panic!("expected a query")
        };
        let SetExpr::Select(select) = query.body.as_ref() else {
            panic!("expected SELECT")
        };
        let SelectItem::UnnamedExpr(Expr::BinaryOp { left, .. }) = &select.projection[0] else {
            panic!("expected a conjunction");
        };
        assert!(
            matches!(left.as_ref(), Expr::IsMultiset { expr, predicate, negated: n, .. }
            if *predicate == expected && *n == negated && matches!(expr.as_ref(), Expr::MultisetValue { .. }))
        );
    }
}

#[test]
fn implicit_aliases_end_the_predicate_in_select_lists() {
    use feldera_sql_parser::syntax_diagnostic;

    for (sql, expected) in [
        ("SELECT x REGEXP FROM t", "SELECT x AS REGEXP FROM t"),
        ("SELECT x IS FROM t", "SELECT x AS IS FROM t"),
        (
            "SELECT x IS JSON WITHOUT FROM t",
            "SELECT x IS JSON AS WITHOUT FROM t",
        ),
        (
            "SELECT (x IS NOT EMPTY AS field), ROW(DEFAULT) FROM t",
            "SELECT (x IS NOT EMPTY AS field), ROW(DEFAULT) FROM t",
        ),
    ] {
        assert_eq!(emit_statements(&parse(sql).unwrap()).unwrap(), [expected]);
    }
    for (sql, token) in [
        ("SELECT x AT TIME ZONE 'UTC' FROM t", "TIME"),
        ("SELECT x IS NOT NFC NORMALIZED", "NOT"),
        ("SELECT x IS NOT A bag", "A"),
        ("SELECT x NOT LIKE ANY (1, 2)", "ANY"),
        ("SELECT * FROM t WHERE x REGEXP y", "REGEXP"),
        (
            "SELECT * FROM t WHERE x IS JSON WITHOUT UNIQUE KEYS",
            "WITHOUT",
        ),
        ("SELECT (x AS SELECT)", "AS"),
    ] {
        let start = sql.find(token).unwrap();
        let span = syntax_diagnostic(sql).unwrap().unwrap().span.unwrap();
        assert_eq!(
            (span.start.column, span.end.column),
            (start as u64 + 1, (start + token.len()) as u64 + 1),
            "{sql}"
        );
    }
}
