use feldera_sql_parser::{emit_statements, parse};

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
