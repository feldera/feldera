use crate::FelderaDialect;
use sqlparser_064::{
    ast::{
        Expr, FunctionArgumentClause, FunctionArguments, GroupByExpr, NullTreatment, Select,
        SelectItem, SetExpr, Statement,
    },
    parser::Parser,
};

fn select(sql: &str) -> Box<Select> {
    let mut statements = Parser::parse_sql(&FelderaDialect, sql).unwrap();
    let Statement::Query(query) = statements.remove(0) else {
        panic!("expected query")
    };
    let SetExpr::Select(select) = *query.body else {
        panic!("expected select")
    };
    select
}

#[test]
fn aggregate_filter_preserves_predicate_distinct_and_window() {
    let query = select("SELECT SUM(DISTINCT x) FILTER (WHERE y > 0) OVER (PARTITION BY z) FROM t");
    let SelectItem::UnnamedExpr(Expr::Function(function)) = &query.projection[0] else {
        panic!("expected function");
    };
    assert_eq!(function.filter.as_ref().unwrap().to_string(), "y > 0");
    assert!(function.over.is_some());
    let FunctionArguments::List(arguments) = &function.args else {
        panic!("expected arguments");
    };
    assert!(arguments.duplicate_treatment.is_some());
    for sql in [
        "SELECT SUM(x) FILTER () FROM t",
        "SELECT SUM(x) FILTER (x > 0) FROM t",
        "SELECT FIRST_VALUE(x) FILTER (WHERE x > 0) IGNORE NULLS OVER () FROM t",
    ] {
        assert!(!super::parse(sql).is_ok(), "{sql}");
    }
    for alias in ["FILTER", "AS FILTER", "\"FILTER\""] {
        let sql = format!("SELECT SUM(x) {alias} FROM t");
        assert!(super::parse(&sql).is_ok());
        assert!(
            matches!(&select(&sql).projection[0], SelectItem::ExprWithAlias { alias, .. } if alias.value == "FILTER")
        );
    }
}

#[test]
fn grouping_sets_keep_empty_sets_tuple_groups_duplicates_and_order() {
    let query = select("SELECT COUNT(*) FROM t GROUP BY GROUPING SETS ((a, b), (), (a), (a))");
    let GroupByExpr::Expressions(groups, modifiers) = query.group_by else {
        panic!("expected grouping expressions")
    };
    assert!(modifiers.is_empty());
    let [Expr::GroupingSets(sets)] = groups.as_slice() else {
        panic!("grouping sets became an ordinary function")
    };
    assert_eq!(sets.iter().map(Vec::len).collect::<Vec<_>>(), [2, 0, 1, 1]);
    assert_eq!(sets[0][0].to_string(), "a");
    assert_eq!(sets[0][1].to_string(), "b");
    assert_eq!(sets[2][0].to_string(), "a");
    assert_eq!(sets[3][0].to_string(), "a");
    for (name, cube) in [("ROLLUP", false), ("CUBE", true)] {
        let query = select(&format!(
            "SELECT COUNT(*) FROM t GROUP BY {name} ((a, b), c)"
        ));
        let GroupByExpr::Expressions(groups, _) = query.group_by else {
            panic!("expected groups")
        };
        let sets = match &groups[0] {
            Expr::Cube(sets) if cube => sets,
            Expr::Rollup(sets) if !cube => sets,
            _ => panic!("grouping operator lost its kind"),
        };
        assert_eq!(sets.iter().map(Vec::len).collect::<Vec<_>>(), [2, 1]);
    }
}

#[test]
fn null_treatment_keeps_placement_and_window_clause() {
    for (keyword, expected) in [
        ("IGNORE", NullTreatment::IgnoreNulls),
        ("RESPECT", NullTreatment::RespectNulls),
    ] {
        for inside in [false, true] {
            let function = if inside {
                format!("ARRAY_AGG(x {keyword} NULLS)")
            } else {
                format!("FIRST_VALUE(x) {keyword} NULLS")
            };
            let query = select(&format!(
                "SELECT {function} OVER (PARTITION BY a ORDER BY b) FROM t"
            ));
            let SelectItem::UnnamedExpr(Expr::Function(function)) = &query.projection[0] else {
                panic!("expected function")
            };
            assert!(function.over.is_some());
            let FunctionArguments::List(arguments) = &function.args else {
                panic!("expected arguments")
            };
            assert_eq!(arguments.args.len(), 1);
            if inside {
                assert_eq!(
                    arguments.clauses,
                    vec![FunctionArgumentClause::IgnoreOrRespectNulls(expected)]
                );
                assert!(function.null_treatment.is_none());
            } else {
                assert!(arguments.clauses.is_empty());
                assert_eq!(function.null_treatment, Some(expected));
            }
        }
    }
}

#[test]
fn inline_null_treatment_is_rejected_outside_its_feldera_grammar() {
    use super::parse;
    for expression in [
        "FIRST_VALUE(x IGNORE NULLS) OVER ()",
        "SUM(x IGNORE NULLS)",
        "COUNT(* IGNORE NULLS)",
        "f(x RESPECT NULLS)",
        "schema.ARRAY_AGG(x IGNORE NULLS)",
        "\"ARRAY_AGG\"(x IGNORE NULLS)",
    ] {
        for sql in [
            format!("SELECT {expression} FROM t"),
            format!("SELECT * FROM (SELECT {expression} FROM t) q"),
        ] {
            assert!(!parse(&sql).is_ok(), "{sql}");
        }
    }
    for expression in [
        "ARRAY_AGG(x IGNORE NULLS)",
        "ARRAY_CONCAT_AGG(x RESPECT NULLS)",
        "STRING_AGG(x, ',' IGNORE NULLS)",
        "GROUP_CONCAT(x IGNORE NULLS)",
        "FIRST_VALUE(x) IGNORE NULLS OVER ()",
    ] {
        let sql = format!("SELECT {expression} FROM t");
        assert!(parse(&sql).is_ok(), "{sql}");
    }
}

#[test]
fn window_exclusions_keep_their_distinct_ast_variants() {
    use sqlparser_064::ast::{WindowFrameExclusion, WindowType};
    for (sql, exclusion) in [
        ("CURRENT ROW", WindowFrameExclusion::CurrentRow),
        ("GROUP", WindowFrameExclusion::Group),
        ("TIES", WindowFrameExclusion::Ties),
        ("NO OTHERS", WindowFrameExclusion::NoOthers),
    ] {
        let source = format!(
            "SELECT sum(x) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW EXCLUDE {sql}) FROM t"
        );
        let query = select(&source);
        let SelectItem::UnnamedExpr(Expr::Function(f)) = &query.projection[0] else {
            panic!("expected aggregate")
        };
        let Some(WindowType::WindowSpec(spec)) = &f.over else {
            panic!("expected window frame")
        };
        assert_eq!(
            spec.window_frame.as_ref().unwrap().exclusion,
            Some(exclusion)
        );
        assert_eq!(
            crate::emit_statements(&crate::parse(&source).unwrap()).unwrap(),
            [source]
        );
    }
    for suffix in [
        "CURRENT",
        "NO OTHER",
        "NONE",
        "OTHERS",
        "GROUP EXCLUDE TIES",
    ] {
        assert!(crate::parse(&format!(
            "SELECT sum(x) OVER (ROWS CURRENT ROW EXCLUDE {suffix}) FROM t"
        ))
        .is_err());
    }
}

#[test]
fn special_aggregates_preserve_percentile_and_separator_operands() {
    let source = "SELECT percentile_disc(DEFAULT, -DECIMAL '.25' IGNORE NULLS), string_agg(DISTINCT x ORDER BY y SEPARATOR ';') FROM t";
    let query = select(source);
    let arguments = |i: usize| {
        let SelectItem::UnnamedExpr(Expr::Function(f)) = &query.projection[i] else {
            panic!("expected call")
        };
        let FunctionArguments::List(args) = &f.args else {
            panic!("expected arguments")
        };
        args
    };
    use sqlparser_064::ast::{FunctionArg, FunctionArgExpr};
    let pct = arguments(0);
    assert!(matches!(
        pct.args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::DefaultValue(_)))
    ));
    assert!(matches!(
        pct.clauses.as_slice(),
        [FunctionArgumentClause::IgnoreOrRespectNulls(
            NullTreatment::IgnoreNulls
        )]
    ));
    let string = arguments(1);
    assert!(string.duplicate_treatment.is_some());
    assert!(
        matches!(string.clauses.as_slice(), [FunctionArgumentClause::OrderBy(_), FunctionArgumentClause::Separator(Expr::Value(v))] if v.value.to_string() == "';'")
    );
    assert_eq!(
        crate::emit_statements(&crate::parse(source).unwrap()).unwrap(),
        [source]
    );
    for sql in [
        "percentile_cont(*)",
        "percentile_cont(DISTINCT x)",
        "percentile_cont(x, (0.5))",
        "percentile_cont(x, 0.5 + 0.1)",
        "percentile_cont(x, DECIMAL 'NaN')",
        "percentile_cont(x, 0.25, 0.75)",
        "percentile_cont(x, 0.5 IGNORE NULLS RESPECT NULLS)",
        "s.percentile_cont(x, 0.5 IGNORE NULLS)",
        "array_agg()",
        "string_agg(* SEPARATOR ',')",
        "string_agg(x SEPARATOR 3)",
        "string_agg(x SEPARATOR ('a'))",
        "s.string_agg(x SEPARATOR ',')",
    ] {
        assert!(
            crate::parse(&format!("SELECT {sql} FROM t")).is_err(),
            "{sql}"
        );
    }
}

#[test]
fn snapshot_membership_and_extract_keep_query_structure() {
    use sqlparser_064::ast::{BinaryOperator, DateTimeField, TableFactor, TableVersion};
    let sql = r#"SELECT a MEMBER OF b + c AND d AS END, EXTRACT("Fiscal Week" FROM t.x) FROM t FOR SYSTEM_TIME AS OF a AS old"#;
    let query = select(sql);
    let SelectItem::ExprWithAlias {
        expr:
            Expr::BinaryOp {
                left,
                op: BinaryOperator::And,
                ..
            },
        alias,
    } = &query.projection[0]
    else {
        panic!("expected aliased conjunction")
    };
    assert_eq!(alias.value, "END");
    assert!(
        matches!(left.as_ref(), Expr::BinaryOp {op: BinaryOperator::MemberOf, right, ..} if matches!(right.as_ref(), Expr::BinaryOp {op: BinaryOperator::Plus, ..}))
    );
    assert!(
        matches!(&query.projection[1], SelectItem::UnnamedExpr(Expr::Extract { field: DateTimeField::Custom(name), .. }) if name.value == "Fiscal Week" && name.quote_style == Some('"'))
    );
    assert!(
        matches!(&query.from[0].relation, TableFactor::Table { alias: Some(alias), version: Some(TableVersion::ForSystemTimeAsOf(Expr::Identifier(time))), .. } if alias.name.value == "old" && time.value == "a")
    );
    assert_eq!(
        crate::emit_statements(&crate::parse(sql).unwrap()).unwrap(),
        [sql]
    );
    for sql in [
        "SELECT * FROM t FOR SYSTEM_TIME AS OF (SELECT x FROM t)",
        "SELECT * FROM t VERSION AS OF 1",
        "SELECT EXTRACT(WHERE FROM x)",
        "SELECT a NOT MEMBER OF b FROM t",
    ] {
        assert!(crate::parse(sql).is_err(), "{sql}");
    }
}

#[test]
fn values_defaults_and_named_fields_are_contextual() {
    let sql = "VALUES (DEFAULT, 1 AS x, t.*)";
    let parsed = crate::parse(sql).unwrap();
    let crate::Statement::Upstream(statement) = &parsed[0] else {
        panic!("expected upstream statement")
    };
    let Statement::Query(query) = statement.as_ref() else {
        panic!("expected query")
    };
    let SetExpr::Values(values) = query.body.as_ref() else {
        panic!("expected values")
    };
    assert!(
        matches!(values.rows[0].as_slice(), [Expr::DefaultValue(_), Expr::Named { name, .. }, Expr::QualifiedWildcard(..)] if name.value == "x")
    );
    assert_eq!(crate::emit_statements(&parsed).unwrap(), [sql]);
    for bad in [
        "VALUES DEFAULT",
        "VALUES (DEFAULT AS x)",
        "VALUES 1 AS x",
        "VALUES (ROW(1 AS x))",
        "SELECT DEFAULT",
    ] {
        assert!(crate::parse(bad).is_err(), "{bad}");
    }
}
