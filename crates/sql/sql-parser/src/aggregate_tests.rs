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
