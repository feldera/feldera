use feldera_type_derivation_probe::{
    extras,
    model::{Outcome, SqlType},
};
use serde_json::json;

fn scalar(text: &str, nullable: bool) -> SqlType {
    SqlType::parse(text, nullable).unwrap()
}

fn result(name: &str, args: &[SqlType], expected: SqlType) {
    let actual = serde_json::to_value(extras::infer(name, args)).unwrap();
    assert_eq!(actual["status"], "bound");
    assert_eq!(actual["result"], json!(expected));
}

#[test]
fn set_nullability_follows_each_set_operator() {
    for (a, b) in [(false, true), (true, false), (true, true), (false, false)] {
        let args = [scalar("CHAR(1)", a), scalar("CHAR(5)", b)];
        for (name, nullable) in [
            ("UNION ALL", a || b),
            ("INTERSECT ALL", a && b),
            ("EXCEPT ALL", a),
        ] {
            result(name, &args, scalar("VARCHAR(5)", nullable));
        }
    }
}

#[test]
fn date_decimal_common_type_preserves_directional_calcite_behavior() {
    let decimal = scalar("DECIMAL(10,2)", false);
    let date = scalar("DATE", true);
    result("CASE", &[decimal, date], date);
    assert!(matches!(
        extras::infer("CASE", &[date, decimal]),
        Outcome::Rejected { .. }
    ));
    for args in [
        [scalar("INTEGER", false), date],
        [date, scalar("INTEGER", false)],
    ] {
        result("CASE", &args, date);
    }
}

#[test]
fn array_aggregate_preserves_component_and_outer_nullability() {
    for nullable in [false, true] {
        let input = scalar("DECIMAL(38,10)", nullable);
        let actual = serde_json::to_value(extras::infer("ARRAY_AGG", &[input])).unwrap();
        assert_eq!(
            actual["result"],
            json!({"component":input,"nullable":nullable})
        );
    }
}

#[test]
fn arithmetic_string_coercion_uses_peer_type_before_family_defaults() {
    let args = [scalar("VARCHAR(5)", true), scalar("SMALLINT", false)];
    result("+", &args, scalar("SMALLINT", true));
    result("/", &args, scalar("SMALLINT", true));
    let actual = serde_json::to_value(extras::infer("+", &args)).unwrap();
    assert_eq!(actual["operands"][0], json!(scalar("SMALLINT", true)));
}

#[test]
fn decimal_operations_keep_distinct_precision_rules() {
    let args = [
        scalar("DECIMAL(10,2)", false),
        scalar("DECIMAL(10,2)", false),
    ];
    result("+", &args, scalar("DECIMAL(11,2)", false));
    result("*", &args, scalar("DECIMAL(20,4)", false));
    result("/", &args, scalar("DECIMAL(23,13)", false));
    result(
        "/",
        &[scalar("DECIMAL(38,10)", false), args[0]],
        scalar("DECIMAL(38,6)", false),
    );
}

#[test]
fn global_aggregate_nullability_differs_from_scalar_calls() {
    let input = scalar("DECIMAL(10,2)", false);
    result("SUM", &[input], scalar("DECIMAL(38,2)", true));
    result("AVG", &[input], scalar("DECIMAL(10,2)", true));
    result("COUNT", &[input], scalar("BIGINT", false));
}

#[test]
fn equality_and_ordering_have_distinct_boolean_coercions() {
    let args = [scalar("BOOLEAN", false), scalar("INTEGER", true)];
    result("=", &args, scalar("BOOLEAN", true));
    assert!(matches!(
        extras::infer("<", &args),
        Outcome::Rejected { .. }
    ));
}
