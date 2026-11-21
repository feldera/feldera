//! Independent scalar-port boundary checks. These are refusal/recipe tests;
//! SQL result equivalence is checked separately by the weighted runtime fixtures.
use feldera_correlate_probe::{Error, Fragment, decorrelate};
use feldera_sql_rel_model::{LiteralLimits, ObjectLimits, ValidationLimits};
use serde_json::{Value, json};

fn original() -> Value {
    serde_json::from_str(include_str!("fixtures/correlated-filter.json")).unwrap()
}

fn validated(value: Value) -> Fragment {
    let fragment: Fragment = serde_json::from_value(value).unwrap();
    fragment
        .expressions
        .validate(
            &fragment.table,
            &fragment.producers,
            &fragment.properties,
            ValidationLimits::default(),
            LiteralLimits::default(),
            ObjectLimits::default(),
        )
        .unwrap();
    fragment
        .catalog
        .validate(
            &fragment.table.type_table,
            &fragment.expressions,
            &fragment.producers,
            &fragment.properties,
            ValidationLimits::default(),
        )
        .unwrap();
    fragment
}

#[test]
fn count_cannot_default_an_integer_that_is_not_bigint() {
    let mut value = original();
    value["expressions"]["operator_objects"][0]["key"] = json!("SqlStdOperatorTable.COUNT");
    // Make the declared COUNT result nonnullable INTEGER, consistently across
    // the aggregate row and its consumer's input environment. Structural FRel
    // validation does not infer builtin return types; the port must reject it.
    value["properties"]["relations"][2]["fields"]["calls"][0]["ty"] = json!(4);
    value["table"]["type_table"]["types"][8]["kind"]["fields"][0]["ty"] = json!(4);
    value["table"]["type_table"]["types"][0]["kind"]["fields"][1]["ty"] = json!(4);
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported("COUNT result must be signed BIGINT"))
    ));
}

#[test]
fn min_max_cannot_silently_discard_extra_arguments() {
    for operator in ["SqlStdOperatorTable.MIN", "SqlStdOperatorTable.MAX"] {
        let mut value = original();
        value["expressions"]["operator_objects"][0]["key"] = json!(operator);
        value["properties"]["relations"][2]["fields"]["calls"][0]["arguments"] = json!([0, 0]);
        assert!(matches!(
            decorrelate(&validated(value)),
            Err(Error::Unsupported("aggregate call metadata or arguments"))
        ));
    }
}

#[test]
fn absent_grouping_sets_are_not_one_empty_scalar_group() {
    let mut value = original();
    value["properties"]["relations"][2]["fields"]["groups"] = json!([]);
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported("aggregate grouping sets"))
    ));
}

#[test]
fn sum_does_not_inherit_count_empty_group_defaults() {
    let mut value = original();
    value["expressions"]["operator_objects"][0]["key"] = json!("SqlStdOperatorTable.SUM");
    let recipe = decorrelate(&validated(value)).unwrap();
    let serialized = serde_json::to_value(recipe).unwrap();
    assert!(
        serialized["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .any(|node| node["kind"] == "aggregate")
    );
    assert!(!serialized.to_string().contains("zero_if_null"));
}
