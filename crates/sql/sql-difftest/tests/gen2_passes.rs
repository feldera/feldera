use feldera_sql_difftest::compare_circuits;
use feldera_sql_ir::Circuit;
use serde_json::Value;

/// Each Java expression constructor runs twice on a shared parent containing Clone(x).
/// The output records which expression and type identities the Java visitor shares.
#[test]
fn rebuilt_expressions_match_java_result_type_sharing() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/gen2_shared_expressions.json")).unwrap();
    for (name, case) in fixture.as_object().unwrap() {
        let input = Circuit::from_json(case["input"].clone()).unwrap();
        let output = feldera_sql_opt::gen2_passes(input).unwrap().to_json();
        compare_circuits(&case["java"], &output).unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}
