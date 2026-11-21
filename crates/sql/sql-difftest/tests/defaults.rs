use feldera_sql_difftest::compare_circuits;
use feldera_sql_expr::{prepare_column_default, Rewrite, Rule};
use feldera_sql_ir::{Circuit, CircuitEditor};
use serde_json::{json, Value};

fn circuit(default: &Value) -> Value {
    json!({"class":"DBSPCircuit","id":0,"declarations":[],"allOperators":[
        {"class":"DBSPConstantOperator","id":1,"inputs":[],"function":default}
    ]})
}

/// The defaults and their expected graphs are from Java's ExpandCasts observations.
#[test]
fn scalar_defaults_match_java_including_shared_types_and_positions() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/scalar_defaults.json")).unwrap();
    for (name, case) in cases.as_object().unwrap() {
        let input = Circuit::from_json(circuit(&case["before"])).unwrap();
        let actual = CircuitEditor::new(input)
            .rewrite_inner_roots(|editor, id| {
                let expression = editor.expression(id)?;
                prepare_column_default(editor, expression, &mut 0).map(Into::into)
            })
            .unwrap();
        compare_circuits(&circuit(&case["after"]), &actual.to_json())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
    }
}

#[test]
fn collection_converters_match_java_and_propagate_failures() {
    let cases: Value =
        serde_json::from_str(include_str!("fixtures/compound_defaults.json")).unwrap();
    check_converters(&cases);
}

#[test]
fn scalar_literal_casts_follow_java_rounding_and_runtime_error_rules() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/literal_casts.json")).unwrap();
    check_converters(&cases);
}

#[test]
fn safe_converters_allocate_names_before_unsafe_siblings() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/sibling_casts.json")).unwrap();
    check_converters(&cases);
}

fn check_converters(cases: &Value) {
    for (name, case) in cases.as_object().unwrap() {
        let input = Circuit::from_json(circuit(&case["before"])).unwrap();
        let mut next = 0;
        let actual = CircuitEditor::new(input).rewrite_inner_roots(|editor, id| {
            let input = editor.expression(id)?;
            prepare_column_default(editor, input, &mut next).map(Into::into)
        });
        if let Some(java_error) = case.get("java_error") {
            let expected = match name.as_str() {
                "map-safe" => "result unwrap has an incompatible target type",
                "variant-to-nested-array-safe" => "SAFE_CAST requires a nullable result",
                _ => panic!("unclassified Java error for {name}: {java_error}"),
            };
            assert_eq!(actual.unwrap_err().to_string(), expected, "{name}");
        } else {
            let result = actual.unwrap_or_else(|e| panic!("{name}: {e}"));
            compare_circuits(&circuit(&case["after"]), &result.to_json())
                .unwrap_or_else(|e| panic!("{name}: {e}\n{}", result.to_json()));
            assert_eq!(case["next_name"].as_u64(), Some(next), "{name}");
        }
    }
}

#[test]
fn named_rows_match_the_java_tuple_conversion() {
    let cases: Value = serde_json::from_str(include_str!("fixtures/named_row_types.json")).unwrap();
    for (name, case) in cases.as_object().unwrap() {
        let input = Circuit::from_json(circuit(&case["before"])).unwrap();
        let mut visitor = Rewrite::new(Rule::EliminateStructs);
        let actual = CircuitEditor::new(input)
            .rewrite_inner_roots(|editor, id| visitor.apply(editor, id))
            .unwrap();
        compare_circuits(&circuit(&case["after"]), &actual.to_json())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
    }
}
