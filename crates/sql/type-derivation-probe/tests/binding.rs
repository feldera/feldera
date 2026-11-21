use feldera_type_derivation_probe::{binding, model::SqlType, rewritten, translated};
use serde_json::{Value, json};

fn t(name: &str, nullable: bool) -> SqlType {
    SqlType::parse(name, nullable).unwrap()
}
fn outcomes(name: &str, types: &[SqlType]) -> [Value; 2] {
    [
        translated::infer(name, types),
        rewritten::infer(name, types),
    ]
    .map(|result| serde_json::to_value(binding::bind(name, types, &result)).unwrap())
}

#[test]
fn coalesce_common_type_does_not_always_add_operand_casts() {
    for bound in outcomes("COALESCE", &[t("INTEGER", true), t("BIGINT", false)]) {
        assert_eq!(bound["result_type"]["sql_type"], "BIGINT");
        assert_eq!(bound["arguments"][0]["coerced_type"]["sql_type"], "INTEGER");
        assert_eq!(bound["arguments"][0]["inferred_casts"], json!([]));
    }
    for bound in outcomes("COALESCE", &[t("BOOLEAN", false), t("CHAR(5)", true)]) {
        assert_eq!(
            bound["arguments"][0]["inferred_casts"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(bound["arguments"][1]["inferred_casts"], json!([]));
        assert_eq!(bound["arguments"][1]["coerced_type"]["sql_type"], "CHAR");
    }
}

#[test]
fn abs_uses_custom_registry_and_preserves_explicit_cast_target() {
    for bound in outcomes("ABS", &[t("VARCHAR(5)", true)]) {
        assert_eq!(bound["operator"]["registry"], "feldera-s1-builtins");
        assert!(
            bound["operator"]["key"]
                .as_str()
                .unwrap()
                .ends_with("CustomFunctions$AbsFunction#abs")
        );
        let cast = &bound["arguments"][0]["inferred_casts"][0];
        assert_eq!(cast["full"], "DECIMAL(38, 19)");
        assert_eq!(cast["nullable"], true);
    }
}

#[test]
fn trim_defaults_do_not_change_original_argument_index() {
    for bound in outcomes("TRIM", &[t("CHAR(5)", false)]) {
        assert_eq!(bound["arguments"][0]["argument"], 0);
        assert_eq!(bound["arguments"][0]["operand"], 2);
        assert_eq!(bound["defaults"][0]["sql"], "BOTH");
        assert_eq!(bound["defaults"][1]["sql"], "' '");
        assert_eq!(bound["defaults"][1]["type"]["coercibility"], "IMPLICIT");
    }
}

#[test]
fn validated_sqrt_does_not_claim_later_double_cast() {
    for bound in outcomes("SQRT", &[t("INTEGER", false)]) {
        assert_eq!(bound["result_type"]["sql_type"], "DOUBLE");
        assert_eq!(bound["arguments"][0]["inferred_casts"], json!([]));
    }
}

#[test]
fn malformed_inference_descriptor_is_not_serialized_as_valid_binding() {
    use feldera_type_derivation_probe::model::{Kind, Outcome};
    let input = t("INTEGER", false);
    let malformed = SqlType {
        kind: Kind::Integer { bits: 128 },
        nullable: false,
    };
    let outcome = Outcome::bound(malformed, vec![input]);
    let value = serde_json::to_value(binding::bind("ABS", &[input], &outcome)).unwrap();
    assert_eq!(value, json!({"status":"unsupported"}));
}
