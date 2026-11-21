//! Independent boundary checks derived from the pinned Calcite rules.
//! These assert type inference only; they do not certify emitted casts or C9.
use feldera_type_derivation_probe::{model::SqlType, rewritten, translated};
use serde_json::{Value, json};

fn scalar(name: &str, nullable: bool) -> SqlType {
    SqlType::parse(name, nullable).expect("valid declared scalar type")
}

fn outcomes(operation: &str, arguments: &[SqlType]) -> [Value; 2] {
    [
        serde_json::to_value(translated::infer(operation, arguments)).unwrap(),
        serde_json::to_value(rewritten::infer(operation, arguments)).unwrap(),
    ]
}

fn assert_result(operation: &str, arguments: &[SqlType], expected: SqlType) {
    for outcome in outcomes(operation, arguments) {
        assert_eq!(outcome["status"], "bound", "{operation}: {outcome}");
        assert_eq!(outcome["result"], json!(expected), "{operation}");
    }
}

#[test]
fn decimal_common_type_preserves_whole_digits_before_fraction() {
    // SqlTypeFactoryImpl.leastRestrictiveSqlType caps the whole part first.
    assert_result(
        "COALESCE",
        &[scalar("DECIMAL(38,38)", true), scalar("BIGINT", false)],
        scalar("DECIMAL(38,19)", false),
    );
    assert_result(
        "COALESCE",
        &[
            scalar("DECIMAL(38,0)", true),
            scalar("DECIMAL(38,38)", true),
        ],
        scalar("DECIMAL(38,0)", true),
    );
}

#[test]
fn integral_mod_uses_divisor_type_not_widest_operand() {
    // ReturnTypes.NULLABLE_MOD falls back to ARG1_NULLABLE.
    assert_result(
        "MOD",
        &[scalar("BIGINT", true), scalar("TINYINT", false)],
        scalar("TINYINT", true),
    );
    assert_result(
        "MOD",
        &[scalar("TINYINT", false), scalar("BIGINT", false)],
        scalar("BIGINT", false),
    );
}

#[test]
fn approximate_mod_uses_decimal_family_conversion() {
    // AbstractTypeCoercion.implicitCast -> decimalOf, then deriveDecimalModType.
    let args = [scalar("REAL", false), scalar("INTEGER", true)];
    assert_result("MOD", &args, scalar("DECIMAL(14,7)", true));
    for outcome in outcomes("MOD", &args) {
        assert_eq!(
            outcome["operands"][0],
            json!(scalar("DECIMAL(14,7)", false))
        );
        assert_eq!(outcome["operands"][1], json!(args[1]));
    }
}

#[test]
fn double_result_does_not_imply_double_family_operand() {
    // SQRT uses NUMERIC and DOUBLE_NULLABLE: the checker accepts INTEGER intact.
    let argument = scalar("INTEGER", true);
    assert_result("SQRT", &[argument], scalar("DOUBLE", true));
    for outcome in outcomes("SQRT", &[argument]) {
        assert_eq!(outcome["operands"], json!([argument]));
    }
}

#[test]
fn text_to_numeric_uses_default_decimal_and_keeps_nullability() {
    // SqlTypeFamily.NUMERIC default, rather than precision inferred from a value.
    for nullable in [false, true] {
        let argument = scalar("VARCHAR(5)", nullable);
        let expected = scalar("DECIMAL(38,19)", nullable);
        assert_result("ABS", &[argument], expected);
        for outcome in outcomes("ABS", &[argument]) {
            assert_eq!(outcome["operands"], json!([expected]));
        }
    }
}

#[test]
fn concat_length_overflow_becomes_unspecified() {
    // ReturnTypes.DYADIC_STRING_SUM_PRECISION rejects precision above max length.
    assert_result(
        "CONCAT",
        &[
            scalar("VARCHAR(2147483647)", false),
            scalar("CHAR(1)", false),
        ],
        scalar("VARCHAR", false),
    );
    assert_result(
        "CONCAT",
        &[scalar("CHAR(20)", false), scalar("VARCHAR(30)", true)],
        scalar("VARCHAR(50)", true),
    );
}

#[test]
fn differing_fixed_widths_promote_to_varying_common_type() {
    assert_result(
        "COALESCE",
        &[scalar("CHAR(1)", true), scalar("CHAR(5)", false)],
        scalar("VARCHAR(5)", false),
    );
    assert_result(
        "COALESCE",
        &[scalar("CHAR(5)", true), scalar("CHAR(5)", true)],
        scalar("CHAR(5)", true),
    );
}

#[test]
fn nullif_result_keeps_first_type_and_forces_nullable() {
    // SqlNullifFunction ARG0_FORCE_NULLABLE; comparison coercion is separate.
    assert_result(
        "NULLIF",
        &[scalar("TINYINT", false), scalar("BIGINT", false)],
        scalar("TINYINT", true),
    );
}

#[test]
fn parser_rejects_invalid_scalar_parameters() {
    for text in [
        "DECIMAL(0,0)",
        "DECIMAL(39,0)",
        "DECIMAL(10,11)",
        "CHAR(0)",
        "VARCHAR(2147483648)",
    ] {
        assert!(SqlType::parse(text, false).is_err(), "accepted {text}");
    }
}

#[test]
fn unknown_operation_is_not_an_ordinary_sql_rejection() {
    for outcome in outcomes("UNREVIEWED_FUNCTION", &[scalar("INTEGER", false)]) {
        assert_eq!(outcome["status"], "unsupported");
    }
    for outcome in outcomes("ABS", &[scalar("BOOLEAN", false)]) {
        assert_eq!(outcome["status"], "rejected");
    }
}

#[test]
fn date_timestamp_common_type_does_not_bypass_operand_checker() {
    // The v1 formula accepted these; the pinned validator rejects both functions.
    for operation in ["COALESCE", "NULLIF"] {
        for nullable_a in [false, true] {
            for nullable_b in [false, true] {
                for arguments in [
                    [scalar("DATE", nullable_a), scalar("TIMESTAMP", nullable_b)],
                    [scalar("TIMESTAMP", nullable_a), scalar("DATE", nullable_b)],
                ] {
                    for outcome in outcomes(operation, &arguments) {
                        assert_eq!(outcome["status"], "rejected", "{operation}: {outcome}");
                    }
                }
            }
        }
    }
}

#[test]
fn valid_unsampled_signatures_remain_prototype_limits() {
    // These arities exist in Calcite but are absent from this probe's input slice.
    for (operation, arguments) in [
        ("COALESCE", vec![scalar("INTEGER", false)]),
        ("LOG", vec![scalar("INTEGER", false)]),
        (
            "ROUND",
            vec![scalar("DECIMAL(10,2)", false), scalar("INTEGER", false)],
        ),
    ] {
        for outcome in outcomes(operation, &arguments) {
            assert_eq!(outcome["status"], "unsupported", "{operation}: {outcome}");
        }
    }
}
