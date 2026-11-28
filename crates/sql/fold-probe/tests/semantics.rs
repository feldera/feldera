use feldera_fold_probe::{Expression, Refusal, evaluate};
use serde_json::{Value, json};

fn ty(name: &str) -> Value {
    json!({"name": name, "precision": -1, "scale": 0})
}

fn literal(name: &str, value: Option<&str>) -> Value {
    json!({"kind": "LITERAL", "type": ty(name), "value": value})
}

fn call(kind: &str, name: &str, args: Vec<Value>) -> Value {
    json!({"kind": kind, "operator": kind, "deterministic": true,
           "type": ty(name), "arguments": args})
}

fn run(expression: Value) -> Result<Value, Refusal> {
    evaluate(&serde_json::from_value::<Expression>(expression).unwrap())
}

#[test]
fn maps_retain_last_values_and_refuse_unbounded_or_malformed_constructors() {
    let duplicate = call(
        "MAP_VALUE_CONSTRUCTOR",
        "MAP",
        vec![
            literal("INTEGER", Some("1")),
            literal("VARCHAR", Some("old")),
            literal("INTEGER", Some("1")),
            literal("VARCHAR", None),
        ],
    );
    assert_eq!(
        run(duplicate).unwrap(),
        json!({"type":"map", "value":[[
            {"type":"integer", "value":"1"}, {"type":"null"}
        ]]})
    );
    assert!(matches!(
        run(call(
            "MAP_VALUE_CONSTRUCTOR",
            "MAP",
            vec![literal("INTEGER", Some("1"))]
        )),
        Err(Refusal::Unsupported(_))
    ));
    let too_many = (0..257)
        .flat_map(|n| {
            [
                literal("INTEGER", Some(&n.to_string())),
                literal("VARCHAR", None),
            ]
        })
        .collect();
    assert!(matches!(
        run(call("MAP_VALUE_CONSTRUCTOR", "MAP", too_many)),
        Err(Refusal::Budget)
    ));
}

#[test]
fn array_join_skips_nulls_only_when_replacement_is_omitted() {
    let array = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![
            literal("VARCHAR", Some("a")),
            literal("VARCHAR", None),
            literal("VARCHAR", Some("🦀")),
        ],
    );
    for (replacement, expected) in [
        (None, json!({"type":"string", "value":"a,🦀"})),
        (
            Some(literal("VARCHAR", Some("*"))),
            json!({"type":"string", "value":"a,*,🦀"}),
        ),
        (Some(literal("VARCHAR", None)), json!({"type":"null"})),
    ] {
        let mut arguments = vec![array.clone(), literal("VARCHAR", Some(","))];
        arguments.extend(replacement);
        let mut expression = call("OTHER_FUNCTION", "VARCHAR", arguments);
        expression["operator"] = "ARRAY_TO_STRING".into();
        assert_eq!(run(expression).unwrap(), expected);
    }
    let array = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![literal("VARCHAR", Some("")); 100],
    );
    let mut expression = call(
        "OTHER_FUNCTION",
        "VARCHAR",
        vec![array, literal("VARCHAR", Some(&"a".repeat(20000)))],
    );
    expression["operator"] = "ARRAY_TO_STRING".into();
    assert!(matches!(run(expression), Err(Refusal::Budget)));
}

#[test]
fn integer_binary_casts_preserve_source_width_and_pad_on_the_left() {
    for (source, value, target, precision, expected) in [
        ("TINYINT", "-1", "BINARY", 4, "000000ff"),
        ("INTEGER", "-1", "VARBINARY", 8, "ffffffff"),
        ("INTEGER", "16909060", "BINARY", 2, "0304"),
        ("SMALLINT", "256", "VARBINARY", -1, "0100"),
        ("BIGINT", "-1", "BINARY", 0, ""),
        (
            "UBIGINT",
            "18446744073709551615",
            "BINARY",
            10,
            "0000ffffffffffffffff",
        ),
    ] {
        let mut expression = call("CAST", target, vec![literal(source, Some(value))]);
        expression["type"]["precision"] = precision.into();
        assert_eq!(
            run(expression).unwrap(),
            json!({"type":"binary", "value":expected})
        );
    }
    let mut oversized = call("CAST", "BINARY", vec![literal("INTEGER", Some("1"))]);
    oversized["type"]["precision"] = (1024 * 1024 + 1).into();
    assert!(matches!(run(oversized), Err(Refusal::Budget)));
}

#[test]
fn array_subscript_variants_are_not_one_based_item() {
    let array = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![
            literal("INTEGER", Some("10")),
            literal("INTEGER", Some("20")),
        ],
    );
    for operator in ["OFFSET", "SAFE_OFFSET", "ORDINAL", "SAFE_ORDINAL"] {
        let mut expression = call(
            "ITEM",
            "INTEGER",
            vec![array.clone(), literal("INTEGER", Some("1"))],
        );
        expression["operator"] = operator.into();
        assert!(matches!(run(expression), Err(Refusal::Unsupported(_))));
    }
}

#[test]
fn legacy_map_concat_preserves_keys_distinct_before_coercion() {
    use feldera_fold_probe::{SemanticsProfile, evaluate_with_profile};
    let decimal = json!({"name":"DECIMAL", "precision":11, "scale":1});
    let mut a = call(
        "MAP_VALUE_CONSTRUCTOR",
        "MAP",
        vec![literal("INTEGER", Some("1")), literal("INTEGER", Some("2"))],
    );
    a["type"]["key"] = ty("INTEGER");
    a["type"]["value"] = ty("INTEGER");
    let mut key = literal("DECIMAL", Some("1.0"));
    key["type"] = decimal.clone();
    let mut b = call(
        "MAP_VALUE_CONSTRUCTOR",
        "MAP",
        vec![key, literal("INTEGER", None)],
    );
    b["type"]["key"] = decimal.clone();
    b["type"]["value"] = ty("INTEGER");
    let mut concat = call("MAP_CONCAT", "MAP", vec![a, b]);
    concat["type"]["key"] = decimal;
    concat["type"]["value"] = ty("INTEGER");
    let expression = serde_json::from_value(concat).unwrap();
    let result = evaluate_with_profile(&expression, SemanticsProfile::LegacyJanino).unwrap();
    assert_eq!(
        result,
        json!({"type":"map", "value":[
            [{"type":"decimal", "value":"1"}, {"type":"integer", "value":"2"}],
            [{"type":"decimal", "value":"1"}, {"type":"null"}]
        ]})
    );
}

#[test]
fn spatial_builtins_require_identity_and_preserve_nulls() {
    let mut point = call(
        "ST_POINT",
        "GEOMETRY",
        vec![literal("INTEGER", Some("0")), literal("INTEGER", Some("0"))],
    );
    point["operator_class"] = "org.apache.calcite.sql.validate.SqlUserDefinedFunction".into();
    assert!(matches!(run(point.clone()), Err(Refusal::Unsupported(_))));
    point["builtin"] = "calcite-point-2d-v1".into();
    let mut distance = call("OTHER_FUNCTION", "DOUBLE", vec![point.clone(), point]);
    distance["operator"] = "ST_DISTANCE".into();
    distance["builtin"] = "calcite-point-distance-v1".into();
    assert_eq!(run(distance.clone()).unwrap()["value"], "0000000000000000");
    distance["arguments"][0] = literal("GEOMETRY", None);
    assert_eq!(run(distance).unwrap(), json!({"type":"null"}));
}

#[test]
fn legacy_split_preserves_empty_array_results() {
    use feldera_fold_probe::{SemanticsProfile, evaluate_with_profile};
    let mut wire = call(
        "OTHER_FUNCTION",
        "ARRAY",
        vec![literal("VARCHAR", Some("")), literal("VARCHAR", Some(""))],
    );
    wire["operator"] = "SPLIT".into();
    let expression = serde_json::from_value(wire).unwrap();
    assert_eq!(
        evaluate(&expression).unwrap(),
        json!({"type":"array", "value":[{"type":"string", "value":""}]})
    );
    assert_eq!(
        evaluate_with_profile(&expression, SemanticsProfile::LegacyJanino).unwrap(),
        json!({"type":"array", "value":[]})
    );
}

#[test]
fn legacy_parser_checks_apply_only_to_evaluated_casts() {
    use feldera_fold_probe::{SemanticsProfile, evaluate_with_profile};
    for (target, value) in [
        ("DOUBLE", "nan"),
        ("REAL", " inf "),
        ("TIMESTAMP", "2059-10-5 5:3:37"),
    ] {
        let wire = call("CAST", target, vec![literal("VARCHAR", Some(value))]);
        let expression: Expression = serde_json::from_value(wire.clone()).unwrap();
        assert!(evaluate(&expression).is_ok());
        assert!(matches!(
            evaluate_with_profile(&expression, SemanticsProfile::LegacyJanino),
            Err(Refusal::RuntimeError(_))
        ));
        let selected = call(
            "CASE",
            target,
            vec![
                literal("BOOLEAN", Some("false")),
                wire,
                literal(target, None),
            ],
        );
        assert_eq!(
            evaluate_with_profile(
                &serde_json::from_value(selected).unwrap(),
                SemanticsProfile::LegacyJanino
            )
            .unwrap(),
            json!({"type": "null"})
        );
    }
    for value in [
        "NaN",
        "+NaN",
        "-NaN",
        "Infinity",
        "+Infinity",
        "-Infinity",
        "1e999",
    ] {
        let expression = serde_json::from_value(call(
            "CAST",
            "DOUBLE",
            vec![literal("VARCHAR", Some(value))],
        ))
        .unwrap();
        assert!(
            evaluate_with_profile(&expression, SemanticsProfile::LegacyJanino).is_ok(),
            "{value}"
        );
    }
    let mut log = call(
        "OTHER_FUNCTION",
        "DOUBLE",
        vec![literal("DOUBLE", Some("2")), literal("DOUBLE", Some("0"))],
    );
    log["operator"] = "LOG".into();
    let expression = serde_json::from_value(log).unwrap();
    assert!(evaluate(&expression).is_ok());
    assert!(matches!(
        evaluate_with_profile(&expression, SemanticsProfile::LegacyJanino),
        Err(Refusal::RuntimeError(_))
    ));
}

#[test]
fn legacy_math_requires_an_explicit_profile_and_preserves_measured_java_bits() {
    use feldera_fold_probe::{SemanticsProfile, evaluate_with_profile};
    for (name, operands, expected) in [
        (
            "EXP",
            vec![literal("DOUBLE", Some("1"))],
            "4005bf0a8b14576a",
        ),
        (
            "LOG",
            vec![literal("DOUBLE", Some("3")), literal("DOUBLE", Some("4"))],
            "3fe95c01a39fbd68",
        ),
    ] {
        let mut wire = call("OTHER_FUNCTION", "DOUBLE", operands);
        wire["operator"] = name.into();
        let expression: Expression = serde_json::from_value(wire).unwrap();
        assert_eq!(
            evaluate_with_profile(&expression, SemanticsProfile::LegacyExpLog).unwrap()["value"],
            expected
        );
        assert_eq!(
            evaluate(&expression).unwrap(),
            evaluate_with_profile(&expression, SemanticsProfile::Runtime).unwrap()
        );
    }
    assert!("unknown-profile".parse::<SemanticsProfile>().is_err());
}

#[test]
fn selection_distinguishes_strict_and_ignored_nulls() {
    let values = vec![
        literal("BIGINT", Some("2")),
        literal("BIGINT", None),
        literal("BIGINT", Some("-1")),
    ];
    for kind in ["GREATEST", "LEAST"] {
        assert_eq!(
            run(call(kind, "BIGINT", values.clone())).unwrap(),
            json!({"type":"null"})
        );
    }
    for (kind, expected) in [("GREATEST_PG", "2"), ("LEAST_PG", "-1")] {
        assert_eq!(
            run(call(kind, "BIGINT", values.clone())).unwrap()["value"],
            expected
        );
        assert_eq!(
            run(call(kind, "BIGINT", vec![literal("BIGINT", None)])).unwrap(),
            json!({"type":"null"})
        );
    }
    assert!(matches!(
        run(call("LEAST", "BIGINT", vec![])),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn nullif_compares_values_without_treating_null_as_equal() {
    for (left, right, expected) in [
        (Some("1"), Some("1"), None),
        (Some("1"), Some("2"), Some("1")),
        (Some("1"), None, Some("1")),
        (None, Some("1"), None),
    ] {
        let result = run(call(
            "NULLIF",
            "BIGINT",
            vec![literal("BIGINT", left), literal("BIGINT", right)],
        ))
        .unwrap();
        assert_eq!(
            result,
            expected.map_or_else(
                || json!({"type":"null"}),
                |value| json!({"type":"integer", "value":value})
            )
        );
    }
    let cast = |value| call("CAST", "DOUBLE", vec![literal("VARCHAR", Some(value))]);
    assert_eq!(
        run(call("NULLIF", "DOUBLE", vec![cast("NaN"), cast("NaN")])).unwrap(),
        json!({"type":"null"})
    );
    assert_eq!(
        run(call(
            "NULLIF",
            "DOUBLE",
            vec![literal("DOUBLE", Some("-0.0")), cast("1")]
        ))
        .unwrap()["value"],
        "0000000000000000"
    );
    assert!(matches!(
        run(call("NULLIF", "BIGINT", vec![literal("BIGINT", Some("1"))])),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn sort_array_preserves_duplicates_and_reverses_null_placement() {
    let array = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![
            literal("BIGINT", Some("2")),
            literal("BIGINT", None),
            literal("BIGINT", Some("1")),
            literal("BIGINT", Some("2")),
        ],
    );
    for (ascending, expected) in [
        (
            "true",
            json!([{"type":"null"},{"type":"integer","value":"1"},{"type":"integer","value":"2"},{"type":"integer","value":"2"}]),
        ),
        (
            "false",
            json!([{"type":"integer","value":"2"},{"type":"integer","value":"2"},{"type":"integer","value":"1"},{"type":"null"}]),
        ),
    ] {
        assert_eq!(
            run(call(
                "SORT_ARRAY",
                "ARRAY",
                vec![array.clone(), literal("BOOLEAN", Some(ascending))]
            ))
            .unwrap()["value"],
            expected
        );
    }
    let oversized = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![literal("BIGINT", None); 257],
    );
    assert!(matches!(
        run(call("SORT_ARRAY", "ARRAY", vec![oversized])),
        Err(Refusal::Budget)
    ));
    let mixed = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![literal("VARCHAR", Some("a")), literal("BIGINT", Some("1"))],
    );
    assert!(matches!(
        run(call("SORT_ARRAY", "ARRAY", vec![mixed])),
        Err(Refusal::Unsupported(_))
    ));
}

fn division_by_zero() -> Value {
    call(
        "CHECKED_DIVIDE",
        "INTEGER",
        vec![literal("INTEGER", Some("1")), literal("INTEGER", Some("0"))],
    )
}

#[test]
fn decimal_arithmetic_promotes_integers_and_preserves_remainder_sign() {
    for (kind, left_type, left, right_type, right, expected) in [
        ("TIMES", "DECIMAL", "0.809", "INTEGER", "100", "80.9"),
        ("TIMES", "INTEGER", "100", "DECIMAL", "0.809", "80.9"),
        ("DIVIDE", "INTEGER", "1", "DECIMAL", "4", "0.25"),
        ("MOD", "DECIMAL", "-5.25", "INTEGER", "2", "-1.25"),
        ("MOD", "DECIMAL", "5.25", "DECIMAL", "-2", "1.25"),
        ("MOD", "INTEGER", "-5", "DECIMAL", "2.25", "-0.5"),
        ("PLUS", "INTEGER", "1", "INTEGER", "2", "3"),
    ] {
        let mut expression = call(
            kind,
            "DECIMAL",
            vec![
                literal(left_type, Some(left)),
                literal(right_type, Some(right)),
            ],
        );
        expression["type"] = json!({"name":"DECIMAL", "precision":20, "scale":3});
        assert_eq!(run(expression).unwrap()["value"], expected);
    }
    let mut expression = call(
        "MOD",
        "DECIMAL",
        vec![
            literal("DECIMAL", Some("5.25")),
            literal("INTEGER", Some("0")),
        ],
    );
    expression["type"] = json!({"name":"DECIMAL", "precision":20, "scale":3});
    assert!(matches!(
        run(expression.clone()),
        Err(Refusal::RuntimeError(_))
    ));
    expression["arguments"][0]["value"] = Value::Null;
    assert_eq!(run(expression).unwrap(), json!({"type":"null"}));
}

#[test]
fn uuid_binary_casts_preserve_network_byte_order_and_exact_length() {
    let uuid = "123e4567-e89b-12d3-a456-426655440000";
    let bytes = "123e4567e89b12d3a456426655440000";
    assert_eq!(
        run(call("CAST", "VARBINARY", vec![literal("UUID", Some(uuid))])).unwrap(),
        json!({"type":"binary", "value":bytes})
    );
    assert_eq!(
        run(call(
            "CAST",
            "UUID",
            vec![literal("VARBINARY", Some(bytes))]
        ))
        .unwrap(),
        json!({"type":"uuid", "value":uuid})
    );
    for bytes in [
        "",
        "00",
        "123e4567e89b12d3a4564266554400",
        "123e4567e89b12d3a45642665544000000",
    ] {
        assert!(matches!(
            run(call(
                "CAST",
                "UUID",
                vec![literal("VARBINARY", Some(bytes))]
            )),
            Err(Refusal::RuntimeError(_))
        ));
        assert_eq!(
            run(call(
                "SAFE_CAST",
                "UUID",
                vec![literal("VARBINARY", Some(bytes))]
            ))
            .unwrap(),
            json!({"type":"null"})
        );
    }
}

#[test]
fn binary_search_and_hash_cover_empty_non_utf8_and_bounded_work() {
    let binary = |text| literal("VARBINARY", Some(text));
    for (name, args, expected) in [
        (
            "STARTS_WITH",
            vec![binary("ff00"), binary("ff")],
            json!(true),
        ),
        ("ENDS_WITH", vec![binary("00ff"), binary("ff")], json!(true)),
        ("STARTS_WITH", vec![binary(""), binary("")], json!(true)),
        ("ENDS_WITH", vec![binary(""), binary("00")], json!(false)),
    ] {
        assert_eq!(
            run(call(name, "BOOLEAN", args)).unwrap(),
            json!({"type":"boolean", "value":expected})
        );
    }
    for (needle, haystack, expected) in [("ff", "00ffff", "2"), ("", "", "1"), ("00ff", "00", "0")]
    {
        assert_eq!(
            run(call(
                "POSITION",
                "INTEGER",
                vec![binary(needle), binary(haystack)]
            ))
            .unwrap(),
            json!({"type":"integer", "value":expected})
        );
    }
    let mut md5 = call("OTHER_FUNCTION", "VARCHAR", vec![binary("")]);
    md5["operator"] = json!("MD5");
    assert_eq!(
        run(md5).unwrap(),
        json!({"type":"string", "value":"d41d8cd98f00b204e9800998ecf8427e"})
    );
    assert!(matches!(
        run(call(
            "POSITION",
            "INTEGER",
            vec![binary(&"ff".repeat(1024)), binary(&"ff".repeat(4096))]
        )),
        Err(Refusal::Budget)
    ));
}

#[test]
fn temporal_rounding_keeps_calendar_origins_and_boundary_errors() {
    let rounded = |op, name, value, unit| {
        run(call(
            op,
            name,
            vec![literal(name, value), literal("SYMBOL", Some(unit))],
        ))
    };
    for (op, unit, input, expected) in [
        ("FLOOR", "CENTURY", "2000-06-01", "2000-01-01"),
        ("DATE_TRUNC", "CENTURY", "2000-06-01", "1901-01-01"),
        ("DATE_TRUNC", "MILLENNIUM", "2000-06-01", "1001-01-01"),
        ("CEIL", "MILLENNIUM", "0001-01-01", "1000-01-01"),
        ("CEIL", "MONTH", "2024-02-29", "2024-03-01"),
        ("FLOOR", "WEEK", "2024-04-01", "2024-03-31"),
        ("CEIL", "WEEK", "2024-03-31", "2024-03-31"),
    ] {
        assert_eq!(
            rounded(op, "DATE", Some(input), unit).unwrap(),
            json!({"type":"date", "value":expected})
        );
    }
    for (op, input, unit) in [
        ("FLOOR", "0001-01-01", "MILLENNIUM"),
        ("DATE_TRUNC", "0001-01-01", "WEEK"),
        ("CEIL", "9999-12-31", "MONTH"),
    ] {
        assert!(matches!(
            rounded(op, "DATE", Some(input), unit),
            Err(Refusal::RuntimeError(_))
        ));
    }
    assert_eq!(
        rounded(
            "CEIL",
            "TIMESTAMP",
            Some("1969-12-31 23:59:59.999999"),
            "MILLISECOND"
        )
        .unwrap(),
        json!({"type":"timestamp", "value":"1970-01-01 00:00:00"})
    );
    assert_eq!(
        rounded(
            "FLOOR",
            "TIMESTAMP",
            Some("1969-12-31 23:59:59.999999"),
            "MILLISECOND"
        )
        .unwrap(),
        json!({"type":"timestamp", "value":"1969-12-31 23:59:59.999"})
    );
    assert_eq!(
        rounded("FLOOR", "DATE", None, "YEAR").unwrap(),
        json!({"type":"null"})
    );
    assert!(matches!(
        rounded("FLOOR", "DATE", None, "BOGUS"),
        Err(Refusal::Unsupported(_))
    ));
    assert!(matches!(
        rounded("DATE_TRUNC", "DATE", None, "HOUR"),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn unsigned_casts_preserve_bounds_and_runtime_string_rules() {
    for (name, maximum) in [
        ("UTINYINT", u8::MAX as u64),
        ("USMALLINT", u16::MAX as u64),
        ("UINTEGER", u32::MAX as u64),
        ("UBIGINT", u64::MAX),
    ] {
        let max = maximum.to_string();
        let cast = |text: &str| run(call("CAST", name, vec![literal("VARCHAR", Some(text))]));
        assert_eq!(cast(&max).unwrap(), json!({"type":"integer", "value":max}));
        assert_eq!(
            cast(" \t+0\n").unwrap(),
            json!({"type":"integer", "value":"0"})
        );
        for text in [
            "-0",
            "-1",
            "",
            "1.0",
            "\u{00a0}1",
            &(u128::from(maximum) + 1).to_string(),
        ] {
            assert!(
                matches!(cast(text), Err(Refusal::RuntimeError(_))),
                "{name}: {text}"
            );
        }
        let overflow = call(
            "CHECKED_PLUS",
            name,
            vec![literal(name, Some(&max)), literal(name, Some("1"))],
        );
        assert!(matches!(run(overflow), Err(Refusal::RuntimeError(_))));
        let underflow = call(
            "CHECKED_MINUS",
            name,
            vec![literal(name, Some("0")), literal(name, Some("1"))],
        );
        assert!(matches!(run(underflow), Err(Refusal::RuntimeError(_))));
        assert_eq!(
            run(call(
                "SAFE_CAST",
                name,
                vec![literal("INTEGER", Some("-1"))]
            ))
            .unwrap(),
            json!({"type":"null"})
        );
        assert_eq!(
            run(call("CAST", name, vec![literal("BOOLEAN", Some("true"))])).unwrap(),
            json!({"type":"integer", "value":"1"})
        );
    }
}

#[test]
fn math_domains_preserve_runtime_errors_nan_and_signed_zero() {
    let function = |name: &str, args| {
        let mut expression = call("OTHER_FUNCTION", "DOUBLE", args);
        expression["operator"] = json!(name);
        run(expression)
    };
    for input in ["-1", "-Infinity"] {
        assert!(matches!(
            function("LN", vec![literal("DOUBLE", Some(input))]),
            Err(Refusal::RuntimeError(_))
        ));
    }
    assert_eq!(
        function("SIN", vec![literal("DOUBLE", Some("-0"))]).unwrap()["value"],
        "0000000000000000"
    );
    assert_eq!(
        function("LN", vec![literal("DOUBLE", Some("-0"))]).unwrap()["value"],
        "fff0000000000000"
    );
    assert_eq!(
        function("LN", vec![literal("DOUBLE", None)]).unwrap()["type"],
        "null"
    );
    assert_eq!(
        function("IS_NAN", vec![literal("DOUBLE", Some("NaN"))]).unwrap()["value"],
        true
    );
    assert_eq!(
        function(
            "POWER",
            vec![literal("DOUBLE", Some("1")), literal("DOUBLE", Some("NaN"))]
        )
        .unwrap()["value"],
        "7ff8000000000000"
    );
}

#[test]
fn extract_preserves_calendar_boundaries_and_negative_duration_parts() {
    for (unit, name, value, expected) in [
        ("CENTURY", "DATE", "1900-12-31", "19"),
        ("CENTURY", "DATE", "1901-01-01", "20"),
        ("ISOYEAR", "DATE", "2021-01-01", "2020"),
        ("WEEK", "DATE", "2021-01-01", "53"),
        ("DOW", "DATE", "2024-03-03", "1"),
        ("EPOCH", "TIMESTAMP", "1969-12-31 23:59:59.5", "0"),
        ("SECOND", "INTERVAL_SECOND", "-61000", "-1"),
        ("MONTH", "INTERVAL_MONTH", "-13", "-1"),
    ] {
        assert_eq!(
            run(call(
                "EXTRACT",
                "BIGINT",
                vec![literal("SYMBOL", Some(unit)), literal(name, Some(value))]
            ))
            .unwrap()["value"],
            expected
        );
    }
    assert!(matches!(
        run(call(
            "EXTRACT",
            "BIGINT",
            vec![literal("SYMBOL", Some("UNKNOWN")), literal("DATE", None)]
        )),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn split_trim_and_concat_preserve_empty_strings_and_nullable_elements() {
    let function = |name: &str, arguments| {
        let mut expression = call("OTHER_FUNCTION", "VARCHAR", arguments);
        expression["operator"] = json!(name);
        run(expression)
    };
    let string = |s| literal("VARCHAR", Some(s));
    assert_eq!(
        function("SPLIT", vec![string(""), string("|")]).unwrap()["value"],
        json!([])
    );
    assert_eq!(
        function("SPLIT", vec![string(""), string("")]).unwrap()["value"],
        json!([{"type":"string", "value":""}])
    );
    assert_eq!(
        function("SPLIT", vec![string("|a|"), string("|")]).unwrap()["value"],
        json!([{"type":"string", "value":""},{"type":"string", "value":"a"},{"type":"string", "value":""}])
    );
    assert_eq!(
        function(
            "CONCAT_WS",
            vec![
                string("|"),
                literal("VARCHAR", None),
                string("a"),
                string("")
            ]
        )
        .unwrap()["value"],
        "a|"
    );
    assert_eq!(
        function("CONCAT_WS", vec![string("|"), literal("VARCHAR", None)]).unwrap()["value"],
        ""
    );
    assert_eq!(
        function("CONCAT_WS", vec![literal("VARCHAR", None), string("a")]).unwrap()["type"],
        "null"
    );
    assert_eq!(
        run(call(
            "TRIM",
            "VARCHAR",
            vec![
                literal("SYMBOL", Some("BOTH")),
                string("x"),
                string("xxéxx")
            ]
        ))
        .unwrap()["value"],
        "é"
    );
    assert!(matches!(
        function("SPLIT", vec![string(&"|".repeat(4096)), string("|")]),
        Err(Refusal::Budget)
    ));
}

#[test]
fn like_handles_wildcards_escapes_and_bounded_work() {
    let pattern = |name: &str, text: &str, pattern: &str, escape: Option<&str>| {
        let mut args = vec![
            literal("VARCHAR", Some(text)),
            literal("VARCHAR", Some(pattern)),
        ];
        if let Some(escape) = escape {
            args.push(literal("VARCHAR", Some(escape)));
        }
        let mut expression = call("LIKE", "BOOLEAN", args);
        expression["operator"] = json!(name);
        run(expression)
    };
    for (text, expression, escape, expected) in [
        ("", "%", None, true),
        ("", "_", None, false),
        ("foo", "%_", None, true),
        ("foo", "f%oo_", None, false),
        ("éclair", "_clair", None, true),
        ("a%b", "a!%b", Some("!"), true),
        ("a_b", "a!_b", Some("!"), true),
        ("a!b", "a!!b", Some("!"), true),
        ("a.b", "a.b", None, true),
        ("axb", "a.b", None, false),
    ] {
        assert_eq!(
            pattern("LIKE", text, expression, escape).unwrap()["value"],
            expected
        );
    }
    assert_eq!(
        pattern("ILIKE", "Hawkeye", "h%", None).unwrap()["value"],
        true
    );
    for (text, expression) in [("É", "é"), ("\n", "_"), ("😀", "_")] {
        assert!(matches!(
            pattern("ILIKE", text, expression, None),
            Err(Refusal::Unsupported(_))
        ));
    }
    for (expression, escape) in [("abc!", "!"), ("!x", "!"), ("abc", ""), ("abc", "ab")] {
        assert!(matches!(
            pattern("LIKE", "abc", expression, Some(escape)),
            Err(Refusal::RuntimeError(_))
        ));
    }
    assert!(matches!(
        pattern("LIKE", &"a".repeat(1000), &"%a".repeat(1000), None),
        Err(Refusal::Budget)
    ));
}

#[test]
fn numeric_rounding_preserves_ties_and_extreme_positions() {
    let round = |name: &str, text: &str, digits: &str| {
        let mut expression = call(
            "OTHER_FUNCTION",
            "DECIMAL",
            vec![
                literal("DECIMAL", Some(text)),
                literal("INTEGER", Some(digits)),
            ],
        );
        expression["operator"] = json!(name);
        expression["type"] = json!({"name":"DECIMAL", "precision":12, "scale":4});
        run(expression)
    };
    for (name, text, digits, expected) in [
        ("ROUND", "1.25", "1", "1.3"),
        ("ROUND", "-1.25", "1", "-1.3"),
        ("ROUND", "250", "-2", "300"),
        ("TRUNCATE", "-1.29", "1", "-1.2"),
        ("TRUNCATE", "250", "-2", "200"),
        ("ROUND", "1.25", "2147483647", "1.25"),
        ("ROUND", "1.25", "-2147483648", "0"),
    ] {
        assert_eq!(round(name, text, digits).unwrap()["value"], expected);
    }
    for (name, expected) in [("FLOOR", "-2"), ("CEIL", "-1")] {
        let mut expression = call(name, "DECIMAL", vec![literal("DECIMAL", Some("-1.25"))]);
        expression["type"] = json!({"name":"DECIMAL", "precision":12, "scale":0});
        assert_eq!(run(expression).unwrap()["value"], expected);
    }
}

#[test]
fn comparisons_preserve_null_nan_and_signed_zero_semantics() {
    for (kind, left, right, expected) in [
        ("LESS_THAN", "-1", "0", true),
        ("LESS_THAN_OR_EQUAL", "0", "-0", true),
        ("GREATER_THAN", "NaN", "Infinity", true),
        ("GREATER_THAN_OR_EQUAL", "NaN", "NaN", true),
        ("LESS_THAN", "NaN", "NaN", false),
        ("IS_NOT_DISTINCT_FROM", "NaN", "NaN", true),
    ] {
        assert_eq!(
            run(call(
                kind,
                "BOOLEAN",
                vec![
                    literal("DOUBLE", Some(left)),
                    literal("DOUBLE", Some(right))
                ]
            ))
            .unwrap()["value"],
            expected
        );
    }
    for (kind, expected) in [("IS_DISTINCT_FROM", false), ("IS_NOT_DISTINCT_FROM", true)] {
        assert_eq!(
            run(call(
                kind,
                "BOOLEAN",
                vec![literal("INTEGER", None), literal("INTEGER", None)]
            ))
            .unwrap()["value"],
            expected
        );
    }
    assert_eq!(
        run(call(
            "LESS_THAN",
            "BOOLEAN",
            vec![literal("INTEGER", None), literal("INTEGER", Some("1"))]
        ))
        .unwrap()["type"],
        "null"
    );
    for kind in ["IS_TRUE", "IS_FALSE", "IS_NOT_TRUE", "IS_NOT_FALSE"] {
        assert_eq!(
            run(call(kind, "BOOLEAN", vec![literal("BOOLEAN", None)])).unwrap()["value"],
            kind.starts_with("IS_NOT_")
        );
    }
}

#[test]
fn string_slices_bound_work_and_preserve_sql_indices() {
    let text = literal("VARCHAR", Some("éclair"));
    let number = |n: &str| literal("BIGINT", Some(n));
    let function = |name: &str, args| {
        let mut expression = call("OTHER_FUNCTION", "VARCHAR", args);
        expression["operator"] = json!(name);
        run(expression)
    };
    for (name, args, expected) in [
        (
            "SUBSTRING",
            vec![text.clone(), number("0"), number("3")],
            "éc",
        ),
        (
            "SUBSTRING",
            vec![text.clone(), number("2"), number("9223372036854775807")],
            "clair",
        ),
        (
            "SUBSTRING",
            vec![text.clone(), number("9223372036854775807")],
            "",
        ),
        ("LEFT", vec![text.clone(), number("2")], "éc"),
        ("RIGHT", vec![text.clone(), number("2")], "ir"),
        ("RIGHT", vec![text.clone(), number("-1")], ""),
        ("REVERSE", vec![text.clone()], "rialcé"),
        (
            "REPEAT",
            vec![literal("VARCHAR", Some("")), number("9223372036854775807")],
            "",
        ),
    ] {
        assert_eq!(function(name, args).unwrap()["value"], expected);
    }
    assert!(matches!(
        function("REPEAT", vec![text.clone(), number("9223372036854775807")]),
        Err(Refusal::Budget)
    ));
    assert!(matches!(
        function("SUBSTRING", vec![text, number("1"), number("-1")]),
        Err(Refusal::Unsupported(_))
    ));
    assert!(matches!(
        function("LEFT", vec![literal("VARCHAR", Some("🦀")), number("1")]),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn array_folds_preserve_null_elements_order_and_growth_limits() {
    let null = literal("INTEGER", None);
    let one = literal("INTEGER", Some("1"));
    let array = |items| call("ARRAY_VALUE_CONSTRUCTOR", "ARRAY", items);
    let value = run(call(
        "ARRAY_PREPEND",
        "ARRAY",
        vec![array(vec![one.clone()]), null.clone()],
    ))
    .unwrap();
    assert_eq!(value["value"][0]["type"], "null");
    assert_eq!(value["value"][1]["value"], "1");
    let value = run(call(
        "ARRAY_REPEAT",
        "ARRAY",
        vec![null.clone(), literal("INTEGER", Some("2"))],
    ))
    .unwrap();
    assert_eq!(value["value"].as_array().unwrap().len(), 2);
    assert_eq!(value["value"][0]["type"], "null");
    let value = run(call(
        "ARRAY_DISTINCT",
        "ARRAY",
        vec![array(vec![null.clone(), one.clone(), null, one.clone()])],
    ))
    .unwrap();
    assert_eq!(value["value"].as_array().unwrap().len(), 2);
    assert_eq!(value["value"][0]["type"], "null");
    assert_eq!(value["value"][1]["value"], "1");
    assert_eq!(
        run(call(
            "ARRAY_MIN",
            "INTEGER",
            vec![array(vec![literal("INTEGER", None), one.clone()])]
        ))
        .unwrap()["value"],
        "1"
    );
    assert!(matches!(
        run(call(
            "ARRAY_REPEAT",
            "ARRAY",
            vec![one.clone(), literal("INTEGER", Some("2147483647"))]
        )),
        Err(Refusal::Budget)
    ));
    let nested = call(
        "ARRAY_REPEAT",
        "ARRAY",
        vec![
            array(vec![literal("VARCHAR", Some(&"x".repeat(1024)))]),
            literal("INTEGER", Some("4096")),
        ],
    );
    assert!(matches!(run(nested), Err(Refusal::Budget)));
    let mut element = call(
        "OTHER_FUNCTION",
        "INTEGER",
        vec![array(vec![one.clone(), one])],
    );
    element["operator"] = json!("ELEMENT");
    assert!(matches!(run(element), Err(Refusal::RuntimeError(_))));
}

#[test]
fn lazy_branches_do_not_evaluate_runtime_errors() {
    let answer = literal("INTEGER", Some("7"));
    assert_eq!(
        run(call(
            "CASE",
            "INTEGER",
            vec![
                literal("BOOLEAN", Some("false")),
                division_by_zero(),
                answer.clone()
            ]
        ))
        .unwrap()["value"],
        "7"
    );
    assert_eq!(
        run(call(
            "COALESCE",
            "INTEGER",
            vec![literal("INTEGER", None), answer, division_by_zero()]
        ))
        .unwrap()["value"],
        "7"
    );
    assert!(matches!(
        run(division_by_zero()),
        Err(Refusal::RuntimeError(_))
    ));
}

#[test]
fn unknown_calls_with_null_are_refused_before_null_propagation() {
    assert!(matches!(
        run(call(
            "OTHER_FUNCTION",
            "INTEGER",
            vec![literal("INTEGER", None)]
        )),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn user_functions_and_dynamic_calls_are_never_executed() {
    let mut expression = call("OTHER_FUNCTION", "DOUBLE", vec![]);
    expression["operator"] = json!("PI");
    expression["operator_class"] = json!("SqlUserDefinedFunction");
    assert!(matches!(
        run(expression.clone()),
        Err(Refusal::Unsupported(_))
    ));
    expression["operator_class"] = json!("SqlBasicFunction");
    expression["dynamic"] = json!(true);
    assert!(matches!(
        run(expression.clone()),
        Err(Refusal::Unsupported(_))
    ));
    expression["dynamic"] = json!(false);
    expression["deterministic"] = json!(false);
    assert!(matches!(run(expression), Err(Refusal::Unsupported(_))));
}

#[test]
fn boolean_operators_preserve_three_valued_logic() {
    let null = literal("BOOLEAN", None);
    assert_eq!(
        run(call(
            "AND",
            "BOOLEAN",
            vec![null.clone(), literal("BOOLEAN", Some("false"))]
        ))
        .unwrap()["value"],
        false
    );
    assert_eq!(
        run(call(
            "OR",
            "BOOLEAN",
            vec![null.clone(), literal("BOOLEAN", Some("false"))]
        ))
        .unwrap()["type"],
        "null"
    );
    assert_eq!(
        run(call(
            "OR",
            "BOOLEAN",
            vec![null, literal("BOOLEAN", Some("true"))]
        ))
        .unwrap()["value"],
        true
    );
    assert!(matches!(
        run(call(
            "CASE",
            "INTEGER",
            vec![
                literal("INTEGER", Some("1")),
                literal("INTEGER", Some("2")),
                literal("INTEGER", Some("3"))
            ]
        )),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn overflow_and_safe_cast_are_distinct_from_unsupported() {
    assert!(matches!(
        run(call(
            "CHECKED_PLUS",
            "INTEGER",
            vec![
                literal("INTEGER", Some("2147483647")),
                literal("INTEGER", Some("1"))
            ]
        )),
        Err(Refusal::RuntimeError(_))
    ));
    assert_eq!(
        run(call(
            "SAFE_CAST",
            "TINYINT",
            vec![literal("INTEGER", Some("128"))]
        ))
        .unwrap()["type"],
        "null"
    );
    assert!(matches!(
        run(call(
            "SAFE_CAST",
            "ARRAY",
            vec![literal("INTEGER", Some("1"))]
        )),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn runtime_decimal_cast_truncates_instead_of_rounding() {
    for (input, expected) in [
        ("12.345", "12.34"),
        ("-12.345", "-12.34"),
        ("99.999", "99.99"),
    ] {
        let mut expression = call("CAST", "DECIMAL", vec![literal("DECIMAL", Some(input))]);
        expression["type"] = json!({"name": "DECIMAL", "precision": 4, "scale": 2});
        assert_eq!(run(expression).unwrap()["value"], expected);
    }
    let mut expression = call("CAST", "DECIMAL", vec![literal("DECIMAL", Some("100.00"))]);
    expression["type"] = json!({"name": "DECIMAL", "precision": 4, "scale": 2});
    assert!(matches!(run(expression), Err(Refusal::RuntimeError(_))));
}

#[test]
fn runtime_nan_equality_and_signed_zero_are_preserved() {
    let nested = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![call(
            "ARRAY_VALUE_CONSTRUCTOR",
            "ARRAY",
            vec![literal("DOUBLE", Some("NaN")), literal("DOUBLE", None)],
        )],
    );
    assert_eq!(
        run(call("EQUALS", "BOOLEAN", vec![nested.clone(), nested])).unwrap()["value"],
        true
    );
    assert_eq!(
        run(call(
            "EQUALS",
            "BOOLEAN",
            vec![
                literal("DOUBLE", Some("NaN")),
                literal("DOUBLE", Some("NaN"))
            ]
        ))
        .unwrap()["value"],
        true
    );
    assert_eq!(
        run(literal("DOUBLE", Some("-0.0"))).unwrap()["value"],
        "8000000000000000"
    );
}

#[test]
fn temporal_casts_follow_runtime_trim_range_and_leap_second_rules() {
    for (name, input, expected) in [
        ("DATE", "\t2024-02-29\n", "2024-02-29"),
        ("TIME", " 12:34:56.123456789 ", "12:34:56.123456789"),
        ("TIME", "23:59:60", "23:59:59.999999999"),
        ("TIMESTAMP", " 2024-02-29 ", "2024-02-29 00:00:00"),
        (
            "TIMESTAMP",
            "2024-02-29 23:59:60",
            "2024-02-29 23:59:59.999999",
        ),
        (
            "TIMESTAMP",
            "2024-02-29 00:00:00.123456789",
            "2024-02-29 00:00:00.123457",
        ),
    ] {
        assert_eq!(
            run(call("CAST", name, vec![literal("VARCHAR", Some(input))])).unwrap()["value"],
            expected
        );
    }
    for input in ["0000-01-01", "+10000-01-01", "2024-02-30"] {
        assert!(matches!(
            run(call("CAST", "DATE", vec![literal("VARCHAR", Some(input))])),
            Err(Refusal::RuntimeError(_))
        ));
    }
}

#[test]
fn calendar_months_clamp_and_date_intervals_truncate_toward_zero() {
    assert_eq!(
        run(call(
            "PLUS",
            "DATE",
            vec![
                literal("DATE", Some("2024-01-31")),
                literal("INTERVAL_MONTH", Some("1"))
            ]
        ))
        .unwrap()["value"],
        "2024-02-29"
    );
    assert_eq!(
        run(call(
            "PLUS",
            "DATE",
            vec![
                literal("DATE", Some("2024-01-31")),
                literal("INTERVAL_SECOND", Some("-86399999"))
            ]
        ))
        .unwrap()["value"],
        "2024-01-31"
    );
}

#[test]
fn binary_values_preserve_bytes_padding_and_unsigned_order() {
    let mut cast = call("CAST", "BINARY", vec![literal("VARCHAR", Some("é"))]);
    cast["type"]["precision"] = json!(4);
    assert_eq!(run(cast.clone()).unwrap()["value"], "c3a90000");
    cast["type"]["precision"] = json!(1);
    assert_eq!(run(cast.clone()).unwrap()["value"], "c3");
    cast["type"]["name"] = json!("VARBINARY");
    cast["type"]["precision"] = json!(4);
    assert_eq!(run(cast.clone()).unwrap()["value"], "c3a9");
    cast["type"]["precision"] = json!(i32::MAX);
    assert!(matches!(run(cast), Err(Refusal::Budget)));
    assert_eq!(
        run(call(
            "LESS_THAN",
            "BOOLEAN",
            vec![literal("BINARY", Some("7f")), literal("BINARY", Some("ff"))]
        ))
        .unwrap()["value"],
        true
    );
    for invalid in ["0", "zz", "00 ff", "🦀"] {
        assert!(matches!(
            run(literal("BINARY", Some(invalid))),
            Err(Refusal::Unsupported(_))
        ));
    }
}

#[test]
fn binary_slices_concatenation_and_overlay_preserve_sql_indices() {
    let function = |name: &str, args| {
        let mut e = call(
            if name == "||" {
                "OTHER"
            } else {
                "OTHER_FUNCTION"
            },
            "VARBINARY",
            args,
        );
        e["operator"] = json!(name);
        run(e)
    };
    assert_eq!(
        function(
            "SUBSTRING",
            vec![
                literal("BINARY", Some("00ff80")),
                literal("INTEGER", Some("0")),
                literal("INTEGER", Some("3"))
            ]
        )
        .unwrap()["value"],
        "00ff"
    );
    assert_eq!(
        function(
            "RIGHT",
            vec![
                literal("BINARY", Some("00ff80")),
                literal("INTEGER", Some("2"))
            ]
        )
        .unwrap()["value"],
        "ff80"
    );
    assert_eq!(
        function(
            "||",
            vec![
                literal("BINARY", Some("00ff")),
                literal("BINARY", Some("80"))
            ]
        )
        .unwrap()["value"],
        "00ff80"
    );
    assert_eq!(
        function(
            "||",
            vec![literal("BINARY", None), literal("BINARY", Some("80"))]
        )
        .unwrap()["type"],
        "null"
    );
    assert_eq!(
        function(
            "OVERLAY",
            vec![
                literal("BINARY", Some("00ff80")),
                literal("BINARY", Some("aabb")),
                literal("INTEGER", Some("2")),
                literal("INTEGER", Some("1"))
            ]
        )
        .unwrap()["value"],
        "00aabb80"
    );
    assert!(matches!(
        function(
            "SUBSTRING",
            vec![
                literal("BINARY", Some("00")),
                literal("INTEGER", Some("1")),
                literal("INTEGER", Some("-1"))
            ]
        ),
        Err(Refusal::Unsupported(_))
    ));
}

#[test]
fn interval_casts_truncate_toward_zero_without_crossing_families() {
    for (source, value, target, expected) in [
        ("INTERVAL_MONTH", "19", "INTERVAL_YEAR", "12"),
        ("INTERVAL_MONTH", "-19", "INTERVAL_YEAR", "-12"),
        (
            "INTERVAL_MONTH",
            "-2147483648",
            "INTERVAL_YEAR",
            "-2147483640",
        ),
        (
            "INTERVAL_SECOND",
            "90500.001",
            "INTERVAL_MINUTE",
            "60000000",
        ),
        (
            "INTERVAL_SECOND",
            "-90500.001",
            "INTERVAL_MINUTE",
            "-60000000",
        ),
        (
            "INTERVAL_SECOND",
            "1500.001",
            "INTERVAL_DAY_SECOND",
            "1500001",
        ),
        (
            "INTERVAL_SECOND",
            "-9223372036854775.808",
            "INTERVAL_SECOND",
            "-9223372036854775808",
        ),
    ] {
        assert_eq!(
            run(call("CAST", target, vec![literal(source, Some(value))])).unwrap()["value"],
            expected
        );
    }
    for (source, target) in [
        ("INTERVAL_MONTH", "INTERVAL_SECOND"),
        ("INTERVAL_SECOND", "INTERVAL_YEAR"),
        ("INTEGER", "INTERVAL_DAY"),
    ] {
        assert!(matches!(
            run(call("CAST", target, vec![literal(source, Some("1"))])),
            Err(Refusal::Unsupported(_))
        ));
    }
}

#[test]
fn interval_overflow_precedes_qualifier_truncation() {
    assert!(matches!(
        run(call(
            "CHECKED_TIMES",
            "INTERVAL_YEAR",
            vec![
                literal("INTERVAL_MONTH", Some("1073741824")),
                literal("INTEGER", Some("2"))
            ]
        )),
        Err(Refusal::RuntimeError(_))
    ));
}

#[test]
fn arrays_keep_nulls_except_for_explicit_compaction() {
    let array = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![literal("INTEGER", None), literal("INTEGER", Some("2"))],
    );
    assert_eq!(
        run(array.clone()).unwrap()["value"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        run(call("ARRAY_COMPACT", "ARRAY", vec![array.clone()])).unwrap()["value"],
        json!([{"type":"integer", "value":"2"}])
    );
    for index in ["0", "-1", "3"] {
        assert_eq!(
            run(call(
                "ITEM",
                "INTEGER",
                vec![array.clone(), literal("BIGINT", Some(index))]
            ))
            .unwrap()["type"],
            "null"
        );
    }
}

#[test]
fn allocation_and_traversal_budgets_refuse_before_work() {
    // Each REPEAT is individually legal and its input is small. Their aggregate
    // must exhaust the budget before evaluating the trailing runtime error.
    let mut repeated = call(
        "OTHER_FUNCTION",
        "VARCHAR",
        vec![
            literal("VARCHAR", Some(&"x".repeat(1024))),
            literal("INTEGER", Some("256")),
        ],
    );
    repeated["operator"] = json!("REPEAT");
    let mut arguments = vec![repeated; 5];
    arguments.push(division_by_zero());
    assert!(matches!(
        run(call("ARRAY_VALUE_CONSTRUCTOR", "ARRAY", arguments)),
        Err(Refusal::Budget)
    ));
    let mut huge = call("CAST", "CHAR", vec![literal("VARCHAR", Some("x"))]);
    huge["type"]["precision"] = json!(i32::MAX);
    assert!(matches!(run(huge), Err(Refusal::Budget)));
    let wide = call(
        "ARRAY_VALUE_CONSTRUCTOR",
        "ARRAY",
        vec![literal("INTEGER", Some("1")); 4096],
    );
    assert!(matches!(run(wide), Err(Refusal::Budget)));
    let mut deep = literal("BOOLEAN", Some("true"));
    for _ in 0..66 {
        deep = call("NOT", "BOOLEAN", vec![deep]);
    }
    assert!(matches!(run(deep), Err(Refusal::Budget)));
}

#[test]
fn uuid_casts_preserve_grammar_safe_errors_and_unsigned_order() {
    let canonical = "123e4567-e89b-12d3-a456-426655440000";
    for text in [
        canonical,
        "123e4567e89b12d3a456426655440000",
        "{123E-4567-E89B-12D3-A456-4266-5544-0000}",
    ] {
        let expression = call("CAST", "UUID", vec![literal("VARCHAR", Some(text))]);
        assert_eq!(
            run(expression.clone()).unwrap(),
            json!({"type":"uuid", "value":canonical})
        );
        assert_eq!(
            run(call("CAST", "VARCHAR", vec![expression])).unwrap()["value"],
            canonical
        );
    }
    for text in [
        "",
        " 123e4567e89b12d3a456426655440000",
        "123e4567e89b12d3a456426655440000-",
        "urn:uuid:123e4567-e89b-12d3-a456-426655440000",
        "123e--4567e89b12d3a456426655440000",
        "123-e4567e89b12d3a456426655440000",
    ] {
        assert!(matches!(
            run(call("CAST", "UUID", vec![literal("VARCHAR", Some(text))])),
            Err(Refusal::RuntimeError(_))
        ));
        assert_eq!(
            run(call(
                "SAFE_CAST",
                "UUID",
                vec![literal("VARCHAR", Some(text))]
            ))
            .unwrap()["type"],
            "null"
        );
    }
    assert_eq!(
        run(call(
            "GREATER_THAN",
            "BOOLEAN",
            vec![
                literal("UUID", Some("ffffffff-ffff-ffff-ffff-ffffffffffff")),
                literal("UUID", Some("00000000-0000-0000-0000-000000000000"))
            ]
        ))
        .unwrap()["value"],
        true
    );
}

#[test]
fn overlay_preserves_character_offsets_and_refuses_incompatible_ranges() {
    let overlay = |source: &str, replacement: &str, start: &str, remove: Option<&str>| {
        let mut args = vec![
            literal("VARCHAR", Some(source)),
            literal("VARCHAR", Some(replacement)),
            literal("INTEGER", Some(start)),
        ];
        if let Some(remove) = remove {
            args.push(literal("INTEGER", Some(remove)));
        }
        let mut expression = call("OTHER_FUNCTION", "VARCHAR", args);
        expression["operator"] = json!("OVERLAY");
        run(expression)
    };
    assert_eq!(
        overlay("abcdef", "45", "4", None).unwrap()["value"],
        "abc45f"
    );
    assert_eq!(
        overlay("éclair", "X", "2", Some("0")).unwrap()["value"],
        "éXclair"
    );
    assert_eq!(overlay("", "X", "1", Some("0")).unwrap()["value"], "X");
    for (source, start, remove) in [
        ("abc", "0", "1"),
        ("abc", "2", "-1"),
        ("abc", "5", "0"),
        ("abc", "2", "3"),
        ("😀x", "1", "1"),
    ] {
        assert!(matches!(
            overlay(source, "X", start, Some(remove)),
            Err(Refusal::Unsupported(_))
        ));
    }
}

#[test]
fn legacy_trigonometry_preserves_the_java_fold_bits() {
    use feldera_fold_probe::{SemanticsProfile, trigonometric_kernel};
    for (name, angle, expected) in [
        ("SIN", -47684.6543254, 13830554026030611534_u64),
        ("COS", 0.772086721204076, 4604628485705084684),
        ("COS", 1.2345678901234568e37, 4605872953008218190),
        ("SIN", -8.03745686315932e26, 0x3fe77a34e4f564a5),
        ("COS", -45221668420832.195, 0x3fe8da3d4ac6f3e2),
    ] {
        let kernel = trigonometric_kernel(name, SemanticsProfile::LegacyJanino).unwrap();
        assert_eq!(kernel(angle).to_bits(), expected, "{name}({angle})");
    }
}
