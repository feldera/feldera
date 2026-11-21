use feldera_sql_rel_model::{
    ExpressionLimits, LiteralLimits, ValidationLimits,
    expressions::{BindingId, ExprId, ExprKind, ExpressionTable, ScopeId},
};
use serde_json::{Value, json};

// Two equal integer types remain distinct arena entries. The local input and
// outer correlation deliberately have the same row type, so type checks alone
// cannot detect a binding substitution.
fn fixture() -> Value {
    json!({
        "schema_version":"expressions-v4",
        "type_table": {
            "schema_version":"types-v0", "sources":[], "roots":[],
            "types":[
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i64","signed":true}},
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[{"name":{"name":"X","quoted":true,"origin":{"kind":"absent"}},"ty":0}]}},
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"function","parameters":[0],"result":0}},
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i64","signed":true}}
            ]
        },
        "operator_objects":[{"kind":"builtin","registry":"feldera-test","registry_version":"1","key":"integer.add"}],
        "operators":[{"object":0,"parameters":[0,0],"result":0}],
        "bindings":[{"kind":"input","ty":1},{"kind":"correlation","ty":1},{"kind":"lambda_parameter","ty":0}],
        "scopes":[{"parent":null,"bindings":[0,1]},{"parent":0,"bindings":[2]}],
        "expressions":[
            {"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"input_ref","binding":0,"field":0}},
            {"ty":1,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"correlation","binding":1}},
            {"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"field_access","input":1,"field":0}},
            {"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"literal","literal":{"ty":0,"origin":{"kind":"absent"},"value":{"kind":"integer","value":"9223372036854775807"}}}},
            {"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"call","operator":0,"arguments":[0,3]}},
            {"ty":0,"scope":1,"origin":{"kind":"absent"},"kind":{"kind":"lambda_ref","binding":2}},
            {"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"lambda","parameters":[{"declaration":5,"binding":2,"name":"item","index":0}],"body_scope":1,"body":5}}
        ],
        "roots":[4,6,4]
    })
}

fn decode(value: Value) -> ExpressionTable {
    serde_json::from_value(value).unwrap()
}

fn function_fixture() -> Value {
    let mut value = fixture();
    value["operator_objects"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "kind":"function","name":"f","return_type":0,
            "parameters":[{"name":"X","index":0,"ty":0}],"generated":false,
            "origin":{"kind":"absent"},
            "diagnostic":{"origin":{"kind":"absent"},"text":"f","internal_text":"f","empty":false},
            "body":{"kind":"sql","scope":2,"input":3,"root":7,"source":{"kind":"inherited"}}
        }));
    value["scopes"]
        .as_array_mut()
        .unwrap()
        .push(json!({"parent":null,"bindings":[3]}));
    value["bindings"]
        .as_array_mut()
        .unwrap()
        .push(json!({"kind":"input","ty":1}));
    value["operators"]
        .as_array_mut()
        .unwrap()
        .push(json!({"object":1,"parameters":[0],"result":0}));
    value["expressions"].as_array_mut().unwrap().extend([
        json!({"ty":0,"scope":2,"origin":{"kind":"absent"},"kind":{"kind":"input_ref","binding":3,"field":0}}),
        json!({"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"call","operator":1,"arguments":[0]}}),
        json!({"ty":0,"scope":0,"origin":{"kind":"absent"},"kind":{"kind":"call","operator":1,"arguments":[0]}}),
    ]);
    value["roots"] = json!([8, 9]);
    value
}

#[test]
fn function_bodies_share_the_expression_arena_with_isolated_parameters() {
    let table = decode(function_fixture());
    check(&table).unwrap();
    let encoded = serde_json::to_vec(&table).unwrap();
    assert_eq!(
        serde_json::from_slice::<ExpressionTable>(&encoded).unwrap(),
        table
    );
}

#[test]
fn cycles_across_operator_and_expression_arenas_are_rejected() {
    let mut value = function_fixture();
    value["operator_objects"][1]["body"]["root"] = json!(8);
    assert!(
        check(&decode(value))
            .unwrap_err()
            .message
            .contains("cyclic")
    );
}

#[test]
fn function_parameter_scopes_cannot_capture_callers_or_change_declarations() {
    for (pointer, replacement) in [
        ("/expressions/7/kind/binding", json!(0)),
        ("/operator_objects/1/body/scope", json!(0)),
        ("/operator_objects/1/body/input", json!(99)),
        ("/operator_objects/1/body/root", json!(99)),
        ("/operator_objects/1/parameters/0/name", json!("other")),
        ("/operator_objects/1/parameters/0/ty", json!(3)),
        ("/scopes/2/parent", json!(0)),
        ("/roots", json!([7])),
    ] {
        let mut value = function_fixture();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert!(check(&decode(value)).is_err(), "{pointer}");
    }
    let mut value = function_fixture();
    let function = value["operator_objects"][1].clone();
    value["operator_objects"]
        .as_array_mut()
        .unwrap()
        .push(function);
    assert!(check(&decode(value)).is_err());
}

#[test]
fn window_aggregate_metadata_survives_without_becoming_scope_independent() {
    let mut value = fixture();
    value["expressions"][4]["kind"] = json!({
        "kind":"window_aggregate","operator":0,"arguments":[3,3],
        "ordinal":7,"distinct":true,"ignore_nulls":true
    });
    check(&decode(value.clone())).unwrap();
    let table = decode(value.clone());
    assert_eq!(serde_json::to_value(table).unwrap(), value);
    // Even a window aggregate with constant arguments depends on its group.
    value["expressions"][6]["kind"]["body"] = json!(4);
    assert!(check(&decode(value)).is_err());
}

#[test]
fn window_aggregate_fields_are_required_and_strictly_typed() {
    let mut value = fixture();
    value["expressions"][4]["kind"] = json!({
        "kind":"window_aggregate","operator":0,"arguments":[0,3],
        "ordinal":7,"distinct":false,"ignore_nulls":false
    });
    for (field, replacement) in [
        ("ordinal", json!(-1)),
        ("distinct", json!("true")),
        ("ignore_nulls", Value::Null),
    ] {
        let mut invalid = value.clone();
        invalid["expressions"][4]["kind"][field] = replacement;
        assert!(serde_json::from_value::<ExpressionTable>(invalid).is_err());
    }
    value["expressions"][4]["kind"]
        .as_object_mut()
        .unwrap()
        .remove("ordinal");
    assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
}

#[test]
fn captured_function_source_must_reference_a_source_record() {
    let mut value = function_fixture();
    value["type_table"]["sources"] = json!([{"name":"generated function", "text":"SELECT X"}]);
    value["operator_objects"][1]["body"]["source"] = json!({"kind":"captured", "source":0});
    check(&decode(value.clone())).unwrap();
    value["operator_objects"][1]["body"]["source"]["source"] = json!(1);
    assert!(check(&decode(value)).is_err());
}

#[test]
fn external_bodies_and_diagnostics_are_explicit_not_sql_fallbacks() {
    let mut value = function_fixture();
    value["operator_objects"][1]["body"] = json!({"kind":"external"});
    check(&decode(value.clone())).unwrap();
    value["operator_objects"][1]["body"] = json!({"kind":"sql_text","text":"SELECT 1"});
    assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
}

fn constructor_fixture() -> Value {
    let mut value = fixture();
    let constructor = json!({
        "kind":"type_constructor", "declared_type":1,
        "name":{
            "names":["public","Pair"],
            "position":{"origin":{"kind":"absent"},"quoted":false},
            "components":{"kind":"present","positions":[
                {"origin":{"kind":"absent"},"quoted":false},
                {"origin":{"kind":"absent"},"quoted":true}
            ]}
        }
    });
    value["operator_objects"]
        .as_array_mut()
        .unwrap()
        .extend([constructor.clone(), constructor]);
    value["operators"].as_array_mut().unwrap().extend([
        json!({"object":1,"parameters":[0],"result":1}),
        json!({"object":2,"parameters":[0],"result":1}),
    ]);
    for signature in [1, 1, 2] {
        value["expressions"].as_array_mut().unwrap().push(json!({
            "ty":1,"scope":0,"origin":{"kind":"absent"},
            "kind":{"kind":"call","operator":signature,"arguments":[0]}
        }));
    }
    value["roots"] = json!([7, 8, 9, 7]);
    value
}

#[test]
fn constructor_objects_preserve_sharing_without_interning_equal_names() {
    let table = decode(constructor_fixture());
    check(&table).unwrap();
    assert_eq!(table.operator_objects[1], table.operator_objects[2]);
    assert_ne!(table.operators[1].object, table.operators[2].object);
    let wire = serde_json::to_vec(&table).unwrap();
    assert_eq!(
        serde_json::from_slice::<ExpressionTable>(&wire).unwrap(),
        table
    );
}

#[test]
fn malformed_constructor_declarations_and_builtin_duplicates_fail() {
    for (pointer, replacement) in [
        ("/operators/1/object", json!(99)),
        ("/operator_objects/1/declared_type", json!(99)),
        ("/operator_objects/1/name/names", json!([])),
        ("/operator_objects/1/name/names", json!([""])),
        ("/operator_objects/1/name/components/positions", json!([])),
        (
            "/operator_objects/1/name/position/origin",
            json!({"kind":"source","source":99,"start":{"line":1,"column":1},"end":{"line":1,"column":1}}),
        ),
    ] {
        let mut value = constructor_fixture();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert!(check(&decode(value)).is_err(), "{pointer}");
    }
    let mut value = fixture();
    let builtin = value["operator_objects"][0].clone();
    value["operator_objects"]
        .as_array_mut()
        .unwrap()
        .push(builtin);
    assert!(
        check(&decode(value))
            .unwrap_err()
            .message
            .contains("duplicate builtin")
    );
}

#[test]
fn constructor_name_metadata_is_strict_and_versioned() {
    for pointer in [
        "/operator_objects/1",
        "/operator_objects/1/name",
        "/operator_objects/1/name/position",
        "/operator_objects/1/name/components",
        "/operator_objects/1/name/components/positions/0",
    ] {
        let mut value = constructor_fixture();
        value.pointer_mut(pointer).unwrap()["class"] = json!("java.lang.Runtime");
        assert!(
            serde_json::from_value::<ExpressionTable>(value).is_err(),
            "{pointer}"
        );
    }
    let mut value = constructor_fixture();
    value["schema_version"] = json!("expressions-v1");
    assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
}

#[test]
fn constructor_name_bytes_participate_in_the_identifier_budget() {
    let table = decode(constructor_fixture());
    assert!(
        table
            .validate(
                ValidationLimits::default(),
                LiteralLimits::default(),
                ExpressionLimits {
                    identifier_bytes: 40,
                    ..Default::default()
                }
            )
            .is_err()
    );
}

#[test]
fn row_constructor_names_distinguish_absence_nulls_and_explicit_names() {
    let mut value = fixture();
    value["operator_objects"].as_array_mut().unwrap().extend([
        json!({"kind":"row_constructor","name":"ROW","fields":{"kind":"absent"}}),
        json!({"kind":"row_constructor","name":"ROW","fields":{"kind":"present","names":["left",null]}}),
    ]);
    let table = decode(value.clone());
    check(&table).unwrap();
    assert_ne!(table.operator_objects[1], table.operator_objects[2]);
    let wire = serde_json::to_vec(&table).unwrap();
    assert_eq!(
        serde_json::from_slice::<ExpressionTable>(&wire).unwrap(),
        table
    );
    value["operator_objects"][2]["fields"]["names"][1] = json!(17);
    assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
}

fn check(table: &ExpressionTable) -> Result<(), feldera_sql_rel_model::ValidationError> {
    table.validate(
        ValidationLimits::default(),
        LiteralLimits::default(),
        ExpressionLimits::default(),
    )
}

#[test]
fn all_seven_forms_preserve_order_sharing_and_binding_identity() {
    let table = decode(fixture());
    check(&table).unwrap();
    let wire = serde_json::to_string(&table).unwrap();
    assert_eq!(
        serde_json::from_str::<ExpressionTable>(&wire).unwrap(),
        table
    );
    assert_eq!(table.roots, vec![ExprId(4), ExprId(6), ExprId(4)]);
    assert_eq!(table.expressions.len(), 7);
}

#[test]
fn malformed_bindings_and_scope_substitutions_fail() {
    for (pointer, replacement) in [
        ("/expressions/0/kind/binding", json!(1)), // equal row, wrong binding kind
        ("/expressions/0/kind/binding", json!(99)),
        ("/expressions/0/scope", json!(1)), // outer input is not a correlation
        ("/expressions/5/scope", json!(0)), // parameter escapes its lambda
        ("/scopes/1/parent", json!(1)),     // even an otherwise unused scope cycle fails
        ("/scopes/1/bindings", json!([0, 2])),
        ("/scopes/0/bindings", json!([0])), // undeclared correlation
        ("/scopes/1/parent", json!(99)),
        ("/expressions/6/kind/parameters", json!([])),
        ("/expressions/6/kind/body_scope", json!(99)),
        ("/expressions/6/kind/body", json!(0)), // an input cannot escape into a lambda
    ] {
        let mut value = fixture();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            check(&decode(value)).is_err(),
            "accepted mutation at {pointer}"
        );
    }
}

#[test]
fn exact_types_and_resolved_signatures_are_checked() {
    for (pointer, replacement) in [
        ("/expressions/0/ty", json!(3)), // signature must still use the expression's exact ID
        ("/expressions/0/kind/field", json!(1)),
        ("/expressions/2/kind/input", json!(0)), // field access on scalar
        ("/expressions/4/kind/operator", json!(99)),
        ("/expressions/4/kind/arguments", json!([0])),
        ("/expressions/4/kind/arguments", json!([1, 3])),
        ("/operators/0/result", json!(3)),
        ("/operator_objects/0/key", json!("")),
        ("/operators/0/parameters/0", json!(99)),
        ("/expressions/3/kind/literal/ty", json!(3)),
        (
            "/expressions/3/kind/literal/value/value",
            json!("9223372036854775808"),
        ),
        ("/expressions/6/ty", json!(2)),
        ("/bindings/2/ty", json!(3)),
        (
            "/expressions/0/origin",
            json!({"kind":"source","source":0,"start":{"line":1,"column":1},"end":{"line":1,"column":1}}),
        ),
    ] {
        let mut value = fixture();
        *value.pointer_mut(pointer).unwrap() = replacement;
        assert!(
            check(&decode(value)).is_err(),
            "accepted mutation at {pointer}"
        );
    }
}

#[test]
fn field_references_keep_equal_objects_and_can_widen_outer_nullability() {
    let mut value = fixture();
    value["expressions"][0]["ty"] = json!(3);
    value["operators"][0]["parameters"][0] = json!(3);
    check(&decode(value.clone())).unwrap();
    value["type_table"]["types"][3]["nullable"] = json!(true);
    value["operators"][0]["result"] = json!(3);
    value["expressions"][4]["ty"] = json!(3);
    check(&decode(value.clone())).unwrap();
    value["type_table"]["types"][0]["nullable"] = json!(true);
    value["type_table"]["types"][3]["nullable"] = json!(false);
    assert!(check(&decode(value)).is_err());
}

#[test]
fn orphan_cycles_and_shared_subtree_depth_fail() {
    let mut table = decode(fixture());
    table.expressions[4].kind = ExprKind::Call {
        operator: feldera_sql_rel_model::expressions::OperatorId(0),
        arguments: vec![ExprId(4), ExprId(3)],
    };
    table.roots.clear();
    assert!(check(&table).unwrap_err().message.contains("cyclic"));

    let mut table = decode(fixture());
    // First visit the short branch, then a new chain that reaches that already
    // visited subtree. A DFS stack-length check alone misses the excessive depth.
    let mut child = ExprId(4);
    for _ in 0..4 {
        let mut expression = table.expressions[4].clone();
        expression.kind = ExprKind::Call {
            operator: feldera_sql_rel_model::expressions::OperatorId(0),
            arguments: vec![child, ExprId(0)],
        };
        table.expressions.push(expression);
        child = ExprId((table.expressions.len() - 1) as u32);
    }
    check(&table).unwrap();
    let error = table
        .validate(
            ValidationLimits::default(),
            LiteralLimits::default(),
            ExpressionLimits {
                depth: 4,
                ..Default::default()
            },
        )
        .unwrap_err();
    assert!(error.message.contains("depth"));
}

#[test]
fn lexical_capture_is_explicit_and_siblings_are_invisible() {
    let mut table = decode(fixture());
    // A lambda can reference an explicitly declared outer correlation, but it
    // cannot change that binding into an ordinary input.
    let mut captured = table.expressions[1].clone();
    captured.scope = ScopeId(1);
    table.expressions.push(captured);
    check(&table).unwrap();
    table
        .scopes
        .push(feldera_sql_rel_model::expressions::Scope {
            parent: Some(ScopeId(0)),
            bindings: vec![BindingId(3)],
        });
    table.bindings.push(table.bindings[1].clone());
    table.expressions[7].kind = ExprKind::Correlation {
        binding: BindingId(3),
    };
    assert!(check(&table).unwrap_err().message.contains("lexical"));
}

#[test]
fn shared_lambda_scopes_and_unowned_parameters_fail() {
    let mut table = decode(fixture());
    table.expressions.push(table.expressions[6].clone());
    assert!(
        check(&table)
            .unwrap_err()
            .message
            .contains("multiple owners")
    );
    table.expressions.truncate(6);
    table.roots = vec![ExprId(4)];
    assert!(
        check(&table)
            .unwrap_err()
            .message
            .contains("no owning lambda")
    );
}

#[test]
fn wire_rejects_unknown_missing_and_transient_forms() {
    for pointer in [
        "",
        "/operators/0",
        "/operator_objects/0",
        "/scopes/0",
        "/bindings/0",
        "/expressions/0",
        "/expressions/0/kind",
        "/expressions/6/kind/parameters/0",
    ] {
        let mut value = fixture();
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(true));
        assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
    }
    for pointer in [
        "/scopes/0/parent",
        "/expressions/6/kind/parameters/0/name",
        "/expressions/6/kind/parameters/0/declaration",
        "/operator_objects/0/key",
        "/operators/0/object",
        "/operator_objects",
        "/expressions/0/scope",
        "/expressions/0/origin",
    ] {
        let mut value = fixture();
        let (parent, key) = pointer.rsplit_once('/').unwrap();
        value
            .pointer_mut(parent)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove(key);
        assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
    }
    for kind in [
        "over",
        "sub_query",
        "local_ref",
        "dynamic_param",
        "range_ref",
        "sarg",
    ] {
        let mut value = fixture();
        value["expressions"][0]["kind"] = json!({"kind":kind});
        assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
    }
}

#[test]
fn every_resource_budget_is_enforced() {
    let table = decode(fixture());
    for limits in [
        ExpressionLimits {
            entries: 0,
            ..Default::default()
        },
        ExpressionLimits {
            references: 0,
            ..Default::default()
        },
        ExpressionLimits {
            depth: 0,
            ..Default::default()
        },
        ExpressionLimits {
            identifier_bytes: 0,
            ..Default::default()
        },
    ] {
        assert!(
            table
                .validate(
                    ValidationLimits::default(),
                    LiteralLimits::default(),
                    limits
                )
                .is_err()
        );
    }
}

#[test]
fn generated_schema_matches_the_draft_and_requires_explicit_parent() {
    let expected: Value = serde_json::from_str(include_str!("../expression-schema.json")).unwrap();
    assert_eq!(
        serde_json::to_value(schemars::schema_for!(ExpressionTable)).unwrap(),
        expected
    );
    assert!(
        expected["definitions"]["Scope"]["required"]
            .as_array()
            .unwrap()
            .contains(&json!("parent"))
    );
    let wire = serde_json::to_string(&fixture()).unwrap();
    let duplicate = wire.replacen(
        "\"registry\":",
        "\"registry\":\"duplicate\",\"registry\":",
        1,
    );
    assert!(serde_json::from_str::<ExpressionTable>(&duplicate).is_err());
}

#[test]
fn parameter_names_and_slots_are_preserved_without_retyping_them() {
    let mut value = fixture();
    value["expressions"][6]["kind"]["parameters"][0]["name"] = json!("Quoted λ item");
    value["expressions"][6]["kind"]["parameters"][0]["index"] = json!(17);
    let table = decode(value);
    check(&table).unwrap();
    let restored: ExpressionTable =
        serde_json::from_str(&serde_json::to_string(&table).unwrap()).unwrap();
    assert_eq!(restored, table);
}

#[test]
fn registry_membership_is_an_adapter_obligation() {
    let mut value = fixture();
    value["operator_objects"][0]["key"] = json!("unknown.to.the.adapter");
    // A structurally valid registry key is not evidence that the operator exists.
    check(&decode(value)).unwrap();
}

#[test]
fn root_scope_parent_is_required_and_nullable_in_both_contracts() {
    let schema = serde_json::to_value(schemars::schema_for!(ExpressionTable)).unwrap();
    assert_eq!(
        schema["definitions"]["Scope"]["properties"]["parent"]["type"],
        json!(["integer", "null"])
    );
    let mut value = fixture();
    value["scopes"][0].as_object_mut().unwrap().remove("parent");
    assert!(serde_json::from_value::<ExpressionTable>(value).is_err());
}

#[test]
fn lambda_declarations_retain_unused_and_distinct_reference_nodes() {
    let mut table = decode(fixture());
    table.expressions.push(table.expressions[5].clone());
    let ExprKind::Lambda { parameters, .. } = &mut table.expressions[6].kind else {
        unreachable!()
    };
    parameters[0].declaration = ExprId(7);
    check(&table).unwrap();
    let restored: ExpressionTable =
        serde_json::from_str(&serde_json::to_string(&table).unwrap()).unwrap();
    assert_eq!(restored, table);
    // Replacing a declaration with an ordinary value is not a binding definition.
    let ExprKind::Lambda { parameters, .. } = &mut table.expressions[6].kind else {
        unreachable!()
    };
    parameters[0].declaration = ExprId(3);
    assert!(check(&table).is_err());
}

#[test]
fn binding_independent_subgraphs_can_be_shared_across_scopes() {
    let mut table = decode(fixture());
    let ExprKind::Lambda { body, .. } = &mut table.expressions[6].kind else {
        unreachable!()
    };
    *body = ExprId(3); // Literal also occurs outside the lambda.
    check(&table).unwrap();
    let mut call = table.expressions[4].clone();
    let ExprKind::Call { arguments, .. } = &mut call.kind else {
        unreachable!()
    };
    *arguments = vec![ExprId(3), ExprId(3)];
    table.expressions.push(call);
    let ExprKind::Lambda { body, .. } = &mut table.expressions[6].kind else {
        unreachable!()
    };
    *body = ExprId(7);
    check(&table).unwrap();
    // This permits shared representation, not memoized evaluation or hoisting.
    let ExprKind::Lambda { body, .. } = &mut table.expressions[6].kind else {
        unreachable!()
    };
    *body = ExprId(4); // Reads an input binding from another evaluation scope.
    assert!(check(&table).is_err());
}
