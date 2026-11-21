use feldera_sql_rel_model::{
    LiteralLimits, ObjectLimits, ValidationLimits, expression_objects::ExpressionObjects,
    input_producers::InputProducers, input_uses::InputUses,
    relation_properties::RelationProperties,
};
use serde_json::{Value, json};

fn fixture() -> (Value, Value, Value, Value) {
    let inputs = json!({"schema_version":"input-uses-v1","relations":4,"functions":0,
        "type_table":{"schema_version":"types-v0","sources":[{"name":"first","text":"abc"},{"name":"second","text":"abc"}],"roots":[],"types":[
            {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i32","signed":true}},
            {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[
                {"name":{"name":"x","quoted":true,"origin":{"kind":"absent"}},"ty":0}]}},
            {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[]}},
            {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i64","signed":true}}]},
        "objects":[{"ty":0,"field":0}],"environments":[
            {"owner":{"kind":"relations","inputs":[2]},"row":1},
            {"owner":{"kind":"relations","inputs":[3]},"row":1},
            {"owner":{"kind":"relations","inputs":[]},"row":2}],
        "uses":[{"object":0,"environment":0},{"object":0,"environment":1}]});
    let producers = json!({"schema_version":"input-producers-v0","relations":[
        {"form":"LogicalProject","row":1,"inputs":[2],"constants":[]},
        {"form":"LogicalProject","row":1,"inputs":[3],"constants":[]},
        {"form":"LogicalValues","row":1,"inputs":[],"constants":[]},
        {"form":"LogicalValues","row":1,"inputs":[],"constants":[]}],"roots":[0,1],"function_rows":[]});
    let properties = json!({"schema_version":"relation-properties-v1","operators":{"kind":"owned","objects":[]},"relations":[
        {"metadata":{"traits":[],"hints":[]},"fields":{"kind":"project","expressions":1,"variables":[]}},
        {"metadata":{"traits":[],"hints":[]},"fields":{"kind":"project","expressions":1,"variables":[]}},
        {"metadata":{"traits":[],"hints":[]},"fields":{"kind":"values","tuples":0}},
        {"metadata":{"traits":[],"hints":[]},"fields":{"kind":"values","tuples":0}}]});
    let objects = json!({"schema_version":"expression-objects-v2",
        "operator_objects":[{"kind":"builtin","registry":"test","registry_version":"1","key":"identity"}],
        "operators":[{"object":0,"parameters":[0],"result":0}],
        "expressions":[
            {"ty":0,"position":{"kind":"absent"},"kind":{"kind":"input_ref","object":0}},
            {"ty":0,"position":{"kind":"present","internal":null,"position":{"line":1,"column":1,"end_line":1,"end_column":2,"quoted":false}},"kind":{"kind":"call","operator":0,"arguments":[0]}}],
        "environments":[
            {"input":0,"source":{"kind":"present","source":0},"correlations":[]},
            {"input":1,"source":{"kind":"present","source":1},"correlations":[]},
            {"input":2,"source":{"kind":"absent"},"correlations":[]}],
        "value_uses":[], "relation_uses":[
            {"relation":0,"environment":0,"roots":[1]},
            {"relation":1,"environment":1,"roots":[1]},
            {"relation":2,"environment":2,"roots":[]},
            {"relation":3,"environment":2,"roots":[]}]});
    (inputs, producers, properties, objects)
}

fn check(
    inputs: Value,
    producers: Value,
    properties: Value,
    objects: Value,
    limits: ObjectLimits,
) -> Result<(), String> {
    let inputs: InputUses = serde_json::from_value(inputs).map_err(|e| e.to_string())?;
    let producers: InputProducers = serde_json::from_value(producers).map_err(|e| e.to_string())?;
    let properties: RelationProperties =
        serde_json::from_value(properties).map_err(|e| e.to_string())?;
    let objects: ExpressionObjects = serde_json::from_value(objects).map_err(|e| e.to_string())?;
    objects
        .validate(
            &inputs,
            &producers,
            &properties,
            ValidationLimits::default(),
            LiteralLimits::default(),
            limits,
        )
        .map_err(|e| e.to_string())
}

#[test]
fn shared_calls_keep_one_identity_and_each_uses_source_context() {
    let (inputs, producers, properties, objects) = fixture();
    check(
        inputs.clone(),
        producers.clone(),
        properties.clone(),
        objects.clone(),
        ObjectLimits::default(),
    )
    .unwrap();
    let decoded: ExpressionObjects = serde_json::from_value(objects.clone()).unwrap();
    assert_eq!(decoded.expressions.len(), 2);
    assert_eq!(serde_json::to_value(decoded).unwrap(), objects);
    let mut invalid = objects;
    invalid["environments"][1]["source"] = json!({"kind":"absent"});
    assert!(
        check(
            inputs,
            producers,
            properties,
            invalid,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("source context")
    );
}

#[test]
fn states_and_repeated_edges_have_independent_work_limits() {
    let (inputs, producers, properties, objects) = fixture();
    check(
        inputs.clone(),
        producers.clone(),
        properties.clone(),
        objects.clone(),
        ObjectLimits {
            states: 4,
            ..ObjectLimits::default()
        },
    )
    .unwrap();
    assert!(
        check(
            inputs.clone(),
            producers.clone(),
            properties.clone(),
            objects.clone(),
            ObjectLimits {
                states: 3,
                ..ObjectLimits::default()
            }
        )
        .is_err()
    );
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits {
                work: 1,
                ..ObjectLimits::default()
            }
        )
        .is_err()
    );
}

#[test]
fn relation_fields_and_construction_cycles_are_checked() {
    for (path, replacement) in [
        ("/relation_uses/1/roots", json!([])),
        ("/relation_uses/1/environment", json!(0)),
        ("/relation_uses/0/roots/0", json!(99)),
        ("/expressions/1/kind/arguments", json!([1])),
        ("/operators/0/parameters", json!([3])),
        ("/operators/0/object", json!(9)),
    ] {
        let (inputs, producers, properties, mut objects) = fixture();
        *objects.pointer_mut(path).unwrap() = replacement;
        assert!(
            check(
                inputs,
                producers,
                properties,
                objects,
                ObjectLimits::default()
            )
            .is_err(),
            "accepted {path}"
        );
    }
}

#[test]
fn relation_slots_enforce_conditions_literals_and_window_calls() {
    let (mut inputs, mut producers, mut properties, mut objects) = fixture();
    inputs["type_table"]["types"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"boolean"}
        }));
    for index in 0..2 {
        producers["relations"][index]["form"] = json!("LogicalFilter");
        properties["relations"][index]["fields"] = json!({"kind":"filter","variables":[]});
    }
    objects["operators"][0]["result"] = json!(4);
    objects["expressions"][1]["ty"] = json!(4);
    check(
        inputs.clone(),
        producers.clone(),
        properties.clone(),
        objects.clone(),
        ObjectLimits::default(),
    )
    .unwrap();
    objects["operators"][0]["result"] = json!(0);
    objects["expressions"][1]["ty"] = json!(0);
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("boolean")
    );

    let (inputs, producers, mut properties, mut objects) = fixture();
    properties["relations"][2]["fields"]["tuples"] = json!(1);
    objects["relation_uses"][2]["roots"] = json!([1]);
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("VALUES slot")
    );

    let (inputs, producers, properties, mut objects) = fixture();
    objects["expressions"][1]["kind"] = json!({"kind":"window_aggregate","operator":0,"arguments":[0],
        "ordinal":0,"distinct":false,"ignore_nulls":false});
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("window owner")
    );
}

#[test]
fn shared_lambda_reference_is_resolved_in_each_lexical_frame() {
    let (inputs, producers, properties, mut objects) = fixture();
    let input = objects["expressions"][0].clone();
    let position = objects["expressions"][1]["position"].clone();
    objects["operators"][0]["parameters"] = json!([0, 0]);
    objects["expressions"] = json!([input,
        {"ty":0,"position":{"kind":"absent"},"kind":{"kind":"lambda_ref","index":0,"name":"x"}},
        {"ty":0,"position":position,"kind":{"kind":"call","operator":0,"arguments":[0,1]}},
        {"ty":0,"position":{"kind":"absent"},"kind":{"kind":"lambda","parameters":[1],"body":2}},
        {"ty":0,"position":{"kind":"absent"},"kind":{"kind":"lambda","parameters":[5],"body":2}},
        {"ty":0,"position":{"kind":"absent"},"kind":{"kind":"lambda_ref","index":0,"name":"x"}}]);
    objects["relation_uses"][0]["roots"] = json!([3]);
    objects["relation_uses"][1]["roots"] = json!([4]);
    check(
        inputs.clone(),
        producers.clone(),
        properties.clone(),
        objects.clone(),
        ObjectLimits::default(),
    )
    .unwrap();
    objects["expressions"][5]["ty"] = json!(3);
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("lambda reference type")
    );
}

#[test]
fn function_bodies_use_isolated_parameter_inputs_and_combined_cycle_checks() {
    let (mut inputs, mut producers, properties, mut objects) = fixture();
    inputs["functions"] = json!(1);
    inputs["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"owner":{"kind":"function","function":0},"row":1}));
    inputs["uses"]
        .as_array_mut()
        .unwrap()
        .push(json!({"object":0,"environment":3}));
    producers["function_rows"] = json!([1]);
    objects["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"input":3,"source":{"kind":"absent"},"correlations":[]}));
    objects["operator_objects"][0] = json!({"kind":"function","name":"identity","return_type":0,
        "parameters":[{"name":"x","index":0,"ty":0}],"generated":false,"origin":{"kind":"absent"},
        "diagnostic":{"origin":{"kind":"absent"},"text":"","internal_text":"","empty":true},
        "body":{"kind":"object_sql","environment":3,"root":0,"source":{"kind":"inherited"}}});
    check(
        inputs.clone(),
        producers.clone(),
        properties.clone(),
        objects.clone(),
        ObjectLimits::default(),
    )
    .unwrap();
    objects["operator_objects"][0]["body"]["root"] = json!(1);
    assert!(
        check(
            inputs,
            producers,
            properties,
            objects,
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("cyclic")
    );
}

#[test]
fn object_schema_matches_and_requires_context_fields() {
    let generated = serde_json::to_value(schemars::schema_for!(ExpressionObjects)).unwrap();
    let checked: Value =
        serde_json::from_str(include_str!("../expression-objects-schema.json")).unwrap();
    assert_eq!(generated, checked);
    let (_, _, _, mut objects) = fixture();
    objects["environments"][0]
        .as_object_mut()
        .unwrap()
        .remove("source");
    assert!(serde_json::from_value::<ExpressionObjects>(objects).is_err());
}

fn constant_fixture() -> (Value, Value, Value, Value) {
    let (mut inputs, mut producers, mut properties, mut objects) = fixture();
    inputs["relations"] = json!(0);
    inputs["objects"] = json!([]);
    inputs["uses"] = json!([]);
    inputs["environments"] = json!([{"owner":{"kind":"constant"},"row":2}]);
    producers["relations"] = json!([]);
    producers["roots"] = json!([]);
    properties["relations"] = json!([]);
    objects["expressions"][0]["kind"] = json!({"kind":"literal","literal":{
        "ty":0,"origin":{"kind":"absent"},"value":{"kind":"integer","value":"42"}}});
    objects["environments"] = json!([
        {"input":0,"source":{"kind":"present","source":0},"correlations":[]},
        {"input":0,"source":{"kind":"present","source":1},"correlations":[]}]);
    objects["relation_uses"] = json!([]);
    objects["value_uses"] = json!([
        {"environment":0,"roots":[1,0]}, {"environment":1,"roots":[1]}]);
    (inputs, producers, properties, objects)
}

#[test]
fn statement_values_share_objects_without_relational_inputs() {
    let (inputs, producers, properties, objects) = constant_fixture();
    check(
        inputs,
        producers,
        properties,
        objects.clone(),
        ObjectLimits::default(),
    )
    .unwrap();
    let restored: ExpressionObjects = serde_json::from_value(objects.clone()).unwrap();
    assert_eq!(serde_json::to_value(restored).unwrap(), objects);
}

#[test]
fn statement_values_reject_captured_rows_correlations_and_invalid_roots() {
    let (inputs, producers, properties, objects) = constant_fixture();
    for (path, replacement, expected) in [
        (
            "/value_uses/0/environment",
            json!(99),
            "environment outside arena",
        ),
        (
            "/value_uses/0/roots/0",
            json!(99),
            "root outside expression arena",
        ),
        (
            "/environments/0/correlations",
            json!([{"identity":0,"row":1}]),
            "empty constant environment",
        ),
        (
            "/expressions/0/kind",
            json!({"kind":"correlation","identity":0}),
            "unbound",
        ),
        (
            "/expressions/1/kind",
            json!({"kind":"window_aggregate","operator":0,"arguments":[0],
            "ordinal":0,"distinct":false,"ignore_nulls":false}),
            "window aggregate",
        ),
    ] {
        let mut invalid = objects.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        assert!(
            check(
                inputs.clone(),
                producers.clone(),
                properties.clone(),
                invalid,
                ObjectLimits::default()
            )
            .unwrap_err()
            .contains(expected),
            "{path}"
        );
    }
    let mut invalid = inputs.clone();
    invalid["environments"][0]["row"] = json!(1);
    assert!(
        check(
            invalid,
            producers.clone(),
            properties.clone(),
            objects.clone(),
            ObjectLimits::default()
        )
        .unwrap_err()
        .contains("constant environment must have no fields")
    );
    let mut invalid = objects.clone();
    invalid["value_uses"] = json!([]);
    assert!(
        check(
            inputs,
            producers,
            properties,
            invalid,
            ObjectLimits::default()
        )
        .is_err()
    );
}

#[test]
fn catalog_claims_value_slots_and_preserves_optional_metadata() {
    use feldera_sql_rel_model::catalog::CatalogSchema;
    let (mut inputs, _, _, mut expressions) = constant_fixture();
    inputs["type_table"]["sources"] = json!([{"name":"catalog","text":""}]);
    expressions["environments"][0]["source"] = json!({"kind":"present","source":0});
    expressions["value_uses"][0]["roots"] = json!([0, 0]);
    let inputs: InputUses = serde_json::from_value(inputs).unwrap();
    let expressions: ExpressionObjects = serde_json::from_value(expressions).unwrap();
    let diagnostic = json!({"origin":{"kind":"absent"},"text":"quoted column","internal_text":"internal", "empty":false});
    let wire = json!({"source":0,"name":{"name":"Table","quoted":true},"diagnostic":diagnostic,"values":0,
        "columns":[{"name":{"name":"Column","quoted":true},"index":0,"ty":0,
            "diagnostic":diagnostic,"primary_key":true,"unused":false,"lateness":0,"default_value":1,
            "default_position":{"kind":"absent"}}],
        "properties":[{"name":"expected_size","value":"50","key_position":{"kind":"absent"},"value_position":{"kind":"absent"}}]});
    let schema: CatalogSchema = serde_json::from_value(wire.clone()).unwrap();
    schema
        .validate(
            &inputs.type_table,
            &expressions,
            ValidationLimits::default(),
        )
        .unwrap();
    assert_eq!(serde_json::to_value(schema).unwrap(), wire);
    for (path, replacement, expected) in [
        ("/values", json!(99), "use outside arena"),
        ("/columns/0/default_value", json!(0), "out of order"),
        ("/columns/0/default_value", Value::Null, "unclaimed"),
        ("/columns/0/ty", json!(99), "reference"),
        (
            "/properties",
            json!([wire["properties"][0], wire["properties"][0]]),
            "duplicate",
        ),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        let invalid: CatalogSchema = serde_json::from_value(invalid).unwrap();
        assert!(
            invalid
                .validate(
                    &inputs.type_table,
                    &expressions,
                    ValidationLimits::default()
                )
                .unwrap_err()
                .to_string()
                .contains(expected),
            "{path}"
        );
    }
    for field in ["lateness", "default_value", "default_position"] {
        let mut invalid = wire.clone();
        invalid["columns"][0].as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<CatalogSchema>(invalid).is_err(),
            "{field}"
        );
    }
    let mut invalid = wire.clone();
    invalid.as_object_mut().unwrap().remove("properties");
    assert!(serde_json::from_value::<CatalogSchema>(invalid).is_err());
    let schema: CatalogSchema = serde_json::from_value(wire).unwrap();
    assert!(
        schema
            .validate(
                &inputs.type_table,
                &expressions,
                ValidationLimits {
                    source_bytes: 1,
                    ..ValidationLimits::default()
                }
            )
            .is_err()
    );
    let generated = serde_json::to_value(schemars::schema_for!(CatalogSchema)).unwrap();
    let checked: Value = serde_json::from_str(include_str!("../catalog-schema.json")).unwrap();
    assert_eq!(generated, checked);
}

#[test]
fn catalog_scan_edges_retain_row_identity_and_require_complete_ownership() {
    use feldera_sql_rel_model::catalog::Catalog;
    let (mut inputs, mut producers, mut properties, mut expressions) = constant_fixture();
    inputs["type_table"]["sources"] = json!([{"name":"catalog","text":""}]);
    expressions["environments"][0]["source"] = json!({"kind":"present","source":0});
    producers["relations"] =
        json!([{"form":"LogicalTableScan","row":1,"inputs":[],"constants":[]}]);
    properties["relations"] =
        json!([{"metadata":{"traits":[],"hints":[]},"fields":{"kind":"scan","name":["Table"]}}]);
    expressions["value_uses"][0]["roots"] = json!([]);
    let inputs: InputUses = serde_json::from_value(inputs).unwrap();
    let expressions: ExpressionObjects = serde_json::from_value(expressions).unwrap();
    let producers: InputProducers = serde_json::from_value(producers).unwrap();
    let properties: RelationProperties = serde_json::from_value(properties).unwrap();
    let diagnostic = json!({"origin":{"kind":"absent"},"text":"","internal_text":"","empty":true});
    let wire = json!({"schema_version":"catalog-v1","schemas":[{"source":0,
        "name":{"name":"Table","quoted":true},"diagnostic":diagnostic,"values":0,
        "columns":[{"name":{"name":"x","quoted":true},"index":0,"ty":0,
            "diagnostic":diagnostic,"primary_key":true,"unused":false,"lateness":null,"default_value":null,"default_position":null}],
        "properties":null}],"tables":[{"schema":0,"row":1,"name":["Table"]}],"scans":[{"relation":0,"table":0}]});
    let validate = |wire: Value| {
        serde_json::from_value::<Catalog>(wire).unwrap().validate(
            &inputs.type_table,
            &expressions,
            &producers,
            &properties,
            ValidationLimits::default(),
        )
    };
    validate(wire.clone()).unwrap();
    for (path, replacement, expected) in [
        ("/scans", json!([]), "no catalog edge"),
        (
            "/scans",
            json!([wire["scans"][0], wire["scans"][0]]),
            "duplicate",
        ),
        ("/scans/0/relation", json!(99), "outside relation arena"),
        ("/scans/0/table", json!(99), "table outside arena"),
        ("/tables/0/schema", json!(99), "schema outside arena"),
        ("/tables/0/row", json!(2), "schema fields"),
        ("/tables/0/name", json!(["other"]), "qualified name differs"),
        ("/schemas/0/columns/0/ty", json!(3), "schema fields"),
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = replacement;
        assert!(
            validate(invalid)
                .unwrap_err()
                .to_string()
                .contains(expected),
            "{path}"
        );
    }
    let generated = serde_json::to_value(schemars::schema_for!(Catalog)).unwrap();
    let checked: Value =
        serde_json::from_str(include_str!("../catalog-graph-schema.json")).unwrap();
    assert_eq!(generated, checked);
}

fn search_fixture() -> (Value, Value, Value, Value) {
    let (mut inputs, mut producers, properties, mut objects) = fixture();
    let types = inputs["type_table"]["types"].as_array_mut().unwrap();
    types.push(json!({"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"boolean"}}));
    types.push(json!({"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[
        {"name":{"name":"found","quoted":false,"origin":{"kind":"absent"}},"ty":4}]}}));
    for index in 0..2 {
        producers["relations"][index]["row"] = json!(5);
        objects["relation_uses"][index]["roots"] = json!([2]);
    }
    objects["operator_objects"][0] = json!({"kind":"builtin","registry":"calcite-s1-fixture","registry_version":"1","key":"SqlStdOperatorTable.SEARCH"});
    objects["operators"][0] = json!({"object":0,"parameters":[0,0],"result":4});
    let bound = |value: &str, closed: bool| {
        json!({"kind":"bounded","closed":closed,
        "literal":{"ty":0,"origin":{"kind":"absent"},"value":{"kind":"integer","value":value}}})
    };
    objects["expressions"][1] = json!({"ty":0,"position":{"kind":"absent"},"kind":{
        "kind":"search_argument","null_as":"UNKNOWN","ranges":[
            {"lower":{"kind":"unbounded"},"upper":bound("0",false)},
            {"lower":bound("0",false),"upper":bound("10",true)}]}});
    objects["expressions"]
        .as_array_mut()
        .unwrap()
        .push(json!({"ty":4,"position":{"kind":"absent"},
        "kind":{"kind":"call","operator":0,"arguments":[0,1]}}));
    (inputs, producers, properties, objects)
}

#[test]
fn search_ranges_preserve_holes_unbounded_intervals_and_null_policy() {
    let (inputs, producers, properties, objects) = search_fixture();
    for policy in ["TRUE", "FALSE", "UNKNOWN"] {
        for ranges in [
            objects["expressions"][1]["kind"]["ranges"].clone(),
            json!([]),
            json!([{"lower":{"kind":"unbounded"},"upper":{"kind":"unbounded"}}]),
        ] {
            let mut candidate = objects.clone();
            candidate["expressions"][1]["kind"]["null_as"] = json!(policy);
            candidate["expressions"][1]["kind"]["ranges"] = ranges;
            check(
                inputs.clone(),
                producers.clone(),
                properties.clone(),
                candidate.clone(),
                ObjectLimits::default(),
            )
            .unwrap();
            let decoded: ExpressionObjects = serde_json::from_value(candidate.clone()).unwrap();
            assert_eq!(serde_json::to_value(decoded).unwrap(), candidate);
        }
    }
}

#[test]
fn malformed_search_ranges_and_expression_roles_are_rejected() {
    let (inputs, producers, properties, objects) = search_fixture();
    let mutations: &[(&str, Value)] = &[
        ("/expressions/1/kind/null_as", json!("IGNORE")),
        ("/expressions/1/kind/ranges/0/upper/closed", json!(true)),
        (
            "/expressions/1/kind/ranges/1/upper/literal/value/value",
            json!("-1"),
        ),
        ("/expressions/1/kind/ranges/1/lower/literal/ty", json!(3)),
        (
            "/expressions/1/kind/ranges/1/lower/literal/value",
            json!({"kind":"null"}),
        ),
        (
            "/expressions/1/kind/ranges/1/lower",
            json!({"kind":"unbounded"}),
        ),
        ("/relation_uses/0/roots", json!([1])),
        ("/expressions/2/kind/arguments", json!([1, 0])),
        ("/expressions/2/kind/arguments", json!([0, 0])),
        (
            "/operator_objects/0/key",
            json!("SqlStdOperatorTable.EQUALS"),
        ),
    ];
    for (path, value) in mutations {
        let mut candidate = objects.clone();
        *candidate.pointer_mut(path).unwrap() = value.clone();
        assert!(
            check(
                inputs.clone(),
                producers.clone(),
                properties.clone(),
                candidate,
                ObjectLimits::default()
            )
            .is_err(),
            "accepted mutation {path}"
        );
    }
    let mut reversed = objects;
    reversed["expressions"][1]["kind"]["ranges"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert!(
        check(
            inputs,
            producers,
            properties,
            reversed,
            ObjectLimits::default()
        )
        .is_err()
    );
}
