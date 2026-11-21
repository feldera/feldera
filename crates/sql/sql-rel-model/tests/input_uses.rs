use feldera_sql_rel_model::{ValidationLimits, input_uses::InputUses};
use serde_json::{Value, json};

fn fixture() -> Value {
    json!({
        "schema_version":"input-uses-v1", "relations":2,"functions":0,
        "type_table":{"schema_version":"types-v0","sources":[],"roots":[],
            "types":[
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i32","signed":true}},
                {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified",
                    "fields":[{"name":{"name":"x","quoted":true,"origin":{"kind":"absent"}},"ty":0}]}}
            ]},
        "objects":[{"ty":0,"field":0},{"ty":0,"field":0}],
        "environments":[
            {"owner":{"kind":"relations","inputs":[0]},"row":1},
            {"owner":{"kind":"relations","inputs":[1]},"row":1}],
        "uses":[{"object":0,"environment":0},{"object":0,"environment":1},{"object":1,"environment":0}]
    })
}

fn check(value: Value, states: usize) -> Result<(), String> {
    let table: InputUses = serde_json::from_value(value).map_err(|error| error.to_string())?;
    table
        .validate(ValidationLimits::default(), states)
        .map_err(|error| error.to_string())
}

#[test]
fn equal_distinct_objects_and_shared_objects_keep_their_identity() {
    let value = fixture();
    check(value.clone(), 3).unwrap();
    let table: InputUses = serde_json::from_value(value.clone()).unwrap();
    assert_eq!(serde_json::to_value(table).unwrap(), value);
}

#[test]
fn every_environment_is_checked_even_for_previously_validated_objects() {
    let mut value = fixture();
    let mut wrong_row = value["type_table"]["types"][1].clone();
    let mut wrong_type = value["type_table"]["types"][0].clone();
    wrong_type["kind"]["width"] = json!("i64");
    value["type_table"]["types"]
        .as_array_mut()
        .unwrap()
        .push(wrong_type);
    wrong_row["kind"]["fields"][0]["ty"] = json!(2);
    value["type_table"]["types"]
        .as_array_mut()
        .unwrap()
        .push(wrong_row);
    value["environments"][1]["row"] = json!(3);
    // The first environment passes; the same object's second use must fail.
    assert!(
        check(value, 3)
            .unwrap_err()
            .contains("evaluation environment")
    );
    let mut value = fixture();
    value["environments"][1]["row"] = json!(0);
    assert!(check(value, 3).is_err());
}

#[test]
fn state_budget_and_missing_duplicate_or_invalid_uses_fail() {
    assert!(check(fixture(), 2).is_err());
    for (path, replacement) in [
        ("/uses/1/object", json!(99)),
        ("/uses/1/environment", json!(99)),
        ("/uses/1/environment", json!(0)),
        ("/objects/0/field", json!(1)),
        ("/environments/1/owner/inputs", json!([0])),
        ("/environments/1/owner/inputs", json!([2])),
        (
            "/environments/0/owner",
            json!({"kind":"function","function":0}),
        ),
        (
            "/environments/0/owner",
            json!({"kind":"window","relation":2,"inputs":[0]}),
        ),
    ] {
        let mut value = fixture();
        *value.pointer_mut(path).unwrap() = replacement;
        assert!(check(value, 3).is_err(), "{path}");
    }
    let mut value = fixture();
    value["uses"].as_array_mut().unwrap().pop();
    assert!(check(value, 3).is_err());
}

#[test]
fn input_uses_schema_matches_the_checked_in_contract() {
    let generated = serde_json::to_value(schemars::schema_for!(InputUses)).unwrap();
    let checked: Value = serde_json::from_str(include_str!("../input-uses-schema.json")).unwrap();
    assert_eq!(generated, checked);
}

#[test]
fn owners_and_uses_reject_unknown_or_missing_fields() {
    for path in [
        "/objects/0",
        "/environments/0",
        "/environments/0/owner",
        "/uses/0",
    ] {
        let mut value = fixture();
        value
            .pointer_mut(path)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("extra".into(), json!(true));
        assert!(serde_json::from_value::<InputUses>(value).is_err());
    }
    let mut value = fixture();
    value.as_object_mut().unwrap().remove("uses");
    assert!(serde_json::from_value::<InputUses>(value).is_err());
}

fn producers_fixture() -> (
    InputUses,
    feldera_sql_rel_model::input_producers::InputProducers,
) {
    let mut uses = fixture();
    uses["relations"] = json!(4);
    let producers = json!({
        "schema_version":"input-producers-v0",
        "relations":[
            {"form":"LogicalValues","row":1,"inputs":[],"constants":[]},
            {"form":"LogicalValues","row":1,"inputs":[],"constants":[]},
            {"form":"LogicalProject","row":1,"inputs":[0],"constants":[]},
            {"form":"LogicalProject","row":1,"inputs":[1],"constants":[]}
        ],
        "roots":[2,3], "function_rows":[]
    });
    (
        serde_json::from_value(uses).unwrap(),
        serde_json::from_value(producers).unwrap(),
    )
}

#[test]
fn producer_graph_checks_every_environment_against_its_actual_input() {
    let (mut uses, producers) = producers_fixture();
    producers
        .validate(&uses, ValidationLimits::default(), 3)
        .unwrap();
    // Give the second input a distinct output field name. The input-use arena
    // alone remains valid, but its row no longer matches the actual producer.
    let mut wrong = uses.type_table.types[1].clone();
    if let feldera_sql_rel_model::types::TypeKind::Row { fields, .. } = &mut wrong.kind {
        fields[0].name.name = "different".into();
    }
    uses.type_table.types.push(wrong);
    let mut wrong_producers = producers.clone();
    wrong_producers.relations[1].row = feldera_sql_rel_model::TypeId(2);
    assert!(
        wrong_producers
            .validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
}

#[test]
fn producer_cycles_bad_arity_and_extra_constants_fail() {
    use feldera_sql_rel_model::input_producers::RelationForm;
    let (uses, producers) = producers_fixture();
    let mut cyclic = producers.clone();
    cyclic.relations[0].form = RelationForm::LogicalProject;
    cyclic.relations[0].inputs = vec![2];
    assert!(
        cyclic
            .validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
    let mut arity = producers.clone();
    arity.relations[2].inputs.clear();
    assert!(
        arity
            .validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
    let mut constants = producers.clone();
    constants.relations[2]
        .constants
        .push(feldera_sql_rel_model::TypeId(0));
    assert!(
        constants
            .validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
    let mut root = producers;
    root.roots.push(99);
    assert!(
        root.validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
}

#[test]
fn window_constants_are_appended_after_producer_fields() {
    use feldera_sql_rel_model::{TypeId, input_producers::RelationForm, input_uses::InputOwner};
    let (mut uses, mut producers) = producers_fixture();
    let mut row = uses.type_table.types[1].clone();
    if let feldera_sql_rel_model::types::TypeKind::Row { fields, .. } = &mut row.kind {
        let mut constant = fields[0].clone();
        constant.name.name = "$constant1".into();
        fields.push(constant);
    }
    uses.type_table.types.push(row);
    producers.relations[3].form = RelationForm::LogicalWindow;
    producers.relations[3].constants.push(TypeId(0));
    uses.environments[1].owner = InputOwner::Window {
        relation: 3,
        inputs: vec![1],
    };
    uses.environments[1].row = TypeId(2);
    producers
        .validate(&uses, ValidationLimits::default(), 3)
        .unwrap();
    producers.relations[3].constants.clear();
    assert!(
        producers
            .validate(&uses, ValidationLimits::default(), 3)
            .is_err()
    );
}

#[test]
fn producer_schema_matches_checked_in_contract() {
    let generated = serde_json::to_value(schemars::schema_for!(
        feldera_sql_rel_model::input_producers::InputProducers
    ))
    .unwrap();
    let checked: Value =
        serde_json::from_str(include_str!("../input-producers-schema.json")).unwrap();
    assert_eq!(generated, checked);
}
