use feldera_sql_rel_model::{
    ValidationLimits, input_producers::InputProducers, input_uses::InputUses,
    relation_properties::RelationProperties,
};
use serde_json::{Value, json};

fn fixture() -> (Value, Value, Value) {
    let types = json!([
        {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"integer","width":"i32","signed":true}},
        {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"boolean"}},
        {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[
            {"name":{"name":"x","quoted":true,"origin":{"kind":"absent"}},"ty":0},
            {"name":{"name":"p","quoted":true,"origin":{"kind":"absent"}},"ty":1}]}},
        {"nullable":false,"origin":{"kind":"absent"},"kind":{"kind":"row","struct_kind":"fully_qualified","fields":[
            {"name":{"name":"sum","quoted":true,"origin":{"kind":"absent"}},"ty":0}]}}
    ]);
    let uses = json!({"schema_version":"input-uses-v1","relations":2,"functions":0,
        "type_table":{"schema_version":"types-v0","sources":[],"roots":[],"types":types},
        "objects":[],"environments":[{"owner":{"kind":"relations","inputs":[0]},"row":2}],"uses":[]});
    let producers = json!({"schema_version":"input-producers-v0","relations":[
        {"form":"LogicalValues","row":2,"inputs":[],"constants":[]},
        {"form":"LogicalAggregate","row":3,"inputs":[0],"constants":[]}],"roots":[1],"function_rows":[]});
    let mut properties = json!({"schema_version":"relation-properties-v1","operators":{"kind":"owned","objects":[{"kind":"builtin","registry":"test","registry_version":"1","key":"sum"}]},"relations":[
        {"kind":"values","tuples":1},
        {"kind":"aggregate","group":[],"groups":[[]],"calls":[{
            "operator":0,"ty":0,
            "name":{"kind":"absent"},"distinct":true,"approximate":false,"ignore_nulls":true,
            "arguments":[0],"direct_arguments":0,"filter":{"kind":"present","field":1},
            "distinct_keys":{"kind":"present","fields":[0]},
            "collation":[{"field":0,"direction":"DESCENDING","null_direction":"FIRST"}],
            "position":{"line":1,"column":4,"end_line":1,"end_column":9,"quoted":false}}]}]});
    for relation in properties["relations"].as_array_mut().unwrap() {
        *relation = json!({"metadata":{"traits":[{"kind":"convention_none"}],"hints":[]},"fields":relation.clone()});
    }
    (uses, producers, properties)
}

fn check(uses: Value, producers: Value, properties: Value) -> Result<(), String> {
    let uses: InputUses = serde_json::from_value(uses).map_err(|e| e.to_string())?;
    let producers: InputProducers = serde_json::from_value(producers).map_err(|e| e.to_string())?;
    let properties: RelationProperties =
        serde_json::from_value(properties).map_err(|e| e.to_string())?;
    properties
        .validate(&producers, &uses, ValidationLimits::default(), 1_000_000)
        .map_err(|e| e.to_string())
}

#[test]
fn aggregate_properties_preserve_order_flags_and_explicit_absence() {
    let (uses, producers, properties) = fixture();
    check(uses, producers, properties.clone()).unwrap();
    let decoded: RelationProperties = serde_json::from_value(properties.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), properties);
    let mut missing = properties;
    missing["relations"][1]["fields"]["calls"][0]
        .as_object_mut()
        .unwrap()
        .remove("distinct_keys");
    assert!(serde_json::from_value::<RelationProperties>(missing).is_err());
}

#[test]
fn aggregate_fields_grouping_and_output_types_are_checked() {
    for (path, replacement) in [
        ("/relations/1/fields/calls/0/arguments", json!([2])),
        ("/relations/1/fields/calls/0/filter/field", json!(2)),
        ("/relations/1/fields/calls/0/filter/field", json!(0)),
        (
            "/relations/1/fields/calls/0/distinct_keys/fields",
            json!([0, 0]),
        ),
        ("/relations/1/fields/calls/0/collation/0/field", json!(2)),
        ("/relations/1/fields/calls/0/ty", json!(1)),
        ("/operators/objects/0/key", json!("")),
        ("/relations/1/fields/group", json!([0, 0])),
        ("/relations/1/fields/groups", json!([[0]])),
        ("/relations/1/fields", json!({"kind":"set","all":true})),
    ] {
        let (uses, producers, mut properties) = fixture();
        *properties.pointer_mut(path).unwrap() = replacement;
        assert!(
            check(uses, producers, properties).is_err(),
            "accepted {path}"
        );
    }
}

#[test]
fn aggregate_expression_slot_counts_are_bounded() {
    let (uses, producers, mut properties) = fixture();
    properties["relations"][1]["fields"]["calls"][0]["direct_arguments"] = json!(1_000_001);
    assert!(
        check(uses, producers, properties)
            .unwrap_err()
            .contains("limit")
    );
}

#[test]
fn fresh_aggregate_objects_remain_distinct_and_builtin_duplicates_fail() {
    let (uses, producers, mut properties) = fixture();
    properties["operators"]["objects"] = json!([{"kind":"sum_zero"},{"kind":"sum_zero"}]);
    check(uses.clone(), producers.clone(), properties.clone()).unwrap();
    let decoded: RelationProperties = serde_json::from_value(properties.clone()).unwrap();
    let feldera_sql_rel_model::relation_properties::OperatorArena::Owned { objects } =
        decoded.operators
    else {
        panic!("expected owned arena")
    };
    assert_eq!(objects.len(), 2);
    properties["relations"][1]["fields"]["calls"][0]["operator"] = json!(2);
    assert!(check(uses.clone(), producers.clone(), properties).is_err());
    let (_, _, mut properties) = fixture();
    let builtin = properties["operators"]["objects"][0].clone();
    properties["operators"]["objects"]
        .as_array_mut()
        .unwrap()
        .push(builtin);
    assert!(check(uses, producers, properties).is_err());
}

#[test]
fn window_output_width_and_collation_are_checked() {
    let (mut uses, mut producers, mut properties) = fixture();
    // One window invocation appends a column to the two-field input.
    let field = uses["type_table"]["types"][3]["kind"]["fields"][0].clone();
    let mut row = uses["type_table"]["types"][2].clone();
    row["kind"]["fields"].as_array_mut().unwrap().push(field);
    uses["type_table"]["types"][3] = row;
    uses["environments"][0]["owner"] = json!({"kind":"window","relation":1,"inputs":[0]});
    producers["relations"][1]["form"] = json!("LogicalWindow");
    properties["relations"][1]["fields"] = json!({"kind":"window","groups":[{
        "keys":[0],"rows":true,"lower":"preceding","upper":"current_row",
        "exclusion":"EXCLUDE_TIES","collation":[{"field":1,"direction":"ASCENDING","null_direction":"LAST"}],"calls":1}]});
    check(uses.clone(), producers.clone(), properties.clone()).unwrap();
    properties["relations"][1]["fields"]["groups"][0]["calls"] = json!(2);
    assert!(check(uses.clone(), producers.clone(), properties.clone()).is_err());
    properties["relations"][1]["fields"]["groups"][0]["calls"] = json!(1);
    properties["relations"][1]["fields"]["groups"][0]["collation"][0]["field"] = json!(3);
    assert!(check(uses, producers, properties).is_err());
}

#[test]
fn uncollect_mappings_can_overlap_but_must_be_in_range() {
    let (uses, mut producers, mut properties) = fixture();
    producers["relations"][1]["form"] = json!("Uncollect");
    properties["relations"][1]["fields"] = json!({"kind":"uncollect","ordinality":false,"outer":false,
        "expand_struct_fields":true,"aliases":[],"passthrough":[0],"collections":[1]});
    check(uses.clone(), producers.clone(), properties.clone()).unwrap();
    properties["relations"][1]["fields"]["collections"] = json!([0]);
    check(uses.clone(), producers.clone(), properties.clone()).unwrap();
    properties["relations"][1]["fields"]["collections"] = json!([2]);
    assert!(check(uses, producers, properties).is_err());
}

#[test]
fn schema_matches_the_published_properties_contract() {
    let generated = serde_json::to_value(schemars::schema_for!(RelationProperties)).unwrap();
    let checked: Value =
        serde_json::from_str(include_str!("../relation-properties-schema.json")).unwrap();
    assert_eq!(generated, checked);
}

fn metadata() -> Value {
    json!({"traits":[{"kind":"convention_none"},
        {"kind":"collation","values":[
            [{"field":0,"direction":"DESCENDING","null_direction":"FIRST"}],
            [{"field":1,"direction":"ASCENDING","null_direction":"LAST"}]]},
        {"kind":"distribution","values":[
            {"distribution":"HASH_DISTRIBUTED","keys":[0,0,1]},
            {"distribution":"RANGE_DISTRIBUTED","keys":[1,0]}]}],
        "hints":[{"name":"hint","position":{"line":2,"column":3,"end_line":2,"end_column":9,"quoted":true},
            "inherit_path":[0,2],"list":[],"options":[{"key":"\u{e000}","value":"first"},{"key":"\u{10000}","value":"second"}]}]})
}

#[test]
fn metadata_preserves_traits_hint_positions_and_unicode_map_keys() {
    let (uses, producers, mut properties) = fixture();
    properties["relations"][0]["metadata"] = metadata();
    check(uses, producers, properties.clone()).unwrap();
    let decoded: RelationProperties = serde_json::from_value(properties.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), properties);
}

#[test]
fn metadata_rejects_wrong_rows_duplicate_definitions_and_noncanonical_alternatives() {
    for (path, replacement) in [
        ("/traits/1/values/0/0/field", json!(2)),
        ("/traits/1/values/1/0/field", json!(0)),
        ("/traits/1/values", json!([])),
        ("/traits/2/values/0/keys", json!([1, 0])),
        ("/traits/2/values/1/keys", json!([2])),
        ("/traits/2", json!({"kind":"convention_none"})),
        ("/hints/0/list", json!(["mixed"])),
        ("/hints/0/name", json!("")),
        ("/hints/0/options/1/key", json!("\u{e000}")),
    ] {
        let (uses, producers, mut properties) = fixture();
        let mut metadata = metadata();
        *metadata.pointer_mut(path).unwrap() = replacement;
        properties["relations"][0]["metadata"] = metadata;
        assert!(
            check(uses, producers, properties).is_err(),
            "accepted {path}"
        );
    }
}
