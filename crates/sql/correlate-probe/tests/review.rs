//! Adversarial boundary checks derived from one retained transported fragment.
//! These assert safe refusals and recipe shape, not C7 result equivalence.
use feldera_correlate_probe::{Error, Fragment, decorrelate, recipe::Node};
use feldera_sql_rel_model::{LiteralLimits, ObjectLimits, ValidationLimits};
use serde_json::{Value, json};

fn original() -> Value {
    serde_json::from_str(include_str!("fixtures/correlated-filter.json")).unwrap()
}

fn supported() -> Value {
    let mut value = original();
    // The retained source has a correlated MAX aggregate. Replace only that
    // node with a projection of the already represented outer field, preserving
    // its one-column nullable INTEGER output. This derived fixture exercises the
    // bounded rule subset without claiming the original aggregate was ported.
    value["id"] = json!("review-derived-correlated-filter");
    value["producers"]["relations"][2]["form"] = json!("LogicalProject");
    value["properties"]["relations"][2]["fields"] =
        json!({"kind":"project","expressions":1,"variables":[]});
    value["expressions"]["relation_uses"][2]["roots"] = json!([2]);
    value["expressions"]["environments"][2]["correlations"] = json!([{"identity":1,"row":7}]);
    value
}

fn validated(value: Value) -> Fragment {
    let fragment: Fragment = serde_json::from_value(value).unwrap();
    // Prove that these are gaps in the rewriter's semantic preconditions, not
    // malformed references already caught by the existing FRel validation.
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
fn preserved_leaf_aliases_do_not_block_supported_domain_rewrite() {
    let fragment = validated(supported());
    let recipe = decorrelate(&fragment).unwrap();
    assert_eq!(recipe.removed_correlates, 1);
    for node in &recipe.nodes {
        if let Node::Original { relation } = node {
            assert!(
                fragment.producers.relations[*relation as usize]
                    .inputs
                    .is_empty()
            );
        }
    }
}

#[test]
fn scalar_max_restores_missing_domain_groups_before_parent_projection() {
    let recipe = decorrelate(&validated(original())).unwrap();
    assert_eq!(recipe.removed_correlates, 1);
    assert!(recipe.nodes.iter().any(|node| matches!(node,
        Node::Aggregate { group, arguments, .. } if group == &[0] && arguments == &[vec![1]])));
    assert!(
        recipe
            .nodes
            .iter()
            .filter(|node| matches!(
                node,
                Node::Join {
                    join_kind: feldera_sql_rel_model::relation_properties::JoinKind::Left,
                    ..
                }
            ))
            .count()
            >= 2
    );
}

#[test]
fn unported_scalar_call_metadata_is_refused() {
    for field in ["distinct", "approximate", "ignore_nulls"] {
        let mut value = original();
        value["properties"]["relations"][2]["fields"]["calls"][0][field] = json!(true);
        assert!(matches!(
            decorrelate(&validated(value)),
            Err(Error::Unsupported(_))
        ));
    }
}

#[test]
fn repeated_empty_grouping_sets_do_not_masquerade_as_scalar_aggregation() {
    let mut value = original();
    value["properties"]["relations"][2]["fields"]["groups"] = json!([[], []]);
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn other_hints_remain_refused() {
    let mut value = supported();
    value["properties"]["relations"][1]["metadata"]["hints"][0]["name"] = json!("broadcast");
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn correlate_projection_does_not_silently_truncate_payload() {
    let mut value = supported();
    value["producers"]["relations"][0]["row"] = json!(7); // one field instead of two
    let result = decorrelate(&validated(value));
    assert!(
        matches!(result, Err(Error::Invalid(message)) if message.contains("correlate output width"))
    );
}

#[test]
fn filter_output_cannot_change_its_input_payload_width() {
    let mut value = supported();
    value["producers"]["relations"][3]["row"] = json!(2); // empty row
    value["table"]["environments"][2]["row"] = json!(2); // its Project consumer
    let result = decorrelate(&validated(value));
    assert!(matches!(result, Err(Error::Invalid(message)) if message.contains("filter output")));
}

#[test]
fn explicit_expression_binding_does_not_supply_a_relational_ancestor() {
    let mut value = supported();
    value["producers"]["roots"] = json!([2]); // start at the correlated Project
    let result = decorrelate(&validated(value));
    assert!(
        matches!(result, Err(Error::Unsupported(message)) if message.contains("free correlation"))
    );
}

#[test]
fn set_projection_does_not_silently_truncate_branch_payload() {
    let mut value = supported();
    let mut row = value["table"]["type_table"]["types"][8].clone();
    let field = row["kind"]["fields"][0].clone();
    row["kind"]["fields"] = json!([field, field]);
    value["table"]["type_table"]["types"]
        .as_array_mut()
        .unwrap()
        .push(row);
    value["table"]["relations"] = json!(6);
    value["table"]["environments"][0] =
        json!({"owner":{"kind":"relations","inputs":[1,5]},"row":7});
    value["table"]["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"owner":{"kind":"relations","inputs":[2,2]},"row":10}));
    value["producers"]["relations"][0]["inputs"] = json!([1, 5]);
    value["producers"]["relations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"form":"LogicalUnion","row":2,"inputs":[2,2],"constants":[]}));
    value["properties"]["relations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"fields":{"kind":"set","all":true},"metadata":{"traits":[],"hints":[]}}));
    value["expressions"]["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"input":5,"source":{"kind":"present","source":0},"correlations":[]}));
    value["expressions"]["relation_uses"]
        .as_array_mut()
        .unwrap()
        .push(json!({"relation":5,"environment":6,"roots":[]}));
    let result = decorrelate(&validated(value));
    assert!(
        matches!(result, Err(Error::Invalid(message)) if message.contains("set operation branch"))
    );
}

#[test]
fn rebuilt_parent_does_not_emit_unused_original_correlate_subtree() {
    let mut value = supported();
    value["table"]["relations"] = json!(6);
    value["table"]["objects"] = json!([{"ty":1,"field":0},{"ty":1,"field":1}]);
    value["table"]["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"owner":{"kind":"relations","inputs":[0]},"row":6}));
    value["table"]["uses"] = json!([{"object":0,"environment":5},{"object":1,"environment":5}]);
    value["producers"]["relations"]
        .as_array_mut()
        .unwrap()
        .push(json!({"form":"LogicalProject","row":6,"inputs":[0],"constants":[]}));
    value["producers"]["roots"] = json!([5]);
    value["properties"]["relations"].as_array_mut().unwrap().push(
        json!({"fields":{"kind":"project","expressions":2,"variables":[]},"metadata":{"traits":[],"hints":[]}}));
    for object in 0..2 {
        value["expressions"]["expressions"].as_array_mut().unwrap().push(
            json!({"ty":1,"position":{"kind":"absent"},"kind":{"kind":"input_ref","object":object}}));
    }
    value["expressions"]["environments"]
        .as_array_mut()
        .unwrap()
        .push(json!({"input":5,"source":{"kind":"present","source":0},"correlations":[]}));
    value["expressions"]["relation_uses"]
        .as_array_mut()
        .unwrap()
        .push(json!({"relation":5,"environment":6,"roots":[4,5]}));
    let fragment = validated(value);
    let recipe = decorrelate(&fragment).unwrap();
    assert_eq!(recipe.removed_correlates, 1);
    assert!(recipe.nodes.iter().all(|node| {
        match node {
            Node::Original { relation } => fragment.producers.relations[*relation as usize]
                .inputs
                .is_empty(),
            _ => true,
        }
    }));
}

/// One new shared row type can describe many empty Values roots. The existing
/// correlated fixture remains unchanged but unreachable from these test roots.
fn wide_roots(width: usize, roots: usize, shared_node: bool, name_bytes: usize) -> Fragment {
    let mut value = supported();
    let row_id = value["table"]["type_table"]["types"]
        .as_array()
        .unwrap()
        .len();
    let names: Vec<_> = (0..width)
        .map(|index| {
            let name = format!("f{index}{}", "x".repeat(name_bytes));
            json!({"name":{"name":name,"quoted":true,"origin":{"kind":"absent"}},"ty":4})
        })
        .collect();
    value["table"]["type_table"]["types"]
        .as_array_mut()
        .unwrap()
        .push(json!({"nullable":false,"origin":{"kind":"absent"},"kind":{
            "kind":"row","struct_kind":"peek_fields_no_expand","fields":names}}));
    let first = value["producers"]["relations"].as_array().unwrap().len();
    let nodes = if shared_node { 1 } else { roots };
    for relation in first..first + nodes {
        value["producers"]["relations"]
            .as_array_mut()
            .unwrap()
            .push(json!({"form":"LogicalValues","row":row_id,"inputs":[],"constants":[]}));
        value["properties"]["relations"]
            .as_array_mut()
            .unwrap()
            .push(
                json!({"fields":{"kind":"values","tuples":0},"metadata":{"traits":[],"hints":[]}}),
            );
        value["expressions"]["relation_uses"]
            .as_array_mut()
            .unwrap()
            .push(json!({"relation":relation,"environment":1,"roots":[]}));
    }
    value["table"]["relations"] = json!(first + nodes);
    let selected: Vec<_> = (0..roots)
        .map(|index| first + if shared_node { 0 } else { index })
        .collect();
    value["producers"]["roots"] = json!(selected);
    validated(value)
}

#[test]
fn shared_wide_row_type_cannot_expand_unbounded_frames() {
    assert!(decorrelate(&wide_roots(1024, 2, false, 0)).is_ok());
    assert!(matches!(
        decorrelate(&wide_roots(1024, 100, false, 0)),
        Err(Error::Limit)
    ));
}

#[test]
fn memoized_frames_still_charge_copied_payload_and_columns() {
    assert!(decorrelate(&wide_roots(1, 100, true, 0)).is_ok());
    assert!(matches!(
        decorrelate(&wide_roots(1024, 100, true, 0)),
        Err(Error::Limit)
    ));
}

#[test]
fn repeated_long_field_names_have_a_separate_byte_budget() {
    assert!(decorrelate(&wide_roots(1, 1, true, 100_000)).is_ok());
    assert!(matches!(
        decorrelate(&wide_roots(1, 100, true, 100_000)),
        Err(Error::Limit)
    ));
}

#[test]
fn reconstructed_project_and_filter_identify_alias_sources() {
    let mut value = supported();
    let hint = value["properties"]["relations"][1]["metadata"]["hints"][0].clone();
    for relation in [2, 3] {
        value["properties"]["relations"][relation]["metadata"]["hints"] = json!([hint]);
    }
    let recipe = decorrelate(&validated(value.clone())).unwrap();
    assert!(recipe.nodes.iter().any(|node| matches!(
        node,
        Node::Project {
            relation: Some(2),
            ..
        }
    )));
    assert!(
        recipe
            .nodes
            .iter()
            .any(|node| matches!(node, Node::Filter { relation: 3, .. }))
    );
    assert_eq!(
        recipe
            .nodes
            .iter()
            .filter(|node| matches!(
                node,
                Node::Project {
                    relation: Some(_),
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(
        recipe
            .nodes
            .iter()
            .any(|node| matches!(node, Node::Project { relation: None, .. }))
    );
    value["properties"]["relations"][2]["metadata"]["hints"][0]["name"] = json!("broadcast");
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn alias_on_removed_correlate_is_not_silently_lost() {
    let mut value = supported();
    value["properties"]["relations"][0]["metadata"]["hints"] =
        value["properties"]["relations"][1]["metadata"]["hints"].clone();
    assert!(matches!(
        decorrelate(&validated(value)),
        Err(Error::Unsupported(_))
    ));
}

#[test]
fn duplicate_internal_names_do_not_merge_payload_and_domain_positions() {
    let mut value = supported();
    // A right payload named x collides with the propagated outer x. Give the
    // hidden field a deterministic unique name; the final row keeps x/x0.
    value["table"]["type_table"]["types"][8]["kind"]["fields"][0]["name"]["name"] = json!("x");
    value["table"]["type_table"]["types"][0]["kind"]["fields"][1]["name"]["name"] = json!("x");
    value["table"]["type_table"]["types"][6]["kind"]["fields"][1]["name"]["name"] = json!("x0");
    let recipe = decorrelate(&validated(value)).unwrap();
    let columns = recipe
        .nodes
        .iter()
        .find_map(|node| match node {
            Node::Project {
                relation: Some(2),
                columns,
                ..
            } => Some(columns),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["x", "__correlation_1"]
    );
    assert!(
        recipe
            .nodes
            .iter()
            .any(|node| matches!(node, Node::Join { keys, .. } if keys == &[(0,1)]))
    );
    let Node::Project { columns, .. } = &recipe.nodes[recipe.roots[0].0 as usize] else {
        panic!("final projection")
    };
    assert_eq!(
        columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["x", "x0"]
    );
}

#[test]
fn user_payload_cannot_collide_with_generated_hidden_name() {
    let mut value = supported();
    for (ty, field) in [(8, 0), (0, 1), (6, 1)] {
        value["table"]["type_table"]["types"][ty]["kind"]["fields"][field]["name"]["name"] =
            json!("__correlation_1");
    }
    let recipe = decorrelate(&validated(value)).unwrap();
    let columns = recipe
        .nodes
        .iter()
        .find_map(|node| match node {
            Node::Project {
                relation: Some(2),
                columns,
                ..
            } => Some(columns),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        ["__correlation_1", "__correlation_1_1"]
    );
}

#[test]
fn nullable_union_uses_actual_input_type_and_explicit_widening() {
    use feldera_correlate_probe::recipe::Expression;
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/nullable-union.json")).unwrap();
    let fragment = validated(fixture);
    let recipe = decorrelate(&fragment).unwrap();
    let mut widened = 0;
    for node in &recipe.nodes {
        if let Node::Project {
            expressions,
            columns,
            ..
        } = node
        {
            let names: std::collections::BTreeSet<_> =
                columns.iter().map(|column| &column.name).collect();
            assert_eq!(names.len(), columns.len());
            for expression in expressions {
                if let Expression::WidenNullability { input, ty } = expression {
                    assert_eq!(ty.0, 0); // nullable INTEGER union output
                    assert!(matches!(input.as_ref(), Expression::Input { ty, .. } if ty.0 == 5));
                    widened += 1;
                }
            }
        }
        if let Node::Set { inputs, .. } = node {
            let mut expected = None;
            for input in inputs {
                let Node::Project { columns, .. } = &recipe.nodes[input.0 as usize] else {
                    panic!("aligned branch")
                };
                let names: Vec<_> = columns.iter().map(|column| column.name.as_str()).collect();
                if let Some(previous) = &expected {
                    assert_eq!(&names, previous);
                }
                expected = Some(names);
            }
        }
    }
    assert_eq!(widened, 1);
}

#[test]
fn nullable_adaptation_cannot_narrow_or_change_the_value_type() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/nullable-union.json")).unwrap();
    let mut changed_integer = fixture.clone();
    changed_integer["table"]["type_table"]["types"][5]["kind"]["width"] = json!("i64");
    let mut narrowed_nullability = fixture;
    narrowed_nullability["table"]["type_table"]["types"][0]["nullable"] = json!(false);
    narrowed_nullability["table"]["type_table"]["types"][5]["nullable"] = json!(true);
    for value in [changed_integer, narrowed_nullability] {
        assert!(
            matches!(decorrelate(&validated(value)), Err(Error::Unsupported(message))
            if message.contains("coercion beyond top-level nullable widening"))
        );
    }
}
