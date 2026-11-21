use feldera_sql_rel_model::{
    TypeId, TypeTable, ValidationLimits,
    source::{Identifier, Origin, Position, Source, SourceId},
    types::{
        Cardinality, Field, FloatFlavor, IntegerWidth, IntervalEnd, IntervalUnit, Precision,
        SqlType, StructKind, TypeKind, TypeSchemaVersion,
    },
};
use serde_json::json;

fn ty(kind: TypeKind) -> SqlType {
    SqlType {
        nullable: false,
        origin: Origin::Absent {},
        kind,
    }
}

fn table(types: Vec<SqlType>) -> TypeTable {
    TypeTable {
        schema_version: TypeSchemaVersion::V0,
        sources: vec![],
        types,
        roots: vec![TypeId(0)],
    }
}

fn array(element: u32) -> SqlType {
    ty(TypeKind::Array {
        element: TypeId(element),
        cardinality: Cardinality::Unbounded,
    })
}

fn name(value: &str) -> Identifier {
    Identifier {
        name: value.into(),
        quoted: true,
        origin: Origin::Absent {},
    }
}

#[test]
fn round_trip_preserves_order_sharing_and_sql_distinctions() {
    let mut value = table(vec![
        ty(TypeKind::Row {
            fields: vec![
                Field {
                    name: name("A"),
                    ty: TypeId(1),
                },
                Field {
                    name: name("A"),
                    ty: TypeId(1),
                },
                Field {
                    name: name("a"),
                    ty: TypeId(2),
                },
            ],
            struct_kind: StructKind::FullyQualified,
        }),
        ty(TypeKind::Float {
            flavor: FloatFlavor::Float,
        }),
        ty(TypeKind::Float {
            flavor: FloatFlavor::Double,
        }),
        // Equal, unshared nodes must not be interned by serialization.
        ty(TypeKind::Float {
            flavor: FloatFlavor::Float,
        }),
        ty(TypeKind::Named {
            name: vec![name("schema"), name("T")],
            representation: TypeId(0),
        }),
    ]);
    value.types[2].nullable = true;
    value.roots = vec![TypeId(4), TypeId(0), TypeId(4)];
    value.validate(ValidationLimits::default()).unwrap();
    let encoded = serde_json::to_string(&value).unwrap();
    let restored: TypeTable = serde_json::from_str(&encoded).unwrap();
    assert_eq!(restored, value);
    assert_eq!(restored.types.len(), 5);
}

#[test]
fn unknown_missing_duplicate_and_future_fields_are_rejected() {
    let valid = serde_json::to_value(table(vec![ty(TypeKind::Boolean {})])).unwrap();
    for pointer in ["", "/types/0", "/types/0/kind", "/types/0/origin"] {
        let mut changed = valid.clone();
        changed
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(true));
        assert!(
            serde_json::from_value::<TypeTable>(changed).is_err(),
            "{pointer}"
        );
    }
    for field in ["nullable", "origin", "kind"] {
        let mut changed = valid.clone();
        changed["types"][0].as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<TypeTable>(changed).is_err(),
            "{field}"
        );
    }
    let mut future = valid.clone();
    future["schema_version"] = json!("types-v1");
    assert!(serde_json::from_value::<TypeTable>(future).is_err());
    let duplicate = serde_json::to_string(&valid)
        .unwrap()
        .replace("\"nullable\":false", "\"nullable\":false,\"nullable\":true");
    assert!(serde_json::from_str::<TypeTable>(&duplicate).is_err());
}

#[test]
fn missing_references_and_orphan_cycles_fail() {
    let mut value = table(vec![ty(TypeKind::Boolean {}), array(9)]);
    assert!(
        value
            .validate(ValidationLimits::default())
            .unwrap_err()
            .message
            .contains("outside")
    );
    value.types[1] = array(1);
    assert!(
        value
            .validate(ValidationLimits::default())
            .unwrap_err()
            .message
            .contains("cyclic")
    );
    value.types = vec![array(1), array(0)];
    assert!(value.validate(ValidationLimits::default()).is_err());
    value.types = vec![ty(TypeKind::Boolean {})];
    value.roots = vec![TypeId(1)];
    assert_eq!(
        value
            .validate(ValidationLimits::default())
            .unwrap_err()
            .path,
        "roots[0]"
    );
}

#[test]
fn shared_subtree_does_not_hide_a_longer_path() {
    // Node 0 is completed first; node 3 later reaches it via a longer path.
    let value = table(vec![array(1), ty(TypeKind::Boolean {}), array(0), array(2)]);
    assert!(
        value
            .validate(ValidationLimits {
                depth: 3,
                ..ValidationLimits::default()
            })
            .is_err()
    );
    value
        .validate(ValidationLimits {
            depth: 4,
            ..ValidationLimits::default()
        })
        .unwrap();
}

#[test]
fn depth_and_total_reference_limits_cover_flat_wire_graphs() {
    let mut types: Vec<_> = (1..20_000).map(array).collect();
    types.push(ty(TypeKind::Boolean {}));
    let value = table(types);
    assert!(
        value
            .validate(ValidationLimits::default())
            .unwrap_err()
            .message
            .contains("depth")
    );
    value
        .validate(ValidationLimits {
            depth: 20_000,
            ..ValidationLimits::default()
        })
        .unwrap();
    let value = table(vec![
        ty(TypeKind::Row {
            fields: vec![
                Field {
                    name: name("x"),
                    ty: TypeId(1)
                };
                10
            ],
            struct_kind: StructKind::FullyQualified,
        }),
        ty(TypeKind::Boolean {}),
    ]);
    assert!(
        value
            .validate(ValidationLimits {
                references: 10,
                ..ValidationLimits::default()
            })
            .is_err()
    );
    value
        .validate(ValidationLimits {
            references: 11,
            ..ValidationLimits::default()
        })
        .unwrap();
    assert!(
        value
            .validate(ValidationLimits {
                types: 1,
                ..ValidationLimits::default()
            })
            .is_err()
    );
    assert!(
        table(vec![ty(TypeKind::Boolean {})])
            .validate(ValidationLimits {
                depth: 0,
                ..ValidationLimits::default()
            })
            .is_err()
    );
}

#[test]
fn type_precision_is_preserved_before_lowering_clamps_it() {
    table(vec![ty(TypeKind::Decimal {
        precision: 100,
        scale: 70,
        explicit_scale: true,
    })])
    .validate(ValidationLimits::default())
    .unwrap();
    for (precision, scale) in [(0, 0), (1, 2)] {
        assert!(
            table(vec![ty(TypeKind::Decimal {
                precision,
                scale,
                explicit_scale: true
            })])
            .validate(ValidationLimits::default())
            .is_err()
        );
    }
    let mut value = table(vec![ty(TypeKind::Interval {
        start: IntervalUnit::Day,
        end: IntervalEnd::Through(IntervalUnit::Second),
        leading_precision: Precision::Unspecified,
        fractional_precision: Precision::Specified(0),
    })]);
    value.validate(ValidationLimits::default()).unwrap();
    for (start, end) in [
        (IntervalUnit::Year, IntervalUnit::Day),
        (IntervalUnit::Second, IntervalUnit::Day),
        (IntervalUnit::Month, IntervalUnit::Month),
    ] {
        value.types[0].kind = TypeKind::Interval {
            start,
            end: IntervalEnd::Through(end),
            leading_precision: Precision::Unspecified,
            fractional_precision: Precision::Unspecified,
        };
        assert!(value.validate(ValidationLimits::default()).is_err());
    }
}

#[test]
fn absent_origin_is_distinct_from_invalid_or_reversed_coordinates() {
    let mut value = table(vec![ty(TypeKind::Integer {
        width: IntegerWidth::I32,
        signed: true,
    })]);
    value.sources.push(Source {
        name: "query".into(),
        text: "SELECT '😀'".into(),
    });
    let start = Position { line: 1, column: 8 };
    let end = Position {
        line: 1,
        column: 11,
    };
    value.types[0].origin = Origin::Source {
        source: SourceId(0),
        start,
        end,
    };
    value.validate(ValidationLimits::default()).unwrap();
    for origin in [
        Origin::Source {
            source: SourceId(1),
            start,
            end,
        },
        Origin::Source {
            source: SourceId(0),
            start: Position { line: 0, column: 0 },
            end,
        },
        Origin::Source {
            source: SourceId(0),
            start: end,
            end: start,
        },
    ] {
        value.types[0].origin = origin;
        assert!(value.validate(ValidationLimits::default()).is_err());
    }
    value.types[0].origin = Origin::Absent {};
    assert!(
        value
            .validate(ValidationLimits {
                source_bytes: 1,
                ..ValidationLimits::default()
            })
            .is_err()
    );
    assert!(
        value
            .validate(ValidationLimits {
                sources: 0,
                ..ValidationLimits::default()
            })
            .is_err()
    );
}

#[test]
fn generated_ranges_preserve_partially_unknown_coordinates_without_inventing_origins() {
    let mut value = table(vec![ty(TypeKind::Boolean {})]);
    value.sources.push(Source {
        name: "query.sql".into(),
        text: "CREATE TABLE T(r ROW(a INT), s ROW(a INT));\nCREATE VIEW V AS SELECT r <=> s;"
            .into(),
    });
    let start = Position { line: 0, column: 0 };
    let end = Position {
        line: 2,
        column: 31,
    };
    value.types[0].origin = Origin::Generated {
        source: SourceId(0),
        start,
        end,
    };
    value.validate(ValidationLimits::default()).unwrap();
    let encoded = serde_json::to_string(&value).unwrap();
    let restored: TypeTable = serde_json::from_str(&encoded).unwrap();
    assert_eq!(value, restored);
    for origin in [
        Origin::Generated {
            source: SourceId(1),
            start,
            end,
        },
        Origin::Generated {
            source: SourceId(0),
            start,
            end: start,
        },
        Origin::Generated {
            source: SourceId(0),
            start: end,
            end: start,
        },
        Origin::Generated {
            source: SourceId(0),
            start: end,
            end,
        },
    ] {
        value.types[0].origin = origin;
        assert!(value.validate(ValidationLimits::default()).is_err());
    }
}

#[test]
fn checked_in_schema_matches_rust_contract() {
    let generated = serde_json::to_value(schemars::schema_for!(TypeTable)).unwrap();
    let checked_in: serde_json::Value =
        serde_json::from_str(include_str!("../type-schema.json")).unwrap();
    assert_eq!(generated, checked_in);
}

#[test]
fn every_modeled_wire_object_rejects_unknown_and_missing_fields() {
    use feldera_sql_rel_model::types::{CharacterMetadata, Coercibility, TimeZone};
    let origin = Origin::Source {
        source: SourceId(0),
        start: Position { line: 1, column: 1 },
        end: Position { line: 1, column: 2 },
    };
    let kinds = vec![
        TypeKind::Boolean {},
        TypeKind::Integer {
            width: IntegerWidth::I64,
            signed: false,
        },
        TypeKind::Decimal {
            precision: 38,
            scale: 18,
            explicit_scale: true,
        },
        TypeKind::Float {
            flavor: FloatFlavor::Real,
        },
        TypeKind::Character {
            fixed: true,
            length: Precision::Specified(10),
            metadata: CharacterMetadata::Present {
                charset: "UTF-8".into(),
                collation: "UTF-8$en_US".into(),
                coercibility: Coercibility::Explicit,
                identity: feldera_sql_rel_model::types::CollationIdentity::Named,
            },
        },
        TypeKind::Character {
            fixed: false,
            length: Precision::Unspecified,
            metadata: CharacterMetadata::Absent {},
        },
        TypeKind::Binary {
            fixed: false,
            length: Precision::Specified(0),
        },
        TypeKind::Null {},
        TypeKind::Array {
            element: TypeId(0),
            cardinality: Cardinality::Bounded(5),
        },
        TypeKind::Multiset {
            element: TypeId(0),
            cardinality: Cardinality::Unbounded,
        },
        TypeKind::Map {
            key: TypeId(0),
            value: TypeId(0),
        },
        TypeKind::Row {
            fields: vec![Field {
                name: Identifier {
                    name: "x".into(),
                    quoted: false,
                    origin: origin.clone(),
                },
                ty: TypeId(0),
            }],
            struct_kind: StructKind::PeekFields,
        },
        TypeKind::Named {
            name: vec![name("named")],
            representation: TypeId(0),
        },
        TypeKind::Interval {
            start: IntervalUnit::Year,
            end: IntervalEnd::Through(IntervalUnit::Month),
            leading_precision: Precision::Unspecified,
            fractional_precision: Precision::Unspecified,
        },
        TypeKind::Interval {
            start: IntervalUnit::Second,
            end: IntervalEnd::Single,
            leading_precision: Precision::Specified(2),
            fractional_precision: Precision::Specified(6),
        },
        TypeKind::Date {},
        TypeKind::Time {
            precision: Precision::Specified(6),
            zone: TimeZone::None,
        },
        TypeKind::Timestamp {
            precision: Precision::Unspecified,
            zone: TimeZone::WithTimeZone,
        },
        TypeKind::Geometry {
            representation: feldera_sql_rel_model::types::GeometryRepresentation::Sql,
        },
        TypeKind::Uuid {},
        TypeKind::Variant {},
        TypeKind::Symbol {},
        TypeKind::Any {},
        TypeKind::Unknown {},
        TypeKind::ColumnList {},
        TypeKind::Cursor {},
        TypeKind::Geometry {
            representation: feldera_sql_rel_model::types::GeometryRepresentation::Host,
        },
        TypeKind::Function {
            parameters: vec![TypeId(0)],
            result: TypeId(0),
        },
    ];
    let mut value = table(kinds.into_iter().map(ty).collect());
    value.sources.push(Source {
        name: "query".into(),
        text: "SELECT 1".into(),
    });
    value.types[0].origin = origin;
    value.validate(ValidationLimits::default()).unwrap();
    let encoded = serde_json::to_value(value).unwrap();
    fn object_paths(value: &serde_json::Value, prefix: String, paths: &mut Vec<String>) {
        match value {
            serde_json::Value::Object(fields) => {
                paths.push(prefix.clone());
                for (name, value) in fields {
                    object_paths(value, format!("{prefix}/{name}"), paths);
                }
            }
            serde_json::Value::Array(values) => {
                for (index, value) in values.iter().enumerate() {
                    object_paths(value, format!("{prefix}/{index}"), paths);
                }
            }
            _ => {}
        }
    }
    let mut paths = Vec::new();
    object_paths(&encoded, String::new(), &mut paths);
    assert!(paths.len() > 90);
    for pointer in paths {
        let mut changed = encoded.clone();
        changed
            .pointer_mut(&pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .insert("unknown".into(), json!(true));
        assert!(
            serde_json::from_value::<TypeTable>(changed).is_err(),
            "unknown field at {pointer}"
        );
        for field in encoded
            .pointer(&pointer)
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
        {
            let mut changed = encoded.clone();
            changed
                .pointer_mut(&pointer)
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(
                serde_json::from_value::<TypeTable>(changed).is_err(),
                "missing {pointer}/{field}"
            );
        }
    }
}

#[test]
fn draft_literal_schema_matches_rust_contract() {
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../literal-schema.json")).unwrap();
    let actual = serde_json::to_value(schemars::schema_for!(
        feldera_sql_rel_model::literals::Literal
    ))
    .unwrap();
    assert_eq!(actual, expected);
}
