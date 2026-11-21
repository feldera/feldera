use feldera_sql_rel_model::{
    LiteralLimits, TypeId, TypeTable, ValidationLimits,
    literals::{
        ExactDecimal, IntervalStorageUnit, Literal, LiteralValue as V, MapEntry, MultisetEntry,
    },
    source::{Identifier, Origin, Position, SourceId},
    types::{
        Cardinality, CharacterMetadata, Coercibility, Field, FloatFlavor, IntegerWidth,
        IntervalEnd, IntervalUnit, Precision, SqlType, StructKind, TimeZone, TypeKind as T,
        TypeSchemaVersion,
    },
};

fn node(id: u32, value: V) -> Literal {
    Literal {
        ty: TypeId(id),
        origin: Origin::Absent {},
        value,
    }
}
fn sql_type(kind: T) -> SqlType {
    SqlType {
        nullable: false,
        origin: Origin::Absent {},
        kind,
    }
}
fn table(kinds: Vec<T>) -> TypeTable {
    TypeTable {
        schema_version: TypeSchemaVersion::V0,
        sources: vec![],
        types: kinds.into_iter().map(sql_type).collect(),
        roots: vec![TypeId(0)],
    }
}
fn check(types: &TypeTable, literal: &Literal) -> bool {
    types
        .validate_literal(
            literal,
            ValidationLimits::default(),
            LiteralLimits::default(),
        )
        .is_ok()
}
fn integer(width: IntegerWidth, signed: bool) -> T {
    T::Integer { width, signed }
}
fn name() -> Identifier {
    Identifier {
        name: "field".into(),
        quoted: true,
        origin: Origin::Absent {},
    }
}

#[test]
fn every_integer_width_checks_both_boundaries_without_float_conversion() {
    for (width, bits) in [
        (IntegerWidth::I8, 8),
        (IntegerWidth::I16, 16),
        (IntegerWidth::I32, 32),
        (IntegerWidth::I64, 64),
    ] {
        for signed in [true, false] {
            let types = table(vec![integer(width, signed)]);
            let minimum = if signed { -(1i128 << (bits - 1)) } else { 0 };
            let maximum = if signed {
                (1i128 << (bits - 1)) - 1
            } else {
                (1i128 << bits) - 1
            };
            for (value, accepted) in [
                (minimum - 1, false),
                (minimum, true),
                (maximum, true),
                (maximum + 1, false),
            ] {
                assert_eq!(
                    check(
                        &types,
                        &node(
                            0,
                            V::Integer {
                                value: value.to_string()
                            }
                        )
                    ),
                    accepted,
                    "{width:?} {signed} {value}"
                );
            }
        }
    }
}

#[test]
fn decimal_rescaling_is_exact_and_bounded_even_at_extreme_scales() {
    let types = table(vec![T::Decimal {
        precision: 5,
        scale: 2,
        explicit_scale: true,
    }]);
    for (coefficient, scale, accepted) in [
        ("12345", 2, true),
        ("1234500", 4, true),
        ("1234501", 4, false),
        ("123456", 2, false),
        ("123", 0, true),
        ("1234", 0, false),
        ("1", -2, true),
        ("1", -3, false),
        ("0", i32::MIN, true),
        ("0", i32::MAX, true),
        ("1", i32::MIN, false),
        ("1", i32::MAX, false),
        ("-00012300", 4, true),
    ] {
        assert_eq!(
            check(
                &types,
                &node(
                    0,
                    V::Decimal {
                        value: ExactDecimal {
                            coefficient: coefficient.into(),
                            scale
                        }
                    }
                )
            ),
            accepted,
            "{coefficient} {scale}"
        );
    }
}

#[test]
fn all_nested_references_and_origins_are_checked_even_for_null() {
    let mut types = table(vec![
        T::Array {
            element: TypeId(1),
            cardinality: Cardinality::Bounded(1),
        },
        integer(IntegerWidth::I8, true),
    ]);
    types.types[1].nullable = true;
    let literal = node(
        0,
        V::Array {
            elements: vec![node(1, V::Null {})],
        },
    );
    assert!(check(&types, &literal));
    types.types[1].nullable = false;
    assert!(!check(&types, &literal));
    types.types[1].nullable = true;
    let mut literal = literal;
    let V::Array { elements } = &mut literal.value else {
        unreachable!()
    };
    elements[0].origin = Origin::Source {
        source: SourceId(0),
        start: Position { line: 1, column: 1 },
        end: Position { line: 1, column: 2 },
    };
    let error = types
        .validate_literal(
            &literal,
            ValidationLimits::default(),
            LiteralLimits::default(),
        )
        .unwrap_err();
    assert_eq!(error.path, "literal.elements[0].origin");
    assert!(!check(&types, &node(90, V::Null {})));
}

#[test]
fn equal_but_distinct_type_ids_cannot_replace_a_declared_child_type() {
    let types = table(vec![
        T::Array {
            element: TypeId(1),
            cardinality: Cardinality::Unbounded,
        },
        T::Boolean {},
        T::Boolean {},
    ]);
    assert!(check(
        &types,
        &node(
            0,
            V::Array {
                elements: vec![node(1, V::Boolean { value: true })]
            }
        )
    ));
    assert!(!check(
        &types,
        &node(
            0,
            V::Array {
                elements: vec![node(2, V::Boolean { value: true })]
            }
        )
    ));
    assert!(!check(
        &types,
        &node(
            0,
            V::Array {
                elements: vec![node(1, V::Integer { value: "1".into() })]
            }
        )
    ));
}

#[test]
fn rows_maps_and_multisets_check_each_position_and_cardinality() {
    let types = table(vec![
        T::Row {
            fields: vec![
                Field {
                    name: name(),
                    ty: TypeId(1),
                },
                Field {
                    name: name(),
                    ty: TypeId(2),
                },
            ],
            struct_kind: StructKind::FullyQualified,
        },
        T::Map {
            key: TypeId(3),
            value: TypeId(4),
        },
        T::Multiset {
            element: TypeId(3),
            cardinality: Cardinality::Bounded(3),
        },
        integer(IntegerWidth::I8, true),
        T::Boolean {},
    ]);
    let map_entry = MapEntry {
        key: node(3, V::Integer { value: "1".into() }),
        value: node(4, V::Boolean { value: true }),
    };
    let multiset_entry = MultisetEntry {
        value: node(3, V::Integer { value: "2".into() }),
        multiplicity: "3".into(),
    };
    let row = node(
        0,
        V::Row {
            fields: vec![
                node(
                    1,
                    V::Map {
                        entries: vec![map_entry.clone(), map_entry],
                    },
                ),
                node(
                    2,
                    V::Multiset {
                        entries: vec![multiset_entry.clone()],
                    },
                ),
            ],
        },
    );
    assert!(check(&types, &row)); // Duplicate map keys and field names survive.
    assert!(!check(&types, &node(0, V::Row { fields: vec![] })));
    for multiplicity in ["4", "18446744073709551616", "-1"] {
        assert!(!check(
            &types,
            &node(
                2,
                V::Multiset {
                    entries: vec![MultisetEntry {
                        multiplicity: multiplicity.into(),
                        ..multiset_entry.clone()
                    }]
                }
            )
        ));
    }
    let mut types = types;
    types.types[2].kind = T::Multiset {
        element: TypeId(3),
        cardinality: Cardinality::Unbounded,
    };
    assert!(check(
        &types,
        &node(
            2,
            V::Multiset {
                entries: vec![MultisetEntry {
                    multiplicity: "18446744073709551616".into(),
                    ..multiset_entry
                }]
            }
        )
    ));
}

#[test]
fn named_types_keep_outer_nullability_and_validate_their_representation() {
    let mut types = table(vec![
        T::Named {
            name: vec![name()],
            representation: TypeId(1),
        },
        integer(IntegerWidth::I8, true),
    ]);
    assert!(check(
        &types,
        &node(
            0,
            V::Integer {
                value: "127".into()
            }
        )
    ));
    assert!(!check(
        &types,
        &node(
            0,
            V::Integer {
                value: "128".into()
            }
        )
    ));
    types.types[1].nullable = true;
    assert!(!check(&types, &node(0, V::Null {})));
    types.types[0].nullable = true;
    assert!(check(&types, &node(0, V::Null {})));
    types.types[0].kind = T::Named {
        name: vec![name()],
        representation: TypeId(0),
    };
    assert!(!check(&types, &node(0, V::Integer { value: "1".into() })));
}

#[test]
fn literal_limits_still_apply_before_type_traversal() {
    let types = table(vec![
        T::Array {
            element: TypeId(1),
            cardinality: Cardinality::Bounded(1),
        },
        T::Boolean {},
    ]);
    let literal = node(
        0,
        V::Array {
            elements: vec![node(1, V::Boolean { value: true })],
        },
    );
    assert!(
        types
            .validate_literal(
                &literal,
                ValidationLimits::default(),
                LiteralLimits {
                    nodes: 1,
                    ..LiteralLimits::default()
                }
            )
            .is_err()
    );
    assert!(
        types
            .validate_literal(
                &literal,
                ValidationLimits {
                    depth: 1,
                    ..ValidationLimits::default()
                },
                LiteralLimits::default()
            )
            .is_err()
    );
    assert!(!check(
        &types,
        &node(
            0,
            V::Array {
                elements: vec![node(1, V::Boolean { value: true }); 2]
            }
        )
    ));
}

#[test]
fn scalar_kind_length_precision_and_interval_units_are_not_interchangeable() {
    let types = table(vec![
        T::Float {
            flavor: FloatFlavor::Real,
        },
        T::Float {
            flavor: FloatFlavor::Double,
        },
        T::Character {
            fixed: false,
            length: Precision::Specified(2),
            metadata: CharacterMetadata::Present {
                charset: "UTF-8".into(),
                collation: "binary".into(),
                coercibility: Coercibility::Explicit,
                identity: feldera_sql_rel_model::types::CollationIdentity::Named,
            },
        },
        T::Binary {
            fixed: false,
            length: Precision::Specified(1),
        },
        T::Time {
            precision: Precision::Specified(3),
            zone: TimeZone::None,
        },
        T::Interval {
            start: IntervalUnit::Year,
            end: IntervalEnd::Through(IntervalUnit::Month),
            leading_precision: Precision::Unspecified,
            fractional_precision: Precision::Unspecified,
        },
        T::Timestamp {
            precision: Precision::Specified(6),
            zone: TimeZone::WithTimeZone,
        },
        T::Unknown {},
    ]);
    for (id, value, accepted) in [
        (0, V::Float32 { bits: 0x7fc00042 }, true),
        (0, V::Float64 { bits: 0 }, false),
        (
            1,
            V::Float64 {
                bits: 0x8000000000000000,
            },
            true,
        ),
        (
            2,
            V::Character {
                value: "🦀".into()
            },
            true,
        ),
        (
            2,
            V::Character {
                value: "🦀x".into(),
            },
            false,
        ),
        (3, V::Binary { value: vec![0] }, true),
        (3, V::Binary { value: vec![0, 1] }, false),
        (
            4,
            V::Time {
                nanoseconds_since_midnight: 1_000_000,
            },
            true,
        ),
        (
            4,
            V::Time {
                nanoseconds_since_midnight: 1,
            },
            false,
        ),
        (
            5,
            V::Interval {
                value: ExactDecimal {
                    coefficient: "12".into(),
                    scale: 0,
                },
                unit: IntervalStorageUnit::Months,
            },
            true,
        ),
        (
            5,
            V::Interval {
                value: ExactDecimal {
                    coefficient: "12".into(),
                    scale: 0,
                },
                unit: IntervalStorageUnit::Milliseconds,
            },
            false,
        ),
        (
            6,
            V::TimestampWithZone {
                epoch_seconds: 0,
                nanoseconds: 1000,
                zone: "UTC".into(),
            },
            true,
        ),
        (
            6,
            V::TimestampWithZone {
                epoch_seconds: 0,
                nanoseconds: 1,
                zone: "UTC".into(),
            },
            false,
        ),
        (7, V::Integer { value: "1".into() }, false),
    ] {
        assert_eq!(check(&types, &node(id, value)), accepted, "{id}");
    }
}

#[test]
fn geometry_points_preserve_bits_and_reject_inconsistent_dimensions() {
    let types = table(vec![T::Geometry {
        representation: feldera_sql_rel_model::types::GeometryRepresentation::Sql,
    }]);
    for (dimension, measures) in [(2, 0), (3, 0), (3, 1), (4, 1)] {
        for empty in [true, false] {
            let value = node(
                0,
                V::GeometryPoint {
                    srid: 4326,
                    dimension,
                    measures,
                    ordinates: if empty {
                        vec![]
                    } else {
                        vec![0x7ff8000000000042; usize::from(dimension)]
                    },
                },
            );
            assert!(check(&types, &value));
            assert_eq!(
                serde_json::from_str::<Literal>(&serde_json::to_string(&value).unwrap()).unwrap(),
                value
            );
        }
    }
    for (dimension, measures, ordinates) in [
        (1, 0, vec![]),
        (4, 0, vec![]),
        (3, 2, vec![]),
        (0, 255, vec![]),
        (2, 0, vec![0]),
    ] {
        assert!(!check(
            &types,
            &node(
                0,
                V::GeometryPoint {
                    srid: 0,
                    dimension,
                    measures,
                    ordinates
                }
            )
        ));
    }
    let value = node(
        0,
        V::GeometryPoint {
            srid: 0,
            dimension: 2,
            measures: 0,
            ordinates: vec![0, 0],
        },
    );
    assert!(
        types
            .validate_literal(
                &value,
                ValidationLimits::default(),
                LiteralLimits {
                    payload_bytes: 15,
                    ..LiteralLimits::default()
                }
            )
            .is_err()
    );
}

#[test]
fn local_zoned_clocks_preserve_calendar_fields_and_declared_precision() {
    let types = table(vec![T::Timestamp {
        precision: Precision::Specified(6),
        zone: TimeZone::WithTimeZone,
    }]);
    let value = node(
        0,
        V::LocalTimestampWithZone {
            epoch_days: 19_792,
            nanoseconds_since_midnight: 9_000_000_000_000,
            zone: "America/New_York".into(),
        },
    );
    assert!(check(&types, &value));
    let restored: Literal = serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
    assert_eq!(restored, value);
    for (nanoseconds_since_midnight, zone) in [
        (9_000_000_000_001, "America/New_York"),
        (86_400_000_000_000, "America/New_York"),
        (9_000_000_000_000, ""),
    ] {
        assert!(!check(
            &types,
            &node(
                0,
                V::LocalTimestampWithZone {
                    epoch_days: 19_792,
                    nanoseconds_since_midnight,
                    zone: zone.into(),
                }
            )
        ));
    }
}

#[test]
fn symbol_domains_preserve_sql_members_instead_of_rust_or_java_type_names() {
    let types = table(vec![T::Symbol {}]);
    for (domain, names) in [
        (
            "time_unit_range",
            vec![
                "YEAR_TO_MONTH",
                "DAY_TO_SECOND",
                "ISODOW",
                "NANOSECOND",
                "MILLENNIUM",
            ],
        ),
        ("trim_mode", vec!["BOTH", "LEADING", "TRAILING"]),
    ] {
        for name in names {
            let literal = node(
                0,
                V::Symbol {
                    domain: domain.into(),
                    name: name.into(),
                },
            );
            assert!(check(&types, &literal));
            let restored: Literal =
                serde_json::from_str(&serde_json::to_string(&literal).unwrap()).unwrap();
            assert_eq!(restored, literal);
        }
    }
}

#[test]
fn unknown_or_mismatched_symbol_members_fail_before_import() {
    let types = table(vec![T::Symbol {}]);
    for (domain, name) in [
        ("java.lang.Runtime", "YEAR"),
        ("time_unit_range", "DAY_TO_WEEK"),
        ("time_unit_range", "year"),
        ("trim_mode", "YEAR"),
        ("time_unit_range", "BOTH"),
        ("trim_mode", "INTERNAL"),
        ("", "YEAR"),
        ("trim_mode", ""),
    ] {
        assert!(
            !check(
                &types,
                &node(
                    0,
                    V::Symbol {
                        domain: domain.into(),
                        name: name.into()
                    }
                )
            ),
            "accepted {domain}:{name}"
        );
    }
}
