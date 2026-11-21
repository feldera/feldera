use feldera_sql_rel_model::{
    LiteralLimits, ObjectLimits, TypeId, TypeTable, ValidationLimits,
    program::Program,
    source::{Origin, Position, Source, SourceId},
    types::{
        CharacterMetadata, IntervalEnd, IntervalUnit, Precision, SqlType, TimeZone, TypeKind,
        TypeSchemaVersion,
    },
};
use serde_json::json;

fn table(kind: TypeKind) -> TypeTable {
    TypeTable {
        schema_version: TypeSchemaVersion::V0,
        sources: vec![Source {
            name: "query.sql".into(),
            text: "SELECT 1".into(),
        }],
        types: vec![SqlType {
            nullable: false,
            origin: Origin::Absent {},
            kind,
        }],
        roots: vec![TypeId(0)],
    }
}

#[test]
fn all_scalar_precision_forms_retain_the_signed_adapter_domain() {
    for value in [i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
        let precision = Precision::Specified(value);
        for kind in [
            TypeKind::Decimal {
                precision: value,
                scale: value,
                explicit_scale: true,
            },
            TypeKind::Character {
                fixed: false,
                length: precision,
                metadata: CharacterMetadata::Absent {},
            },
            TypeKind::Binary {
                fixed: false,
                length: precision,
            },
            TypeKind::Time {
                precision,
                zone: TimeZone::None,
            },
            TypeKind::Timestamp {
                precision,
                zone: TimeZone::None,
            },
            TypeKind::Interval {
                start: IntervalUnit::Day,
                end: IntervalEnd::Through(IntervalUnit::Second),
                leading_precision: precision,
                fractional_precision: Precision::Unspecified,
            },
            TypeKind::Interval {
                start: IntervalUnit::Day,
                end: IntervalEnd::Through(IntervalUnit::Second),
                leading_precision: Precision::Unspecified,
                fractional_precision: precision,
            },
        ] {
            let result = table(kind).validate(ValidationLimits::default());
            if value == i32::MAX as u32 {
                // Structural compatibility is separate from lowering support.
                result.unwrap();
            } else {
                assert!(result.unwrap_err().message.contains("signed-integer range"));
            }
        }
    }
}

#[test]
fn source_coordinates_reject_unsigned_values_that_java_cannot_represent() {
    for value in [i32::MAX as u32, i32::MAX as u32 + 1, u32::MAX] {
        let mut types = table(TypeKind::Boolean {});
        types.types[0].origin = Origin::Source {
            source: SourceId(0),
            start: Position {
                line: value,
                column: value,
            },
            end: Position {
                line: value,
                column: value,
            },
        };
        let result = types.validate(ValidationLimits::default());
        if value == i32::MAX as u32 {
            // Text/range correspondence is the submission adapter's contract.
            result.unwrap();
        } else {
            assert!(result.unwrap_err().message.contains("signed-integer range"));
        }
    }
}

#[test]
fn whole_program_checks_precision_statement_coordinates_and_hint_coordinates() {
    let mut wire: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/statement-program.json")).unwrap();
    wire["properties"]["relations"][0]["metadata"]["hints"] = json!([{
        "name": "test", "position": {
            "line": 0, "column": 0, "end_line": 0, "end_column": 0, "quoted": false
        }, "inherit_path": [0], "list": [], "options": []
    }]);
    let check = |value| {
        serde_json::from_value::<Program>(value).unwrap().validate(
            ValidationLimits::default(),
            LiteralLimits::default(),
            ObjectLimits::default(),
        )
    };
    check(wire.clone()).unwrap();
    for path in [
        "/table/type_table/types/10/kind/precision/value",
        "/statements/0/source/position/end_line",
        "/properties/relations/0/metadata/hints/0/position/end_line",
        "/properties/relations/0/metadata/hints/0/inherit_path/0",
    ] {
        let mut invalid = wire.clone();
        *invalid.pointer_mut(path).unwrap() = json!(i32::MAX as u32 + 1);
        assert!(
            check(invalid)
                .unwrap_err()
                .message
                .contains("signed-integer range"),
            "accepted {path}"
        );
    }
}
