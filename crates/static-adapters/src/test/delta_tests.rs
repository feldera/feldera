//! How the static engine's compiled records serialize typed VARIANT values for the Delta
//! writer. The shared suites see a VARIANT as its JSON text, which loses the types.

use std::collections::BTreeMap;

use arrow::datatypes::Schema as ArrowSchema;
use dbsp_adapters::format::parquet::relation_to_arrow_fields;
use dbsp_adapters::integrated::delta_table::{delta_input_serde_config, delta_output_serde_config};
use feldera_sqllib::Variant;
use feldera_types::serde_with_context::DeserializeWithContext;
use feldera_types::serde_with_context::serialize::SerializeWithContextWrapper;
use proptest::prelude::{Arbitrary, Strategy};
use proptest::strategy::ValueTree;
use proptest::test_runner::TestRunner;

use crate::test::DeltaTestStruct;
use crate::test::suites::delta_table::{delta_schema_options, init_logging};

/// One deterministic test row (even `bigint` so `bigint % 2 = 0` filters pass).
fn delta_test_record(bigint: i64) -> DeltaTestStruct {
    let mut runner = TestRunner::default();
    let mut record = DeltaTestStruct::arbitrary()
        .new_tree(&mut runner)
        .unwrap()
        .current();
    record.bigint = bigint;
    record
}

/// A typed value survives the Delta writer's serialization.
///
/// `DeltaTestStruct::variant` is always a map of strings, and the round trips
/// feed their data through a JSON file, which turns a date into a string and
/// binary into an array before the writer ever sees it. So nothing covered a
/// date or a decimal reaching the Delta writer as itself. This drives the
/// connector's own serde configuration over the connector's own Arrow schema,
/// and reads the batch back the way the input connector does.
#[test]
fn delta_output_typed_variant_serialization_test() {
    use chrono::NaiveDate;
    use feldera_sqllib::{ByteArray, Date, SqlString, Timestamp};
    use feldera_types::transport::delta_table::DeltaVariantEncoding;

    init_logging();

    let key = |k: &str| Variant::String(SqlString::from_ref(k));
    let variant = Variant::Map(
        BTreeMap::from([
            (
                key("date"),
                Variant::Date(Date::from_date(
                    NaiveDate::from_ymd_opt(2026, 9, 1).unwrap(),
                )),
            ),
            (key("dec"), Variant::SqlDecimal((12_345, 2))),
            (key("bin"), Variant::Binary(ByteArray::new(&[1, 2, 3]))),
            (key("int"), Variant::BigInt(42)),
            (
                key("ts"),
                Variant::Timestamp(Timestamp::from_microseconds(1_756_684_800_000_000)),
            ),
        ])
        .into(),
    );

    let mut record = delta_test_record(0);
    record.variant = variant.clone();

    let arrow_schema = ArrowSchema::new(relation_to_arrow_fields(
        &DeltaTestStruct::schema(),
        delta_schema_options(),
    ));
    let batch = serde_arrow::to_record_batch(
        arrow_schema.fields(),
        &SerializeWithContextWrapper::new(
            &vec![record.clone()],
            &delta_output_serde_config(DeltaVariantEncoding::Variant),
        ),
    )
    .unwrap();

    // The column is the binary encoding, not JSON text.
    assert!(
        matches!(
            batch
                .schema()
                .field_with_name("variant")
                .unwrap()
                .data_type(),
            arrow::datatypes::DataType::Struct(_)
        ),
        "variant column is {:?}",
        batch
            .schema()
            .field_with_name("variant")
            .unwrap()
            .data_type()
    );

    let deserializer = serde_arrow::Deserializer::from_record_batch(&batch).unwrap();
    let read: Vec<DeltaTestStruct> =
        Vec::deserialize_with_context(deserializer, &delta_input_serde_config()).unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(read[0].variant, variant);
}
