//! The CSV record layer: a CSV record read into a type that the SQL compiler generates.

pub mod deserializer;

pub use deserializer::{byte_record_deserializer, string_record_deserializer};

#[cfg(test)]
mod tests {
    use super::string_record_deserializer;
    use feldera_types::deserialize_table_record;
    use feldera_types::serde_with_context::{DeserializeWithContext, SqlSerdeConfig};

    use feldera_sqllib::{SqlString, Variant};
    use std::collections::BTreeMap;

    #[derive(Debug, Eq, PartialEq)]
    #[allow(non_snake_case)]
    struct CaseSensitive {
        fIeLd1: bool,
        field2: String,
        field3: Option<u8>,
    }

    deserialize_table_record!(CaseSensitive["CaseSensitive", 3] {
    (fIeLd1, "fIeLd1", true, bool, |_| None),
    (field2, "field2", true, String, |_| None),
    (field3, "field3", false, Option<u8>, |_| Some(None))
    });

    #[test]
    fn csv() {
        let data = r#"true,"foo",5
true,bar,buzz"#;
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        let mut records = rdr.into_records();
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .unwrap(),
            CaseSensitive {
                fIeLd1: true,
                field2: "foo".to_string(),
                field3: Some(5)
            }
        );
        assert_eq!(
            CaseSensitive::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .map_err(|e| e.to_string()),
            Err(
                r#"{"field":"field3","description":"field 2: invalid digit found in string"}"#
                    .to_string()
            )
        );
    }

    #[derive(Debug, Eq, PartialEq)]
    struct VariantRow {
        object: Variant,
        positive: Variant,
        negative: Variant,
        float: Variant,
        integral_float: Variant,
        huge: Variant,
        huge_negative: Variant,
        boolean: Variant,
        null: Variant,
    }

    deserialize_table_record!(VariantRow["VariantRow", Variant, 9] {
    (object, "object", true, Variant, |_| None),
    (positive, "positive", true, Variant, |_| None),
    (negative, "negative", true, Variant, |_| None),
    (float, "float", true, Variant, |_| None),
    (integral_float, "integral_float", true, Variant, |_| None),
    (huge, "huge", true, Variant, |_| None),
    (huge_negative, "huge_negative", true, Variant, |_| None),
    (boolean, "boolean", true, Variant, |_| None),
    (null, "null", true, Variant, |_| None)
    });

    /// The `csv` crate infers a field's type rather than always reporting text,
    /// so a bare `123` or `true` reaches the VARIANT deserializer as a number
    /// or a boolean. Each must land on the value that parsing the same text as
    /// JSON produces.
    #[test]
    fn csv_variant() {
        let data =
            r#""{""a"": 1}",123,-4,1.5,1.0,20000000000000000000,-20000000000000000000,true,null"#;
        let rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        let mut records = rdr.into_records();

        assert_eq!(
            VariantRow::deserialize_with_context(
                &mut string_record_deserializer(&records.next().unwrap().unwrap(), None),
                &SqlSerdeConfig::default()
            )
            .unwrap(),
            VariantRow {
                object: Variant::Map(
                    BTreeMap::from([(
                        Variant::String(SqlString::from_ref("a")),
                        Variant::UBigInt(1)
                    )])
                    .into()
                ),
                positive: Variant::UBigInt(123),
                negative: Variant::BigInt(-4),
                // `serde_json` with `arbitrary_precision` reports a fractional
                // number through its private token, so a CSV float has always
                // arrived as a decimal. That holds for a float whose value is
                // integral too, which is only true while the visitor renders
                // it with `Debug`.
                float: Variant::SqlDecimal((15, 1)),
                integral_float: Variant::SqlDecimal((1, 0)),
                // Outside `u64`/`i64`, which the csv crate reports as a 128-bit
                // integer rather than as text.
                huge: Variant::SqlDecimal((20_000_000_000_000_000_000, 0)),
                huge_negative: Variant::SqlDecimal((-20_000_000_000_000_000_000, 0)),
                boolean: Variant::Boolean(true),
                null: Variant::VariantNull,
            }
        );
    }
}
