//! Unit tests of the Parquet format that need no engine. The `parquet` connector suite in
//! `tests` covers the format with an engine.

use std::collections::BTreeMap;

use arrow::datatypes::Schema as ArrowSchema;
use feldera_types::program_schema::{ColumnType, Field};
use pretty_assertions::assert_eq;

use crate::test::relations;

#[test]
fn rel_to_schema() {
    use super::relation_to_parquet_schema;
    relation_to_parquet_schema(&relations::assorted("TestStruct2").fields, false)
        .expect("Can convert");
}

/// `MAP[k, v]` over a nullable `k` gives a view a nullable map key.
#[test]
fn map_with_nullable_sql_key_is_valid_arrow() {
    use crate::format::parquet::{ArrowSchemaOptions, relation_to_arrow_fields};

    let fields = vec![Field::new(
        "m".into(),
        ColumnType::map(false, ColumnType::varchar(true), ColumnType::int(true)),
    )];
    #[derive(serde::Serialize)]
    struct Row {
        m: BTreeMap<Option<String>, Option<i32>>,
    }
    let encode = |schema: &ArrowSchema, key: Option<&str>| {
        let row = Row {
            m: BTreeMap::from([(key.map(str::to_string), Some(1))]),
        };
        serde_arrow::to_record_batch(schema.fields(), &vec![row]).map(|batch| batch.num_rows())
    };
    for delta_lake in [false, true] {
        let arrow = relation_to_arrow_fields(&fields, ArrowSchemaOptions::new(delta_lake));
        arrow::array::ArrayData::new_empty(arrow[0].data_type())
            .validate()
            .unwrap();
        let schema = ArrowSchema::new(arrow);
        assert_eq!(encode(&schema, Some("a")).unwrap(), 1);
        // Parquet has no NULL map key; the changelog quotes this error.
        let error = encode(&schema, None).unwrap_err().to_string();
        assert!(
            error.contains("cannot serialize null into non-nullable array"),
            "{error}"
        );
    }
}
