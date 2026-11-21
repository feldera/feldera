use std::cmp::Ordering;

use feldera_fxp::Fixed;
use feldera_native_probe::{KeyColumn, KeyField, KeyLayout};
use feldera_sqllib::SqlString;

type Decimal = Fixed<6, 2>;
type Row = (Option<SqlString>, Option<Decimal>, Option<i64>);

fn compare_field<T: Ord>(left: &Option<T>, right: &Option<T>, field: KeyField) -> Ordering {
    match (left, right) {
        (Some(left), Some(right)) if field.descending => right.cmp(left),
        (Some(left), Some(right)) => left.cmp(right),
        _ if field.nulls_first => left.cmp(right),
        _ => right.cmp(left),
    }
}

fn compare_rows(left: &Row, right: &Row, fields: &[KeyField]) -> Ordering {
    compare_field(&left.0, &right.0, fields[0])
        .then_with(|| compare_field(&left.1, &right.1, fields[1]))
        .then_with(|| compare_field(&left.2, &right.2, fields[2]))
}

#[test]
fn mixed_keys_match_sql_string_decimal_and_bigint_order() {
    let texts = [
        None,
        Some(""),
        Some("\0"),
        Some("a"),
        Some("a\0"),
        Some("a\0b"),
        Some("aa"),
        Some("é"),
        Some("🐍"),
        Some("abcdefghijklmno"),
        Some("abcdefghijklmnop"),
    ];
    let decimals = [
        None,
        Some(Decimal::MIN),
        Decimal::new(-1, 2),
        Decimal::new(0, 2),
        Some(Decimal::MAX),
    ];
    let integers = [None, Some(i64::MIN), Some(i64::MAX)];
    let mut rows = Vec::new();
    for text in texts {
        for decimal in decimals {
            for integer in integers {
                rows.push((text.map(SqlString::from_ref), decimal, integer));
            }
        }
    }
    let text: Vec<_> = rows
        .iter()
        .map(|row| row.0.as_ref().map(SqlString::str))
        .collect();
    let decimals: Vec<_> = rows
        .iter()
        .map(|row| row.1.map(|value| value.mantissa()))
        .collect();
    let integers: Vec<_> = rows.iter().map(|row| row.2).collect();
    let columns = [
        KeyColumn::Text(&text),
        KeyColumn::WideInt(&decimals),
        KeyColumn::BigInt(&integers),
    ];

    for policy in 0..64 {
        let fields: Vec<_> = (0..3)
            .map(|column| KeyField {
                column,
                descending: policy & (1 << (2 * column)) != 0,
                nulls_first: policy & (2 << (2 * column)) != 0,
            })
            .collect();
        let layout = KeyLayout::new(columns.len(), fields.clone()).unwrap();
        let keys = layout.encode_columns(rows.len(), &columns).unwrap();
        for (i, left) in rows.iter().enumerate() {
            for (j, right) in rows.iter().enumerate() {
                assert_eq!(
                    keys.key(i).unwrap().cmp(keys.key(j).unwrap()),
                    compare_rows(left, right, &fields),
                    "policy {policy}: rows {i} and {j}"
                );
            }
        }
        assert_eq!(keys.rows(), rows.len());
        assert_eq!(keys.key(usize::MAX), None);
    }
}
