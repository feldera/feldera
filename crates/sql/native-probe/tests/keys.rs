use feldera_native_probe::{Batch, KeyField, KeyLayout};
use std::cmp::Ordering;

#[test]
fn cell_access_distinguishes_nulls_and_invalid_coordinates() {
    let batch = Batch::from_rows(2, &[vec![None, Some(i64::MIN)]]).unwrap();
    assert_eq!(batch.get(0, 0), Some(None));
    assert_eq!(batch.get(0, 1), Some(Some(i64::MIN)));
    assert_eq!(batch.get(0, 2), None);
    assert_eq!(batch.get(1, 0), None);
    assert_eq!(batch.get(usize::MAX, usize::MAX), None);
}

fn compare(left: &[Option<i64>], right: &[Option<i64>], fields: &[KeyField]) -> Ordering {
    for field in fields {
        let order = match (left[field.column], right[field.column]) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) => {
                if field.nulls_first {
                    Ordering::Less
                } else {
                    Ordering::Greater
                }
            }
            (Some(_), None) => {
                if field.nulls_first {
                    Ordering::Greater
                } else {
                    Ordering::Less
                }
            }
            (Some(a), Some(b)) => {
                if field.descending {
                    b.cmp(&a)
                } else {
                    a.cmp(&b)
                }
            }
        };
        if !order.is_eq() {
            return order;
        }
    }
    Ordering::Equal
}

#[test]
fn byte_order_matches_each_direction_and_null_placement() {
    let values = [
        None,
        Some(i64::MIN),
        Some(-1),
        Some(0),
        Some(1),
        Some(i64::MAX),
    ];
    let rows: Vec<_> = values
        .iter()
        .flat_map(|a| values.iter().map(move |b| vec![*a, *b]))
        .collect();
    let batch = Batch::from_rows(2, &rows).unwrap();
    for policy in 0..16 {
        let fields: Vec<_> = (0..2)
            .map(|column| KeyField {
                column,
                descending: policy & (1 << (column * 2)) != 0,
                nulls_first: policy & (2 << (column * 2)) != 0,
            })
            .collect();
        let keys = KeyLayout::new(2, fields.clone())
            .unwrap()
            .encode(&batch)
            .unwrap();
        for (i, left) in rows.iter().enumerate() {
            for (j, right) in rows.iter().enumerate() {
                assert_eq!(
                    keys.key(i).unwrap().cmp(keys.key(j).unwrap()),
                    compare(left, right, &fields),
                    "policy {policy}: {left:?} vs {right:?}"
                );
            }
        }
    }
}

#[test]
fn sorted_merge_preserves_duplicates_and_signed_weight_cancellation() {
    let fields = vec![
        KeyField {
            column: 1,
            descending: true,
            nulls_first: false,
        },
        KeyField {
            column: 0,
            descending: false,
            nulls_first: true,
        },
    ];
    let rows = vec![
        vec![Some(1), None],
        vec![Some(1), Some(0)],
        vec![None, Some(i64::MAX)],
        vec![Some(1), Some(0)],
        vec![Some(i64::MIN), Some(0)],
        vec![Some(1), None],
    ];
    let weights = [2_i64, 3, -1, -3, 7, -1];
    let keys = KeyLayout::new(2, fields.clone())
        .unwrap()
        .encode(&Batch::from_rows(2, &rows).unwrap())
        .unwrap();
    let mut reference: Vec<_> = (0..rows.len()).collect();
    reference.sort_by(|a, b| compare(&rows[*a], &rows[*b], &fields));
    let mut encoded: Vec<_> = (0..rows.len()).collect();
    encoded.sort_by(|a, b| keys.key(*a).cmp(&keys.key(*b)));
    assert_eq!(encoded, reference);
    let mut merged = Vec::<(usize, i64)>::new();
    for row in encoded {
        if let Some((last, weight)) = merged.last_mut()
            && keys.key(*last) == keys.key(row)
        {
            *weight += weights[row];
            continue;
        }
        merged.push((row, weights[row]));
    }
    merged.retain(|(_, weight)| *weight != 0);
    let merged: Vec<_> = merged
        .into_iter()
        .map(|(row, weight)| (rows[row].clone(), weight))
        .collect();
    assert_eq!(
        merged,
        vec![
            (vec![None, Some(i64::MAX)], -1),
            (vec![Some(i64::MIN), Some(0)], 7),
            (vec![Some(1), None], 1)
        ]
    );
}

#[test]
fn key_bounds_empty_keys_and_the_highest_null_bit_are_explicit() {
    let field = KeyField {
        column: 63,
        descending: false,
        nulls_first: true,
    };
    let layout = KeyLayout::new(64, vec![field]).unwrap();
    let mut row = vec![Some(0); 64];
    row[63] = None;
    let keys = layout
        .encode(&Batch::from_rows(64, &[row]).unwrap())
        .unwrap();
    assert_eq!(keys.key(0), Some(&[0; 9][..]));
    assert!(keys.key(1).is_none());
    assert!(KeyLayout::new(63, vec![field]).is_err());
    assert!(layout.encode(&Batch::from_rows(0, &[]).unwrap()).is_err());
    let empty = KeyLayout::new(0, vec![])
        .unwrap()
        .encode(&Batch::from_rows(0, &[vec![], vec![]]).unwrap())
        .unwrap();
    assert_eq!(empty.rows(), 2);
    assert_eq!(empty.byte_len(), 0);
    assert_eq!(empty.key(1), Some(&[][..]));
    assert!(empty.key(2).is_none());
    let repeated = KeyField { column: 0, ..field };
    let large = Batch::from_rows(1, &vec![vec![Some(0)]; 65_536]).unwrap();
    assert!(
        KeyLayout::new(1, vec![repeated; 64])
            .unwrap()
            .encode(&large)
            .is_err()
    );
}

#[test]
fn wide_keys_preserve_unsigned_time_order_above_the_signed_limit() {
    let times = [0_u64, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
    let rows: Vec<_> = times
        .iter()
        .map(|&value| vec![Some(i128::from(value))])
        .collect();
    for descending in [false, true] {
        let layout = KeyLayout::new(
            1,
            vec![KeyField {
                column: 0,
                descending,
                nulls_first: true,
            }],
        )
        .unwrap();
        let keys = layout.encode_wide(&rows).unwrap();
        for (i, left) in times.iter().enumerate() {
            for (j, right) in times.iter().enumerate() {
                let expected = if descending {
                    right.cmp(left)
                } else {
                    left.cmp(right)
                };
                assert_eq!(keys.key(i).unwrap().cmp(keys.key(j).unwrap()), expected);
            }
        }
    }
}
