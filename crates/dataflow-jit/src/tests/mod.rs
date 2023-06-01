#![cfg(test)]

mod issue_141;
mod issue_145;
mod issue_146;
mod issue_147;
mod issue_181;
mod issue_184;
mod issue_186;
mod issue_189;

use crate::ir::literal::{RowLiteral, StreamCollection};
use std::collections::BTreeMap;

fn jitset_to_map(data: &[(RowLiteral, i32)]) -> BTreeMap<RowLiteral, i32> {
    let mut result = BTreeMap::new();
    for (r, w) in data.iter() {
        result.insert(r.clone(), *w);
    }
    result
}

fn subtract(
    left: &BTreeMap<RowLiteral, i32>,
    right: &BTreeMap<RowLiteral, i32>,
) -> BTreeMap<RowLiteral, i32> {
    let mut result = left.clone();
    for (r, v) in right.iter() {
        match result.get(r) {
            None => {
                result.insert(r.clone(), -*v);
            }
            Some(lv) => {
                let diff = lv - v;
                if diff == 0 {
                    result.remove(r);
                } else {
                    result.entry(r.clone()).and_modify(|lv| *lv = diff);
                }
            }
        }
    }
    result
}

fn must_equal_sc(left: &StreamCollection, right: &StreamCollection) -> bool {
    match (left, right) {
        (StreamCollection::Set(left_rows), StreamCollection::Set(right_rows)) => {
            let left = jitset_to_map(left_rows);
            let right = jitset_to_map(right_rows);
            let diff = subtract(&left, &right);
            for (r, v) in diff.iter() {
                if *v < 0 {
                    println!("R: {:?}x{:?}", r, -v);
                } else {
                    println!("L: {:?}x{:?}", r, v);
                }
            }
            diff.is_empty()
        }

        (StreamCollection::Map(_), StreamCollection::Map(_)) => {
            todo!()
        }

        _ => {
            println!("Collections of different types");
            false
        }
    }
}
