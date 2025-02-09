//! Readers that can read WSet data from various sources.

#![allow(unused_imports)]
#![allow(non_snake_case)]
#![allow(dead_code)]

use async_std::task;
use csv::{Reader, ReaderBuilder};
use dbsp::utils::Tup2;
use dbsp::{
    algebra::{AddAssignByRef, AddByRef, HasOne, HasZero, MulByRef, NegByRef, ZRingValue, ZSet},
    trace::Batch,
    zset, DBData, DBWeight, OrdZSet,
};
use derive_more::{From, Into, Neg, Sub};
use feldera_sqllib::{WSet, Weight};
use paste::paste;
use rkyv::Archive;
use serde::{Deserialize, Serialize};
use size_of::*;
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::Neg;
use std::{fs::File, io::BufReader, path::Path};

pub fn read_csv<T>(source_file_path: &str) -> WSet<T>
where
    T: DBData + for<'de> serde::Deserialize<'de>,
{
    let path = Path::new(source_file_path);
    let file =
        BufReader::new(File::open(path).unwrap_or_else(|error| {
            panic!("failed to open file '{}': {}", source_file_path, error,)
        }));

    let mut csv_reader = csv::ReaderBuilder::new()
        .delimiter(b',')
        .has_headers(false)
        .from_reader(file);
    let vec = csv_reader
        .deserialize()
        .map(|x| Tup2::new(x.unwrap(), Weight::one()))
        .collect();
    WSet::<T>::from_keys((), vec)
}

#[test]
fn csv_test() {
    use dbsp::utils::Tup3;
    let src = read_csv::<Tup3<bool, Option<String>, Option<u32>>>("src/test.csv");
    assert_eq!(
        zset!(
            Tup3::new(true, Some(String::from("Mihai")),Some(0)) => 1,
            Tup3::new(false, Some(String::from("Leonid")),Some(1)) => 1,
            Tup3::new(true, Some(String::from("Chase")),Some(2)) => 1,
            Tup3::new(false, Some(String::from("Gerd")),Some(3)) => 1,
            Tup3::new(true, None, None) => 1,
            Tup3::new(false, Some(String::from("Nina")),None) => 1,
            Tup3::new(true, None, Some(6)) => 1,
        ),
        src
    );
}
