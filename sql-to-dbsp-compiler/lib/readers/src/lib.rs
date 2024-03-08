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
use derive_more::{Add, AddAssign, From, Into, Neg, Sub};
use paste::paste;
use rkyv::Archive;
use serde::{Deserialize, Serialize};
use size_of::*;
use sqllib::{WSet, Weight};
use sqlx::{
    any::AnyRow, migrate::MigrateDatabase, sqlite::SqliteConnection, sqlite::SqliteRow,
    AnyConnection, Connection, Executor, Row,
};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::ops::{Add, Neg};
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
        .map(|x| Tup2(x.unwrap(), Weight::one()))
        .collect();
    WSet::<T>::from_keys((), vec)
}

pub fn read_db<T>(conn_str: &str, table_name: &str, mapper: impl Fn(&AnyRow) -> T) -> WSet<T>
where
    T: DBData + for<'de> serde::Deserialize<'de>,
{
    let rows = task::block_on(async move {
        let mut conn = AnyConnection::connect(conn_str).await.unwrap();
        let mut query = "SELECT * FROM ".to_owned();
        query.push_str(table_name);
        sqlx::query(query.as_str())
            .fetch_all(&mut conn)
            .await
            .unwrap()
    });
    let vec = rows
        .iter()
        .map(|row| Tup2(mapper(row), Weight::one()))
        .collect();
    WSet::from_keys((), vec)
}

#[cfg(test)]
dbsp::declare_tuples! {
    Tuple3<T0, T1, T2>,
}

#[test]
fn csv_test() {
    let src = read_csv::<Tuple3<bool, Option<String>, Option<u32>>>("src/test.csv");
    assert_eq!(
        zset!(
            Tuple3::new(true, Some(String::from("Mihai")),Some(0)) => 1,
            Tuple3::new(false, Some(String::from("Leonid")),Some(1)) => 1,
            Tuple3::new(true, Some(String::from("Chase")),Some(2)) => 1,
            Tuple3::new(false, Some(String::from("Gerd")),Some(3)) => 1,
            Tuple3::new(true, None, None) => 1,
            Tuple3::new(false, Some(String::from("Nina")),None) => 1,
            Tuple3::new(true, None, Some(6)) => 1,
        ),
        src
    );
}

#[async_std::test]
async fn sql_test_sqlite() {
    let conn_str = "sqlite:///tmp/test.db";
    if !sqlx::Sqlite::database_exists(conn_str).await.unwrap() {
        sqlx::Sqlite::create_database(conn_str).await.unwrap();
        let mut conn = SqliteConnection::connect(conn_str).await.unwrap();
        conn.execute("create table t1(id integer, name varchar, flag bool)")
            .await
            .unwrap();
        conn.execute("insert into t1 values(73, 'name1', true)")
            .await
            .unwrap();
        conn.close();
    }
    let zset = read_db::<Tuple3<i32, String, bool>>(conn_str, "t1", |row: &AnyRow| {
        Tuple3::new(row.get(0), row.get(1), row.get(2))
    });
    assert_eq!(
        zset!(
            Tuple3::new(73i32, String::from("name1"), true) => 1i64,
        ),
        zset
    );
}
