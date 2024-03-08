// TODO: The issue here is that f64 doesn't implement From<ZWeight>.
fn main() {
    todo!()
}

/*
//! ML feature engineering demo: Feature extraction queries for ML-based fraud
//! detection.
//!
//! ```sql
//! CREATE TABLE demographics (
//!     cc_num FLOAT64,
//!     first INTEGER,
//!     gender INTEGER,
//!     street INTEGER,
//!     city INTEGER,
//!     state INTEGER,
//!     zip INTEGER,
//!     lat FLOAT64,
//!     long FLOAT64,
//!     city_pop INTEGER,
//!     job INTEGER,
//!     dob DATE
//! );
//!
//! CREATE TABLE transactions (
//!     trans_date_trans_time TIMESTAMP,
//!     cc_num FLOAT64,
//!     merchant INTEGER,
//!     category INTEGER,
//!     amt FLOAT64,
//!     trans_num STRING,
//!     unix_time INTEGER,
//!     merch_lat FLOAT64,
//!     merch_long FLOAT64,
//!     is_fraud INTEGER,
//! );
//!
//! SELECT
//!     EXTRACT (dayofweek FROM trans_date_trans_time) AS day,
//!     DATE_DIFF(EXTRACT(DATE FROM trans_date_trans_time),dob, YEAR) AS age,
//!     ST_DISTANCE(ST_GEOGPOINT(long,lat), ST_GEOGPOINT(merch_long, merch_lat)) AS distance,
//!     TIMESTAMP_DIFF(trans_date_trans_time, last_txn_date , MINUTE) AS trans_diff,
//!     AVG(amt) OVER(
//!                 PARTITION BY   CAST(cc_num AS NUMERIC)
//!                 ORDER BY unix_time
//!                 -- 1 week is 604800  seconds
//!                 RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS avg_spend_pw,
//!     AVG(amt) OVER(
//!                 PARTITION BY  CAST(cc_num AS NUMERIC)
//!                 ORDER BY unix_time
//!                 -- 1 month(30 days) is 2592000 seconds
//!                 RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS avg_spend_pm,
//!     COUNT(*) OVER(
//!                 PARTITION BY  CAST(cc_num AS NUMERIC)
//!                 ORDER BY unix_time
//!                 -- 1 day is 86400  seconds
//!                 RANGE BETWEEN 86400 PRECEDING AND 1 PRECEDING) AS trans_freq_24,
//!     category,
//!     amt,
//!     state,
//!     job,
//!     unix_time,
//!     city_pop,
//!     merchant,
//!     is_fraud
//! FROM (
//!     SELECT t1.*,t2.* EXCEPT(cc_num),
//!            LAG(trans_date_trans_time) OVER (PARTITION BY t1.cc_num ORDER BY trans_date_trans_time ASC) AS last_txn_date,
//!     FROM
//!         transactions t1 LEFT JOIN  demographics t2 ON t1.cc_num = t2.cc_num
//! )
//! ```

use anyhow::Result;
use chrono::{NaiveDate, NaiveDateTime};
use clap::Parser;
use crossbeam::channel::bounded;
use csv::Reader as CsvReader;
use dbsp::dynamic::{DynData, DynDataTyped};
use dbsp::{
    algebra::F64,
    mimalloc::MiMalloc,
    operator::{
        time_series::{OrdPartitionedIndexedZSet, RelOffset, RelRange},
        Avg,
    },
    utils::{Tup2, Tup3},
    DBSPHandle, OrdIndexedZSet, Runtime, Stream, ZSetHandle,
};
use itertools::Itertools;
use rkyv::{Archive, Serialize};
use serde::de::Error as _;
use size_of::SizeOf;
use std::{
    hash::Hash,
    io::{stdin, Read},
    thread::spawn,
};
use time::Instant;

// TODO: add a test harness.

#[global_allocator]
static ALLOC: MiMalloc = MiMalloc;

const DEFAULT_BATCH_SIZE: &str = "10000";

const DAY_IN_SECONDS: i64 = 24 * 3600;

#[derive(
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct QueryResult {
    // day: Weekday,
    // age: u32,
    // distance: u32,
    // trans_diff: u32,
    avg_spend_pw: Option<F64>,
    avg_spend_pm: Option<F64>,
    trans_freq_24: u32,
    category: u32,
    amt: F64,
    state: u32,
    job: u32,
    unix_time: i32,
    city_pop: u32,
    merchant: u32,
    is_fraud: u32,
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct Demographics {
    cc_num: F64,
    first: u32,
    gender: u32,
    street: u32,
    city: u32,
    state: u32,
    zip: u32,
    lat: F64,
    long: F64,
    city_pop: u32,
    job: u32,
    dob: NaiveDate,
}

#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct Transaction {
    #[serde(deserialize_with = "naive_date_time_from_str")]
    trans_date_trans_time: NaiveDateTime,
    cc_num: F64,
    merchant: u32,
    category: u32,
    amt: F64,
    trans_num: String,
    unix_time: i32,
    merch_lat: F64,
    merch_long: F64,
    is_fraud: u32,
}

fn naive_date_time_from_str<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<NaiveDateTime, D::Error> {
    let s: String = serde::Deserialize::deserialize(d)?;

    match NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
        Ok(o) => Ok(o),
        Err(err) => Err(D::Error::custom(err)),
    }
}

#[derive(Debug, Clone, Parser)]
struct Args {
    #[clap(long)]
    workers: usize,

    #[clap(long, default_value = DEFAULT_BATCH_SIZE)]
    batch_size: usize,

    #[clap(long, default_value = "benches/fraud_data/demographics.csv")]
    demographics: String,

    #[clap(long, default_value = "benches/fraud_data/transactions.csv")]
    transactions: String,

    #[clap(long)]
    stdin: bool,

    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}

type Weight = i32;

type EnrichedTransactions = OrdIndexedZSet<Tup2<F64, i64>, Tup2<Transaction, Demographics>>;
type AverageSpendingPerWeek =
    OrdPartitionedIndexedZSet<F64, i64, DynDataTyped<i64>, Option<F64>, DynData>;
type AverageSpendingPerMonth =
    OrdPartitionedIndexedZSet<F64, i64, DynDataTyped<i64>, Option<F64>, DynData>;
type TransactionFrequency =
    OrdPartitionedIndexedZSet<F64, i64, DynDataTyped<i64>, Option<i32>, DynData>;

struct FraudBenchmark {
    dbsp: DBSPHandle,
    demographics: ZSetHandle<Demographics>,
    transactions: ZSetHandle<Transaction>,
}

impl FraudBenchmark {
    fn new(workers: usize) -> Self {
        let (dbsp, (hdemographics, htransactions)) = Runtime::init_circuit(workers, |circuit| {
            let (demographics, hdemographics) = circuit.add_input_zset::<Demographics>();
            let (transactions, htransactions) = circuit.add_input_zset::<Transaction>();

            let amounts = transactions.map_index(|t| {
                let timestamp = t.trans_date_trans_time.and_utc().timestamp();
                (t.cc_num, Tup2(timestamp, t.amt))
            });

            let transactions_by_ccnum = transactions.map_index(|t| (t.cc_num, t.clone()));
            let demographics_by_ccnum = demographics.map_index(|d| (d.cc_num, d.clone()));

            let enriched_transactions: Stream<_, EnrichedTransactions> = transactions_by_ccnum
                .join_index(&demographics_by_ccnum, |cc_num, tran, dem| {
                    let timestamp = tran.trans_date_trans_time.and_utc().timestamp();
                    Some((Tup2(*cc_num, timestamp), Tup2(tran.clone(), dem.clone())))
                });

            // AVG(amt) OVER(
            //     PARTITION BY CAST(cc_num AS NUMERIC)
            //     ORDER BY unix_time
            //     -- 1 week is 604800  seconds
            //     RANGE BETWEEN 604800  PRECEDING AND 1 PRECEDING) AS avg_spend_pw,
            let avg_spend_pw: Stream<_, AverageSpendingPerWeek> = amounts
                .as_partitioned_zset()
                .partitioned_rolling_aggregate_linear(
                    |amt| Avg::new(*amt, 1),
                    |avg| avg.compute_avg().unwrap(),
                    RelRange::new(RelOffset::Before(DAY_IN_SECONDS * 7), RelOffset::Before(1)),
                );

            let avg_spend_pw_indexed = avg_spend_pw
                .map_index(|(cc_num, Tup2(ts, avg_amt))| (Tup2(*cc_num, *ts), *avg_amt));

            // AVG(amt) OVER(
            //     PARTITION BY  CAST(cc_num AS NUMERIC)
            //     ORDER BY unix_time
            //     -- 1 month(30 days) is 2592000 seconds
            //     RANGE BETWEEN 2592000 PRECEDING AND 1 PRECEDING) AS avg_spend_pm,
            let avg_spend_pm: Stream<_, AverageSpendingPerMonth> = amounts
                .partitioned_rolling_aggregate_linear(
                    |amt| Avg::new(*amt, 1),
                    |avg| avg.compute_avg().unwrap(),
                    RelRange::new(RelOffset::Before(DAY_IN_SECONDS * 30), RelOffset::Before(1)),
                );

            let avg_spend_pm_indexed = avg_spend_pm
                .map_index(|(cc_num, Tup2(ts, avg_amt))| (Tup2(*cc_num, *ts), *avg_amt));

            // COUNT(*) OVER(
            //     PARTITION BY  CAST(cc_num AS NUMERIC)
            //     ORDER BY unix_time
            //     -- 1 day is 86400  seconds
            //     RANGE BETWEEN 86400 PRECEDING AND 1 PRECEDING) AS trans_freq_24,
            let trans_freq_24: Stream<_, TransactionFrequency> = amounts
                .partitioned_rolling_aggregate_linear(
                    |_amt| 1,
                    |cnt| cnt,
                    RelRange::new(RelOffset::Before(DAY_IN_SECONDS), RelOffset::Before(1)),
                );

            let trans_freq_24_indexed = trans_freq_24
                .map_index(|(cc_num, Tup2(ts, freq))| (Tup2(*cc_num, *ts), freq.unwrap_or(0)));

            avg_spend_pw_indexed
                .join_index(&avg_spend_pm_indexed, |&cc_num_ts, pw_avg, pm_avg| {
                    Some((cc_num_ts, Tup2(*pw_avg, *pm_avg)))
                })
                .join_index(
                    &trans_freq_24_indexed,
                    |&cc_num_ts, (pw_avg, pm_avg), freq| {
                        Some((cc_num_ts, Tup3(*pw_avg, *pm_avg, *freq)))
                    },
                )
                .join(
                    &enriched_transactions,
                    |Tup2(_cc_num, _ts), Tup3(pw_avg, pm_avg, freq), Tup2(tran, dem)| QueryResult {
                        avg_spend_pw: *pw_avg,
                        avg_spend_pm: *pm_avg,
                        trans_freq_24: *freq as u32,
                        category: tran.category,
                        amt: tran.amt,
                        state: dem.state,
                        job: dem.job,
                        unix_time: tran.unix_time,
                        city_pop: dem.city_pop,
                        merchant: tran.merchant,
                        is_fraud: tran.is_fraud,
                    },
                );
            Ok((hdemographics, htransactions))
        })
        .unwrap();

        Self {
            dbsp,
            demographics: hdemographics,
            transactions: htransactions,
        }
    }

    fn ingest_demographics(&mut self, path: &str) {
        let mut dem_reader = CsvReader::from_path(path).unwrap();

        println!("Ingesting demographics");
        let dem_iter = dem_reader.deserialize::<Demographics>();
        for record in dem_iter {
            let record = record.unwrap();
            // println!("Person: {record:?}");
            self.demographics.push(record, 1);
        }
        self.dbsp.step().unwrap();
    }

    fn process_transactions<R: Read>(mut self, mut reader: CsvReader<R>, batch_size: usize) {
        let start = Instant::now();
        println!("Ingesting transactions");

        let trans_iter = reader.deserialize::<Transaction>();
        let chunks = trans_iter.chunks(batch_size);

        let (tx, rx) = bounded(1);

        let thread_handle = spawn(move || loop {
            match rx.recv().unwrap() {
                true => {
                    let chunk_start = Instant::now();
                    self.dbsp.step().unwrap();
                    println!("compute: {}", chunk_start.elapsed());
                }
                false => {
                    self.dbsp.kill().unwrap();
                    break;
                }
            }
        });

        let mut total_size: usize = 0;
        for chunk in chunks.into_iter() {
            let chunk_start = Instant::now();
            let mut batch: Vec<_> = chunk
                .map(|record| {
                    let transaction = record.unwrap();
                    // println!("Transaction: {transaction:?}");
                    (transaction, 1)
                })
                .collect();
            self.transactions.append(&mut batch);
            println!("{total_size} parsing: {} ", chunk_start.elapsed());
            total_size += batch_size;

            tx.send(true).unwrap();
        }

        tx.send(false).unwrap();
        thread_handle.join().unwrap();

        println!("total time: {}", start.elapsed());
    }
}

fn main() -> Result<()> {
    let args = Args::parse();

    println!("Running 'fraud' benchmark with {} workers, reading demographics from '{}', reading transactions from '{}'",
             args.workers,
             args.demographics,
             if args.stdin {
                 "stdin"
             } else {
                 &args.transactions
             });

    let mut fraud = FraudBenchmark::new(args.workers);

    fraud.ingest_demographics(args.demographics.as_ref());

    if args.stdin {
        let transaction_reader = CsvReader::from_reader(stdin());
        fraud.process_transactions(transaction_reader, args.batch_size);
    } else {
        let transaction_reader = CsvReader::from_path(args.transactions).unwrap();
        fraud.process_transactions(transaction_reader, args.batch_size);
    }
    Ok(())
}
*/
