use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use csv::Reader;
use dbsp::utils::{Tup2, Tup3};
use dbsp::{
    operator::FilterMap, CollectionHandle, IndexedZSet, OrdIndexedZSet, OutputHandle, RootCircuit,
};
use rkyv::{Archive, Serialize};
use size_of::SizeOf;

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct Record {
    location: String,
    date: NaiveDate,
    daily_vaccinations: Option<u64>,
}

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    SizeOf,
    Archive,
    Serialize,
    rkyv::Deserialize,
    serde::Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct VaxMonthly {
    count: u64,
    year: i32,
    month: u8,
}

fn build_circuit(
    circuit: &mut RootCircuit,
) -> Result<(
    CollectionHandle<Record, i64>,
    OutputHandle<OrdIndexedZSet<String, VaxMonthly, i64>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record, i64>();
    let subset = input_stream.filter(|r| {
        r.location == "England"
            || r.location == "Northern Ireland"
            || r.location == "Scotland"
            || r.location == "Wales"
    });
    let monthly_totals = subset
        .index_with(|r| {
            Tup2(
                Tup3(r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|v| *v as i64);
    let most_vax = monthly_totals
        .map_index(|(Tup3(l, y, m), sum)| {
            (
                l.clone(),
                VaxMonthly {
                    count: *sum as u64,
                    year: *y,
                    month: *m,
                },
            )
        })
        .topk_desc(3);
    Ok((input_handle, most_vax.output()))
}

fn main() -> Result<()> {
    let (circuit, (input_handle, output_handle)) = RootCircuit::build(build_circuit)?;

    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut reader = Reader::from_path(path)?;
    let mut input_records = reader.deserialize();
    loop {
        let mut batch = Vec::new();
        while batch.len() < 500 {
            let Some(record) = input_records.next() else {
                break;
            };
            batch.push((record?, 1));
        }
        if batch.is_empty() {
            break;
        }
        println!("Input {} records:", batch.len());
        input_handle.append(&mut batch);

        circuit.step()?;

        output_handle
            .consolidate()
            .iter()
            .for_each(|(l, VaxMonthly { count, year, month }, w)| {
                println!("   {l:16} {year}-{month:02} {count:10}: {w:+}")
            });
        println!();
    }
    Ok(())
}
