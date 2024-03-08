use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use csv::Reader;
use dbsp::{
    operator::time_series::{RelOffset, RelRange},
    utils::{Tup2, Tup3},
    OrdIndexedZSet, OutputHandle, RootCircuit, ZSetHandle,
};
use rkyv::{Archive, Serialize};
use size_of::SizeOf;

#[derive(
    Clone,
    Default,
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

fn build_circuit(
    circuit: &mut RootCircuit,
) -> Result<(
    ZSetHandle<Record>,
    OutputHandle<OrdIndexedZSet<Tup3<String, i32, u8>, Tup2<i64, i64>>>,
)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record>();
    let subset = input_stream.filter(|r| {
        r.location == "England"
            || r.location == "Northern Ireland"
            || r.location == "Scotland"
            || r.location == "Wales"
    });
    let monthly_totals = subset
        .map_index(|r| {
            (
                Tup3(r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|v| *v as i64);
    let moving_averages = monthly_totals
        .map_index(|(Tup3(l, y, m), v)| (l.clone(), Tup2(*y as u32 * 12 + (*m as u32 - 1), *v)))
        .as_partitioned_zset()
        .partitioned_rolling_average(RelRange::new(RelOffset::Before(2), RelOffset::Before(0)))
        .map_index(|(l, Tup2(date, avg))| {
            (
                Tup3(l.clone(), (date / 12) as i32, (date % 12 + 1) as u8),
                avg.unwrap(),
            )
        });
    let joined = monthly_totals.join_index(&moving_averages, |Tup3(l, y, m), cur, avg| {
        Some((Tup3(l.clone(), *y, *m), Tup2(*cur, *avg)))
    });
    Ok((input_handle, joined.output()))
}

fn main() -> Result<()> {
    let (circuit, (input_handle, output_handle)) = RootCircuit::build(build_circuit)?;

    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut input_records = Reader::from_path(path)?
        .deserialize()
        .map(|result| result.map(|record| Tup2(record, 1)))
        .collect::<Result<Vec<Tup2<Record, i64>>, _>>()?;
    input_handle.append(&mut input_records);

    circuit.step()?;

    output_handle
        .consolidate()
        .iter()
        .for_each(|(Tup3(l, y, m), Tup2(cur, avg), w)| {
            println!("{l:16} {y}-{m:02} {cur:10} {avg:10}: {w:+}")
        });
    Ok(())
}
