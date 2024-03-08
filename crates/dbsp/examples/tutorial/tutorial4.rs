use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use csv::Reader;
use dbsp::utils::{Tup2, Tup3};
use dbsp::{OrdIndexedZSet, OutputHandle, RootCircuit, ZSetHandle};
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
    OutputHandle<OrdIndexedZSet<Tup3<String, i32, u8>, i64>>,
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
    Ok((input_handle, monthly_totals.output()))
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
        .for_each(|(Tup3(l, y, m), sum, w)| println!("{l:16} {y}-{m:02} {sum:10}: {w:+}"));

    Ok(())
}
