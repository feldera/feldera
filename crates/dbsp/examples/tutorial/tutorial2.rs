use anyhow::Result;
use chrono::NaiveDate;
use csv::Reader;
use dbsp::{CollectionHandle, RootCircuit, ZSet};
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
fn build_circuit(circuit: &mut RootCircuit) -> Result<CollectionHandle<Record, i64>> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record, i64>();
    input_stream.inspect(|records| {
        println!("{}", records.weighted_count());
    });
    // ...populate `circuit` with more operators...
    Ok(input_handle)
}

fn main() -> Result<()> {
    // Build circuit.
    let (circuit, input_handle) = RootCircuit::build(build_circuit)?;

    // Feed data into circuit.
    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut input_records = Reader::from_path(path)?
        .deserialize()
        .map(|result| result.map(|record| (record, 1)))
        .collect::<Result<Vec<(Record, i64)>, _>>()?;
    input_handle.append(&mut input_records);

    // Execute circuit.
    circuit.step()?;

    // ...read output from circuit...
    Ok(())
}
