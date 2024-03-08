use anyhow::Result;
use chrono::NaiveDate;
use csv::Reader;
use dbsp::utils::Tup2;
use dbsp::{OrdZSet, OutputHandle, RootCircuit, ZSet, ZSetHandle};
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
) -> Result<(ZSetHandle<Record>, OutputHandle<OrdZSet<Record>>)> {
    let (input_stream, input_handle) = circuit.add_input_zset::<Record>();
    input_stream.inspect(|records| {
        println!("{}", records.weighted_count());
    });
    let filtered = input_stream.filter(|r| {
        r.location == "England"
            || r.location == "Northern Ireland"
            || r.location == "Scotland"
            || r.location == "Wales"
    });
    Ok((input_handle, filtered.output()))
}

fn main() -> Result<()> {
    // Build circuit.
    let (circuit, (input_handle, output_handle)) = RootCircuit::build(build_circuit)?;

    // Feed data into circuit.
    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut input_records = Reader::from_path(path)?
        .deserialize()
        .map(|result| result.map(|record| Tup2(record, 1)))
        .collect::<Result<Vec<Tup2<Record, i64>>, _>>()?;
    input_handle.append(&mut input_records);

    // Execute circuit.
    circuit.step()?;

    // Read output from circuit.
    println!("{}", output_handle.consolidate().weighted_count());

    Ok(())
}
