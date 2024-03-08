use anyhow::Result;
use chrono::{Datelike, NaiveDate};
use csv::Reader;
use dbsp::{
    operator::time_series::{RelOffset, RelRange},
    utils::{Tup2, Tup3},
    IndexedZSetHandle, OrdIndexedZSet, OutputHandle, RootCircuit, ZSetHandle,
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
    IndexedZSetHandle<String, u64>,
    OutputHandle<OrdIndexedZSet<Tup3<String, i32, u8>, Tup2<i64, u64>>>,
)> {
    let (vax_stream, vax_handle) = circuit.add_input_zset::<Record>();
    let (pop_stream, pop_handle) = circuit.add_input_indexed_zset::<String, u64>();
    let subset = vax_stream.filter(|r| {
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
    let running_monthly_totals = monthly_totals
        .map_index(|(Tup3(l, y, m), v)| (l.clone(), Tup2(*y as u32 * 12 + (*m as u32 - 1), *v)))
        .as_partitioned_zset()
        .partitioned_rolling_aggregate_linear(
            |vaxxed| *vaxxed,
            |total| total,
            RelRange::new(RelOffset::Before(u32::MAX), RelOffset::Before(0)),
        );
    let vax_rates = running_monthly_totals
        .map_index(|(l, Tup2(date, total))| {
            (
                l.clone(),
                Tup3((date / 12) as i32, (date % 12 + 1) as u8, total.unwrap()),
            )
        })
        .join_index(&pop_stream, |l, Tup3(y, m, total), pop| {
            Some((Tup3(l.clone(), *y, *m), Tup2(*total, *pop)))
        });
    Ok((vax_handle, pop_handle, vax_rates.output()))
}

fn main() -> Result<()> {
    let (circuit, (vax_handle, pop_handle, output_handle)) = RootCircuit::build(build_circuit)?;

    let path = format!(
        "{}/examples/tutorial/vaccinations.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let mut vax_records = Reader::from_path(path)?
        .deserialize()
        .map(|result| result.map(|record| Tup2(record, 1)))
        .collect::<Result<Vec<Tup2<Record, i64>>, _>>()?;
    vax_handle.append(&mut vax_records);

    let mut pop_records = vec![
        Tup2("England".into(), Tup2(56286961u64, 1i64)),
        Tup2("Northern Ireland".into(), Tup2(1893667, 1)),
        Tup2("Scotland".into(), Tup2(5463300, 1)),
        Tup2("Wales".into(), Tup2(3152879, 1)),
    ];
    pop_handle.append(&mut pop_records);

    circuit.step()?;

    output_handle
        .consolidate()
        .iter()
        .for_each(|(Tup3(l, y, m), Tup2(vaxxes, pop), w)| {
            let rate = vaxxes as f64 / pop as f64 * 100.0;
            println!("{l:16} {y}-{m:02}: {vaxxes:9} {pop:8} {rate:6.2}% {w:+}",)
        });
    Ok(())
}
