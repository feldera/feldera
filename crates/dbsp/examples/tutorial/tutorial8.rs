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
#[archive_attr(derive(Ord, Eq, PartialEq, PartialOrd))]
struct Record {
    location: String,
    date: NaiveDate,
    daily_vaccinations: Option<u64>,
}

#[allow(clippy::type_complexity)]
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
                Tup3::new(r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|v| *v as i64);
    let running_monthly_totals = monthly_totals
        .map_index(|(t, v)| {
            (
                *t.get_1() as u32 * 12 + (*t.get_2() as u32 - 1),
                Tup2::new(t.get_0().clone(), *v),
            )
        })
        .partitioned_rolling_aggregate_linear(
            |t| (t.fst().clone(), *t.snd()),
            |vaxxed| *vaxxed,
            |total| total,
            RelRange::new(RelOffset::Before(u32::MAX), RelOffset::Before(0)),
        );
    let vax_rates = running_monthly_totals
        .map_index(|(l, t)| {
            (
                l.clone(),
                Tup3::new(
                    (*t.fst() / 12) as i32,
                    (*t.fst() % 12 + 1) as u8,
                    t.snd().unwrap(),
                ),
            )
        })
        .join_index(&pop_stream, |l, t, pop| {
            Some((
                Tup3::new(l.clone(), *t.get_0(), *t.get_1()),
                Tup2::new(*t.get_2(), *pop),
            ))
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
        .map(|result| result.map(|record| Tup2::new(record, 1)))
        .collect::<Result<Vec<Tup2<Record, i64>>, _>>()?;
    vax_handle.append(&mut vax_records);

    let mut pop_records = vec![
        Tup2::new("England".into(), Tup2::new(56286961u64, 1i64)),
        Tup2::new("Northern Ireland".into(), Tup2::new(1893667, 1)),
        Tup2::new("Scotland".into(), Tup2::new(5463300, 1)),
        Tup2::new("Wales".into(), Tup2::new(3152879, 1)),
    ];
    pop_handle.append(&mut pop_records);

    circuit.step()?;

    output_handle.consolidate().iter().for_each(|(t3, t2, w)| {
        let rate = *t2.fst() as f64 / *t2.snd() as f64 * 100.0;
        println!(
            "{:16} {}-{:02}: {:9} {:8} {rate:6.2}% {w:+}",
            t3.get_0(),
            t3.get_1(),
            t3.get_2(),
            t2.fst(),
            t2.snd()
        )
    });
    Ok(())
}
