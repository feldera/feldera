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
                Tup3::new(r.location.clone(), r.date.year(), r.date.month() as u8),
                r.daily_vaccinations.unwrap_or(0),
            )
        })
        .aggregate_linear(|v| *v as i64);
    let moving_averages = monthly_totals
        .map_index(|(t, v)| {
            (
                *t.get_1() as u32 * 12 + (*t.get_2() as u32 - 1),
                Tup2::new(t.get_0().clone(), *v),
            )
        })
        .partitioned_rolling_average(
            |t| (t.fst().clone(), *t.snd()),
            RelRange::new(RelOffset::Before(2), RelOffset::Before(0)),
        )
        .map_index(|(l, t)| {
            (
                Tup3::new(l.clone(), (*t.fst() / 12) as i32, (*t.fst() % 12 + 1) as u8),
                t.snd().unwrap(),
            )
        });

    let joined = monthly_totals.join_index(&moving_averages, |t, cur, avg| {
        Some((
            Tup3::new(t.get_0().clone(), *t.get_1(), *t.get_2()),
            Tup2::new(*cur, *avg),
        ))
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
        .map(|result| result.map(|record| Tup2::new(record, 1)))
        .collect::<Result<Vec<Tup2<Record, i64>>, _>>()?;
    input_handle.append(&mut input_records);

    circuit.step()?;

    output_handle.consolidate().iter().for_each(|(t3, t2, w)| {
        println!(
            "{:16} {}-{:02} {:10} {:10}: {w:+}",
            t3.get_0(),
            t3.get_1(),
            t3.get_2(),
            t2.fst(),
            t2.snd()
        );
    });
    Ok(())
}
