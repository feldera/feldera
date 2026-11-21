//! Paired runtime measurement using the frozen Unicode projection graph.

use super::circuit0;
use dbsp::{
    Runtime,
    circuit::{CircuitConfig, StepSize},
    utils::{Tup1, Tup2, Tup5},
};
use feldera_native_dbsp_probe::{StringProjectionSpec, StringRow, build_string_projection_streams};
use feldera_sqllib::SqlString;
use feldera_sqllib::read_output_spine;
use std::{str::FromStr, time::Instant};

fn setting<T: FromStr>(name: &str) -> T {
    std::env::var(name)
        .expect("benchmark setting")
        .parse()
        .ok()
        .expect("valid benchmark setting")
}

#[test]
#[ignore = "explicit benchmark run"]
fn paired_unicode_runtime_sample() {
    let engine: String = setting("P0_NATIVE_BENCH_ENGINE");
    let rows: usize = setting("P0_NATIVE_BENCH_ROWS");
    let steps: usize = setting("P0_NATIVE_BENCH_STEPS");
    let prefixes = ["🐍", "é", "朴", "a"];
    let mut input = Vec::with_capacity(rows);
    let mut expected = Vec::with_capacity(rows);
    for row in 0..rows {
        let prefix = prefixes[row % prefixes.len()];
        let suffix = row.to_string();
        let text = SqlString::from_ref(&format!("{prefix}{suffix}"));
        let prefix = SqlString::from_ref(prefix);
        input.push(Tup2(Tup1(Some(text.clone())), 1));
        let trimmed = if prefix.str() == "🐍" {
            SqlString::from_ref(&suffix)
        } else {
            text
        };
        expected.push(Tup2(
            Tup5(
                Some(prefix),
                SqlString::from_ref("🐍"),
                Some(2),
                2,
                Some(trimmed),
            ),
            1,
        ));
    }
    let expected = dbsp::OrdZSet::from_keys((), expected);

    let init = Instant::now();
    let sample = |mut step: Box<dyn FnMut(Vec<Tup2<StringRow, i64>>) -> _>| {
        let init_ns = init.elapsed().as_nanos();
        // Verify the full weighted output before the measured region.
        assert_eq!(step(input.clone()), expected);
        for _ in 0..5 {
            std::hint::black_box(step(input.clone()));
        }
        let start = Instant::now();
        for _ in 0..steps {
            std::hint::black_box(step(input.clone()));
        }
        println!(
            "engine={engine} rows={rows} steps={steps} init_ns={init_ns} elapsed_ns={}",
            start.elapsed().as_nanos()
        );
    };
    match engine.as_str() {
        "classic" => {
            let (mut runtime, (input, output, _)) =
                circuit0(CircuitConfig::with_workers(1)).unwrap();
            sample(Box::new(move |mut rows| {
                input.append(&mut rows);
                runtime.transaction().unwrap();
                read_output_spine(&output)
            }));
        }
        "native" => {
            let (mut runtime, (input, output, _errors)) = Runtime::init_circuit(
                CircuitConfig::with_workers(1).with_step_size(StepSize::FullSteps),
                |circuit| {
                    let streams = build_string_projection_streams(
                        circuit,
                        &StringProjectionSpec {
                            varchar_literal: "🐍".into(),
                            padded_width: 2,
                            padded_literal: "🐍 ".into(),
                            trim_pattern: "🐍".into(),
                        },
                    )?;
                    Ok((
                        streams.input,
                        streams.output.accumulate_output(),
                        streams.errors.accumulate_output(),
                    ))
                },
            )
            .unwrap();
            sample(Box::new(move |mut rows| {
                input.append(&mut rows);
                runtime.transaction().unwrap();
                read_output_spine(&output)
            }));
        }
        _ => panic!("expected classic or native benchmark mode"),
    }
}
