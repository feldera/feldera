use std::{
    collections::BTreeMap,
    fmt::Write,
    fs::File,
    io::{BufRead, BufReader, ErrorKind},
    path::Path,
};

use itertools::Itertools;
use ordered_float::OrderedFloat;

use crate::cli::DebugActions;

fn parse_metrics(file_name: &Path) -> anyhow::Result<()> {
    #[derive(Clone, Debug, Default)]
    struct Metric {
        values: BTreeMap<Vec<(String, String)>, MetricValue>,
    }

    #[derive(Clone, Debug)]
    enum MetricValue {
        Number(f64),
        Histogram(Histogram),
    }

    impl MetricValue {
        fn as_histogram_mut(&mut self) -> Option<&mut Histogram> {
            match self {
                MetricValue::Histogram(histogram) => Some(histogram),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, Default)]
    struct Histogram {
        sum: f64,
        buckets: BTreeMap<OrderedFloat<f64>, f64>,
    }

    fn read_metrics<R>(reader: R) -> anyhow::Result<BTreeMap<String, Metric>>
    where
        R: BufRead,
    {
        fn get_metric<'a>(metrics: &'a mut BTreeMap<String, Metric>, name: &str) -> &'a mut Metric {
            metrics.entry(String::from(name)).or_default()
        }

        let mut metrics = BTreeMap::new();
        for line in reader.lines() {
            let line = line?;
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let (name, rest) = line.split_once('{').unwrap();
            let (pairs, rest) = rest.split_once('}').unwrap();
            let mut fields = pairs
                .split(',')
                .map(|pair| pair.split_once('=').unwrap())
                .filter(|(key, _value)| *key != "pipeline")
                .map(|(key, value)| (String::from(key), String::from(value.trim_matches('"'))))
                .collect::<Vec<_>>();
            let value: f64 = rest.trim().parse().unwrap();

            if let Some(name) = name.strip_suffix("_bucket") {
                let (key, le) = fields.pop().unwrap();
                assert_eq!(&key, "le");
                let le: f64 = le.parse().unwrap();
                let metric = get_metric(&mut metrics, name);
                metric
                    .values
                    .entry(fields)
                    .or_insert_with(|| MetricValue::Histogram(Histogram::default()))
                    .as_histogram_mut()
                    .unwrap()
                    .buckets
                    .insert(OrderedFloat(le), value);
            } else if let Some(name) = name.strip_suffix("_sum") {
                let metric = get_metric(&mut metrics, name);
                metric
                    .values
                    .entry(fields)
                    .or_insert_with(|| MetricValue::Histogram(Histogram::default()))
                    .as_histogram_mut()
                    .unwrap()
                    .sum = value;
            } else if name.starts_with("_count") {
                // Ignore.
            } else {
                let metric = get_metric(&mut metrics, name);
                metric.values.insert(fields, MetricValue::Number(value));
            }
        }
        Ok(metrics)
    }

    let metrics = read_metrics(BufReader::new(File::open(file_name)?))?;
    let mut by_fields: BTreeMap<&[(String, String)], String> = BTreeMap::new();
    for (name, metric) in &metrics {
        for (fields, value) in &metric.values {
            let s = by_fields.entry(fields.as_slice()).or_default();
            write!(s, "{name:<40}: ").unwrap();

            match value {
                MetricValue::Number(number) => writeln!(s, "{number:15.2}").unwrap(),
                MetricValue::Histogram(histogram) => {
                    let max = histogram.buckets.last_key_value().unwrap().1;
                    let mut sequence = histogram
                        .buckets
                        .values()
                        .copied()
                        .map(|value| ((value / max * 4.0 + 0.5) as usize).clamp(0, 4))
                        .collect::<Vec<_>>();

                    // Convert the histogram into a sequence of [Braille
                    // pattern] characters, with two buckets per Braille character.
                    //
                    // [Braille pattern]: https://en.wikipedia.org/wiki/Braille_Patterns
                    if sequence.len() % 2 == 1 {
                        sequence.push(0);
                    }
                    for (a, b) in sequence.into_iter().tuples() {
                        s.push(
                            ([
                                a >= 4, // ⠁ (dot 1)
                                a >= 3, // ⠂ (dot 2)
                                a >= 2, // ⠄ (dot 3)
                                b >= 4, // ⠈ (dot 4)
                                b >= 3, // ⠐ (dot 5)
                                b >= 2, // ⠠ (dot 6)
                                a >= 1, // ⡀ (dot 7)
                                b >= 1, // ⢀ (dot 8)
                            ]
                            .into_iter()
                            .enumerate()
                            .filter_map(|(index, on)| on.then_some(1u32 << index))
                            .sum::<u32>()
                                + 0x2800)
                                .try_into()
                                .unwrap(),
                        );
                    }
                    writeln!(s, " (count{:13.0}) (sum{:15.2})", histogram.sum, max).unwrap();
                }
            }
        }
    }
    for (fields, output) in by_fields {
        if !fields.is_empty() {
            println!("\n{fields:?}");
        }
        print!("{output}");
    }
    Ok(())
}

pub fn debug(action: DebugActions) {
    match action {
        DebugActions::MsgpCat { path } => {
            let mut file = match File::open(&path) {
                Ok(file) => file,
                Err(error) => {
                    eprintln!("{}: open failed ({error})", path.display());
                    std::process::exit(1);
                }
            };
            loop {
                let value = match rmpv::decode::value::read_value(&mut file) {
                    Ok(value) => value,
                    Err(rmpv::decode::Error::InvalidMarkerRead(error))
                        if error.kind() == ErrorKind::UnexpectedEof =>
                    {
                        break
                    }
                    Err(error) => {
                        eprintln!("{}: read failed ({error})", path.display());
                        std::process::exit(1);
                    }
                };
                println!("{value}");
            }
        }
        DebugActions::Metrics { path } => {
            if let Err(error) = parse_metrics(&path) {
                eprintln!("{}", error);
                std::process::exit(1);
            }
        }
    }
}
