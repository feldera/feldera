#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_assignments)]
#![allow(non_upper_case_globals)]

use dbsp_adapters::{DeCollectionStream, DbspCircuitHandle, CircuitCatalog, RecordFormat, SerBatch};
use hdrhistogram::Histogram;
use feldera_types::format::json::JsonFlavor;

use dbsp::{
    algebra::F64,
    circuit::{
        CircuitConfig,
        metrics::TOTAL_LATE_RECORDS,
    },
    utils::*,
    zset,
};

use std::fmt::Debug;
use rand::{Rng, rngs::ThreadRng};

use std::{
    collections::HashMap,
    time::SystemTime,
};

use metrics::{Key, SharedString, Unit};
use metrics_util::{
    debugging::{DebugValue, DebuggingRecorder},
    CompositeKey, MetricKind,
};

use experiments::circuit;
use dbsp::circuit::Layout;
use uuid::Uuid;

type MetricsSnapshot = HashMap<CompositeKey, (Option<Unit>, Option<SharedString>, DebugValue)>;

fn parse_counter(metrics: &MetricsSnapshot, name: &'static str) -> u64 {
    if let Some((_, _, DebugValue::Counter(value))) = metrics.get(&CompositeKey::new(
        MetricKind::Counter,
        Key::from_static_name(name),
    )) {
        *value
    } else {
        0
    }
}

pub fn gen_input(rng: &mut ThreadRng, max: i32) -> i32 {
    let random_number: i32 = rng.gen_range(0..max);
    random_number
}

pub fn print_row<T>(row: Vec<T>)
where
    T: Debug,
{
    let mut first = true;
    for v in row {
        if !first {
            print!(",");
        }
        first = false;
        print!("{:?}", v);
    }
    println!();
}

pub fn print_array<T, S>(header: Vec<S>, data: Vec<Vec<T>>)
where
    T: Debug + Clone,
    S: Debug,
{
    let rows = data.len();
    if rows == 0 {
        return;
    }
    let columns = data[0].len();
    assert!(rows == header.len());

    print_row(header);
    for j in 0..columns {
        let mut row = vec!();
        for i in 0..rows {
            row.push(data[i][j].clone());
        }
        print_row(row);
    }
}

pub fn main() {
    const max: i32 = 1000000;
    const load: i32 = 20_000_000;
    const steps: i32 = 1000_0000; // 10_000_000;
    const batch_size: i32 = 1000;
    // let mut measurements: Vec<Vec<u128>> = vec![];
    let para = vec!(8); // vec!(1,2,4,8,16);
    let mut rng = rand::thread_rng();

    let begin = SystemTime::now();
    let recorder = DebuggingRecorder::new();
    //let snapshotter = recorder.snapshotter();
    //recorder.install().unwrap();

    fn make_input(data: i32) -> String {
        format!("[ {} ]", data)
    }

    fn print_histogram<T>(hist: &Histogram<T>)
    where T: hdrhistogram::Counter
    {
        println!("{}, {}, {}, {}, {}",
                 hist.value_at_quantile(0.5),
                 hist.value_at_quantile(0.9),
                 hist.value_at_quantile(0.99),
                 hist.value_at_quantile(0.999),
                 hist.value_at_quantile(1.0)
                );
    }

    fn print_all_histogram<T>(hist: &Histogram<T>)
    where T: hdrhistogram::Counter + std::fmt::Display
    {
        for v in hist.iter_recorded() {
            println!("{},{},{}",
                     v.percentile(),
                     v.value_iterated_to(),
                     v.count_at_value());
        }
    }

    for workers in &para {
        println!("starting {:?} worker(s)", workers);
        //let mut cur_mes = vec![];
        let mut hist = Histogram::<u64>::new(2).unwrap();
        const RECORD_FORMAT: RecordFormat = RecordFormat::Json(JsonFlavor::Default);
        let (mut circuit, streams) = circuit(
            CircuitConfig {
                layout: Layout::new_solo(*workers),
                storage: None,
                min_storage_bytes: usize::MAX,
                init_checkpoint: Uuid::nil(),
            }).expect("could not build circuit");

        let input_map_handle = streams.input_collection_handle(&("T".into())).unwrap();
        let mut input_stream_handle = input_map_handle.handle
            .configure_deserializer(RECORD_FORMAT.clone()).unwrap();

        let output_map_handle = &streams.output_handles(&("V".into())).unwrap().delta_handle;

        // uncomment if you want a CPU profile
        // let _ = circuit.enable_cpu_profiler();

        fn flush_step(input_stream_handle: &mut Box<dyn DeCollectionStream>, circuit: &mut dyn DbspCircuitHandle) {
            input_stream_handle.flush();
            circuit.step().expect("could not run circuit");
        }

        // Load the database
        println!("Preloading {:?} rows", load);
        for i in 0..load {
            let data = gen_input(&mut rng, max);
            input_stream_handle.insert(make_input(data).as_bytes()).unwrap();
            if i % 10000 == 0 && i > 0 {
                flush_step(&mut input_stream_handle, &mut circuit);
                //let out = output_map_handle.consolidate();
                //println!("out0={:?}", out);
            }
        }
        flush_step(&mut input_stream_handle, &mut circuit);

        println!("workers={} batch_size={}", workers, batch_size);
        let mut start = SystemTime::now();
        // Initial data value for timestamp
        for i in 0..steps {
            let data = gen_input(&mut rng, max);
            input_stream_handle.insert(make_input(data).as_bytes()).unwrap();
            if i % batch_size == 0 && i > 0 {
                flush_step(&mut input_stream_handle, &mut circuit);
                let end = SystemTime::now();
                let duration = end.duration_since(start).expect("could not get time");
                start = end;
                hist.record(duration.as_nanos() as u64 / batch_size as u64).unwrap();
                // println!("{:?}", duration.as_millis());
            }
        }
        // Insert one record to get the output, but don't measure it
        input_stream_handle.insert(make_input(-1).as_bytes()).unwrap();
        flush_step(&mut input_stream_handle, &mut circuit);

        print_histogram(&hist);

        //let metrics = snapshotter.snapshot();
        //let decoded_metrics: MetricsSnapshot = metrics.into_hashmap();
        // let late = parse_counter(&decoded_metrics, TOTAL_LATE_RECORDS);
        // println!("{:?}", decoded_metrics);
        //let profile = circuit.retrieve_profile().expect("could not get profile");
        //println!("{:?}", profile);

        let out = output_map_handle.consolidate();
        println!("out={:?}", out);
    }

    let end = SystemTime::now();
    let duration = end.duration_since(begin).expect("could not get time");
    println!("{:?}", duration);

    //print_array(para, measurements);

    /*
    let mut data = String::new();
    data.push_str(&format!("{},{},{}\n",
    duration.as_millis(),
    profile.total_used_bytes().unwrap().bytes,
    late));
    println!("{:?},{:?},{:?}", duration, profile.total_used_bytes().unwrap(), late);
     */
}
