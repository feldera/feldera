mod data;
mod personal_network;

use crate::data::{
    build_gdelt_normalizations, get_gkg_file, get_master_file, parse_personal_network_gkg,
    GDELT_URL, GKG_SUFFIX,
};
use clap::Parser;
use dbsp::dynamic::DowncastTrait;
use dbsp::{
    trace::{BatchReader, Cursor},
    utils::Tup2,
    Runtime,
};
use std::{
    cmp::Reverse,
    io::{BufRead, BufReader, Write},
    num::NonZeroUsize,
    thread,
    time::Instant,
};

#[derive(Debug, Clone, Parser)]
struct Args {
    /// The number of threads to use for the dataflow, defaults to the
    /// number of cores the current machine has
    #[clap(long)]
    threads: Option<NonZeroUsize>,

    /// The person who's network we should query
    #[clap(long, default_value = "joe biden")]
    person: String,

    /// The start of the date range to search within
    #[clap(long)]
    date_start: Option<u64>,

    /// The end of the date range to search within
    #[clap(long)]
    date_end: Option<u64>,

    /// The number of 15-minute batches to ingest
    #[clap(long, default_value = "20")]
    batches: NonZeroUsize,

    /// The number of 15-minute batches to aggregate together for each dataflow
    /// epoch
    #[clap(long, default_value = "1")]
    aggregate_batches: NonZeroUsize,

    /// If set, limits the number of returned results to the given amount
    #[clap(long)]
    topk: Option<NonZeroUsize>,

    #[clap(long)]
    update_master_list: bool,

    // When running with `cargo bench` the binary gets the `--bench` flag, so we
    // have to parse and ignore it so clap doesn't get angry
    #[doc(hidden)]
    #[clap(long = "bench", hide = true)]
    __bench: bool,
}

fn main() {
    let args = Args::parse();
    let threads = args
        .threads
        .or_else(|| thread::available_parallelism().ok())
        .map(NonZeroUsize::get)
        .unwrap_or(1);
    let batches = args.batches.get();
    let person = args.person.trim().to_lowercase();

    if let Some((start, end)) = args.date_start.zip(args.date_end) {
        if start > end {
            eprintln!("error: `--date-start` must be less than than or equal to `--date-end` ({start} > {end})");
            return;
        }
    }

    let (mut handle, mut entries) = Runtime::init_circuit(threads, move |circuit| {
        let (events, handle) = circuit.add_input_zset();

        let mut network_buf = Vec::with_capacity(4096);
        personal_network::personal_network(person, args.date_start, args.date_end, &events)
            .gather(0)
            .inspect(move |network| {
                if !network.is_empty() {
                    let mut cursor = network.cursor();
                    while cursor.key_valid() {
                        if cursor.val_valid() {
                            let count = **cursor.weight();
                            let Tup2(source, target) =
                                cursor.key().downcast::<Tup2<String, String>>().clone();
                            network_buf.push((source, target, count));
                        }
                        cursor.step_key();
                    }

                    if !network_buf.is_empty() {
                        let total_connections = network_buf.len();
                        network_buf.sort_unstable_by(
                            |(source1, target1, mentions1), (source2, target2, mentions2)| {
                                Reverse(mentions1)
                                    .cmp(&Reverse(mentions2))
                                    .then_with(|| source1.cmp(source2))
                                    .then_with(|| target1.cmp(target2))
                            },
                        );

                        if let Some(topk) = args.topk.map(NonZeroUsize::get) {
                            network_buf.truncate(topk);
                        }

                        let mut stdout = std::io::stdout().lock();

                        writeln!(stdout, "Network ({total_connections} total connections):")
                            .unwrap();
                        for (source, target, count) in network_buf.drain(..) {
                            writeln!(stdout, "- {source}, {target}, {count}").unwrap();
                        }
                        writeln!(stdout).unwrap();

                        stdout.flush().unwrap();
                    }
                }
            });

        Ok(handle)
    })
    .unwrap();

    let mut file_urls = BufReader::new(get_master_file(args.update_master_list))
        .lines()
        .flatten()
        .filter_map(|line| {
            let line = line.trim();
            // We now have a url in this form: `http://data.gdeltproject.org/gdeltv2/20150218230000.gkg.csv.zip`
            let url = line
                .ends_with(GKG_SUFFIX)
                .then(|| line.split(' ').last().unwrap());

            fn filter_date(url: Option<&str>, cmp: impl Fn(u64) -> bool) -> Option<&str> {
                url.filter(|url| {
                    url.strip_prefix(GDELT_URL)
                        .and_then(|url| url.strip_suffix(GKG_SUFFIX))
                        .and_then(|date| date.parse::<u64>().ok())
                        .map_or(false, cmp)
                })
            }

            match (args.date_start, args.date_end) {
                (None, None) => url,
                (Some(start), None) => filter_date(url, |date| date <= start),
                (None, Some(end)) => filter_date(url, |date| date >= end),
                (Some(start), Some(end)) => filter_date(url, |date| date <= start && date >= end),
            }
            .map(|url| url.to_owned())
        });

    let (normalizations, invalid) = build_gdelt_normalizations();

    let (mut are_remaining_urls, mut current_batch) = (true, 0);
    while current_batch < args.batches.get() && are_remaining_urls {
        let (mut aggregate, mut records) = (0, 0);
        loop {
            if aggregate >= args.aggregate_batches.get() {
                break;
            }

            if let Some(url) = file_urls.next() {
                if let Some(file) = get_gkg_file(&url) {
                    records +=
                        parse_personal_network_gkg(&mut entries, &normalizations, &invalid, file);

                    aggregate += 1;
                    current_batch += 1;
                }
            } else {
                are_remaining_urls = false;
                break;
            }
        }

        let start = Instant::now();
        handle.step().unwrap();

        let elapsed = start.elapsed();
        if args.aggregate_batches.get() == 1 {
            println!(
                "ingested batch {current_batch}/{batches} ({records} record{}) in {elapsed:#?}",
                if records == 1 { "" } else { "s" },
            );
        } else {
            println!(
                "ingested batch{} {}..{current_batch}/{batches} ({records} record{}) in {elapsed:#?}",
                if aggregate == 1 { "" } else { "es" },
                current_batch - aggregate,
                if records == 1 { "" } else { "s" },
            );
        }
    }
}
