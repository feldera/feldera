#![feature(closure_lifetime_binder)]

mod data;
mod personal_network;

use crate::data::{get_gkg_file, get_master_file, parse_personal_network_gkg, GKG_SUFFIX};
use arcstr::{literal, ArcStr};
use clap::Parser;
use dbsp::{
    trace::{BatchReader, Cursor},
    Circuit, Runtime,
};
use hashbrown::{HashMap, HashSet};
use std::{
    cell::Cell,
    cmp::Reverse,
    io::{BufRead, BufReader, Write},
    num::NonZeroUsize,
    panic::{self, AssertUnwindSafe},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Instant,
};
use xxhash_rust::xxh3::Xxh3Builder;

static FINISHED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Clone, Parser)]
struct Args {
    /// The number of threads to use for the dataflow, defaults to the
    /// number of cores the current machine has
    #[clap(long)]
    threads: Option<NonZeroUsize>,

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

thread_local! {
    static CURRENT_BATCH: Cell<usize> = const { Cell::new(0) };
}

fn main() {
    let args = Args::parse();
    let threads = args
        .threads
        .or_else(|| thread::available_parallelism().ok())
        .map(NonZeroUsize::get)
        .unwrap_or(1);
    let batches = args.batches.get();

    Runtime::run(threads, move || {
        let (root, mut handle) = Circuit::build(|circuit| {
            let (events, handle) = circuit.add_input_zset();

            let person = literal!("joe biden");
            // let mut network_buf = Vec::with_capacity(4096);

            personal_network::personal_network(person, None, &events);
            /*
                .gather(0)
                .inspect(move |network| {
                    if !network.is_empty() && CURRENT_BATCH.with(|batch| batch.get() + 1 == batches)
                    {
                        let mut cursor = network.cursor();
                        while cursor.key_valid() {
                            if cursor.val_valid() {
                                let count = cursor.weight();
                                let (source, target) = cursor.key().clone();
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
            */

            handle
        })
        .unwrap();

        if Runtime::worker_index() == 0 {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let mut file_urls = BufReader::new(get_master_file(args.update_master_list))
                    .lines()
                    .filter_map(|line| {
                        let line = line.unwrap();
                        let line = line.trim();
                        line.ends_with(GKG_SUFFIX)
                            .then(|| line.split(' ').last().unwrap().to_owned())
                    });

                let mut interner = HashSet::with_capacity_and_hasher(4096, Xxh3Builder::new());
                let normalizations = {
                    static NORMALS: &[(&str, &[ArcStr])] = &[
                        ("a los angeles", &[literal!("los angeles")]),
                        ("a harry truman", &[literal!("harry truman")]),
                        ("a ronald reagan", &[literal!("ronald reagan")]),
                        ("a lyndon johnson", &[literal!("lyndon johnson")]),
                        ("a sanatan dharam", &[literal!("sanatan dharam")]),
                        ("b richard nixon", &[literal!("richard nixon")]),
                        ("b dwight eisenhower", &[literal!("dwight eisenhower")]),
                        ("c george w bush", &[literal!("george w bush")]),
                        ("c gerald ford", &[literal!("gerald ford")]),
                        ("c john f kennedy", &[literal!("john f kennedy")]),
                        // I can't even begin to explain this one
                        ("obama jeb bush", &[literal!("jeb bush")]),
                        (
                            "brandon morse thebrandonmorse",
                            &[literal!("brandon morse")],
                        ),
                        ("lady michelle obama", &[literal!("michelle obama")]),
                        ("jo biden", &[literal!("joe biden")]),
                        ("joseph robinette biden jr", &[literal!("joe biden")]),
                        ("brad thor bradthor", &[literal!("brad thor")]),
                        ("hilary clinton", &[literal!("hillary clinton")]),
                        ("hillary rodham clinton", &[literal!("hillary clinton")]),
                        (
                            "sherlockian a sherlock holmes",
                            &[literal!("sherlock holmes")],
                        ),
                        ("america larry pratt", &[literal!("larry pratt")]),
                        ("cullen hawkins sircullen", &[literal!("cullen hawkins")]),
                        (
                            "leslie knope joe biden",
                            &[literal!("leslie knope"), literal!("joe biden")],
                        ),
                        ("jacquelyn martin europe", &[literal!("jacquelyn martin")]),
                    ];
                    // Add the static normals to the interner, might as well reuse them
                    interner.extend(NORMALS.iter().flat_map(|&(_, person)| person).cloned());

                    let mut map =
                        HashMap::with_capacity_and_hasher(NORMALS.len(), Xxh3Builder::new());
                    map.extend(NORMALS);
                    map
                };

                // Invalid "people" that aren't really people
                let invalid = {
                    // On one hand, "krispy kreme klub" is most definitely not a person. On the
                    // other hand, it's kinda funny to see it pop up in the graph
                    let invalid = ["whitehouse cvesummit", "islam obama"];

                    let mut set =
                        HashSet::with_capacity_and_hasher(invalid.len(), Xxh3Builder::new());
                    set.extend(invalid.into_iter());
                    set
                };

                let mut are_remaining_urls = true;
                while CURRENT_BATCH.with(|batch| batch.get() < args.batches.get()) && are_remaining_urls {
                    let (mut aggregate, mut records) = (0, 0);
                    loop {
                        if aggregate >= args.aggregate_batches.get() {
                            break;
                        }

                        if let Some(url) = file_urls.next() {
                            if let Some(file) = get_gkg_file(&url) {
                                records += parse_personal_network_gkg(
                                    &mut handle,
                                    &mut interner,
                                    &normalizations,
                                    &invalid,
                                    file,
                                );

                                aggregate += 1;
                                CURRENT_BATCH.with(|batch| batch.set(batch.get() + 1));
                            }
                        } else {
                            are_remaining_urls = false;
                            break;
                        }
                    }

                    let start = Instant::now();
                    root.step().unwrap();

                    let elapsed = start.elapsed();
                    let current = CURRENT_BATCH.with(|batch| batch.get());
                    if args.aggregate_batches.get() == 1 {
                        println!(
                            "ingested batch {}/{batches} ({records} record{}) in {elapsed:#?}",
                            current,
                            if records == 1 { "" } else { "s" },
                        );
                    } else {
                        println!(
                            "ingested batch{} {}..{}/{batches} ({records} record{}) in {elapsed:#?}",
                            if aggregate == 1 { "" } else { "es" },
                            current - aggregate,
                            current,
                            if records == 1 { "" } else { "s" },
                        );
                    }
                }
            }));

            FINISHED.store(true, Ordering::Release);
            if let Err(panic) = result {
                panic::resume_unwind(panic);
            }
        } else {
            let mut current_batch = 0;
            while current_batch < batches && !FINISHED.load(Ordering::Acquire) {
                root.step().unwrap();
                current_batch += 1;
            }
        }
    })
    .join()
    .unwrap();
}
