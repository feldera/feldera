mod data;
mod personal_network;

use crate::data::{
    get_gkg_file, get_master_file, parse_personal_network_gkg, GDELT_URL, GKG_SUFFIX,
};
use dbsp::{
    trace::{BatchReader, Cursor},
    Circuit, Runtime,
};
use hashbrown::HashSet;
use std::{
    cmp::Reverse,
    io::{BufRead, BufReader, Write},
    panic::{self, AssertUnwindSafe},
    sync::atomic::{AtomicBool, Ordering},
};
use xxhash_rust::xxh3::Xxh3Builder;

static FINISHED: AtomicBool = AtomicBool::new(false);

fn main() {
    Runtime::run(4, || {
        let (root, mut handle) = Circuit::build(|circuit| {
            let (events, handle) = circuit.add_input_zset();

            // personal_network::mentioned_people(&events)
            //     .gather(0)
            //     .inspect(|network| {
            //         if !network.is_empty() {
            //             let mut mentions = Vec::with_capacity(10);
            //
            //             let mut cursor = network.cursor();
            //             while cursor.key_valid() {
            //                 if cursor.val_valid() {
            //                     mentions.push((cursor.key().clone(), cursor.weight()));
            //                 }
            //                 cursor.step_key();
            //             }
            //
            //             mentions.sort_by_key(|&(_, mentions)| Reverse(mentions));
            //
            //             println!("Mentions:");
            //             for (person, mentions) in mentions {
            //                 println!(
            //                     "- {person} (mentioned {mentions} time{})",
            //                     if mentions == 1 { "" } else { "s" },
            //                 );
            //             }
            //         }
            //     });

            let person = arcstr::literal!("barack obama");
            let mut network_buf = Vec::with_capacity(4096);
            let mut iteration = 0;

            personal_network::personal_network(person, &events)
                .gather(0)
                .inspect(move |network| {
                    if iteration > 100 && !network.is_empty() {
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
                            network_buf.sort_unstable_by_key(|&(.., count)| Reverse(count));

                            let mut stdout = std::io::stdout().lock();

                            writeln!(stdout, "Network:").unwrap();
                            for (source, target, count) in network_buf.drain(..) {
                                writeln!(stdout, "- {source}, {target}, {count}").unwrap();
                            }
                            writeln!(stdout).unwrap();

                            stdout.flush().unwrap();
                        }
                    }

                    iteration += 1;
                });

            handle
        })
        .unwrap();

        if Runtime::worker_index() == 0 {
            let result = panic::catch_unwind(AssertUnwindSafe(|| {
                let file_urls = BufReader::new(get_master_file())
                    .lines()
                    .filter_map(|line| {
                        let line = line.unwrap();
                        line.ends_with(GKG_SUFFIX)
                            .then(|| line.split(' ').last().unwrap().to_owned())
                            .filter(|url| {
                                let date = url
                                    .strip_prefix(GDELT_URL)
                                    .unwrap()
                                    .strip_suffix(GKG_SUFFIX)
                                    .unwrap()
                                    .parse::<u64>()
                                    .unwrap();
                                (20150302000000..20150304000000).contains(&date)
                            })
                    });

                let mut interner = HashSet::with_capacity_and_hasher(4096, Xxh3Builder::new());
                for url in file_urls {
                    if let Some(file) = get_gkg_file(&url) {
                        parse_personal_network_gkg(&mut handle, &mut interner, file);
                        root.step().unwrap();
                    }
                }
            }));

            FINISHED.store(true, Ordering::Release);
            if let Err(panic) = result {
                panic::resume_unwind(panic);
            }
        } else {
            while !FINISHED.load(Ordering::Acquire) {
                root.step().unwrap();
            }
        }
    })
    .join()
    .unwrap();
}
