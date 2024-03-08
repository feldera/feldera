//! Nexmark Queries in DBSP.

use super::model::Event;
use dbsp::{OrdZSet, RootCircuit, Stream};
use std::time::SystemTime;

type NexmarkStream = Stream<RootCircuit, OrdZSet<Event>>;

type OrdinalDate = (i32, u16);

// Based on the WATERMARK FOR definition in the original [ddl_gen.sql](https://github.com/nexmark/nexmark/blob/54974ef36a0d01ef8ebc0b4ba39cfc50136af0f6/nexmark-flink/src/main/resources/queries/ddl_gen.sql#L37)
const WATERMARK_INTERVAL_SECONDS: u64 = 4;

macro_rules! declare_queries {
    ($($query:ident),* $(,)?) => {
        $(
            mod $query;
            pub use $query::$query;
        )*

        paste::paste! {
            /// All available nexmark queries
            #[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
            pub enum Query {
                $([<$query:upper>],)*
            }
        }
    };
}

declare_queries! {
    q0,
    q1,
    q2,
    q3,
    q4,
    q5,
    q6,
    q7,
    q8,
    q9,
    q12,
    q13,
    q14,
    q15,
    q16,
    q17,
    q18,
    q19,
    q20,
    q21,
    q22,
}

pub use q13::q13_side_input;

fn process_time() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
