//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
use std::time::SystemTime;

pub use q0::q0;
pub use q1::q1;
pub use q2::q2;
pub use q3::q3;
pub use q4::q4;
pub use q5::q5;
pub use q6::q6;
pub use q7::q7;
pub use q8::q8;
pub use q9::q9;

pub use q12::q12;
pub use q13::{q13, q13_side_input};
pub use q14::q14;
pub use q15::q15;
pub use q16::q16;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

// Based on the WATERMARK FOR definition in the original [ddl_gen.sql](https://github.com/nexmark/nexmark/blob/54974ef36a0d01ef8ebc0b4ba39cfc50136af0f6/nexmark-flink/src/main/resources/queries/ddl_gen.sql#L37)
const WATERMARK_INTERVAL_SECONDS: u64 = 4;

mod q0;
mod q1;
mod q2;
mod q3;
mod q4;
mod q5;
mod q6;
mod q7;
mod q8;
mod q9;

mod q12;
mod q13;
mod q14;
mod q15;
mod q16;

fn process_time() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}
