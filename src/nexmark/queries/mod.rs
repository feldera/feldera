//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
pub use q0::q0;
pub use q1::q1;
pub use q2::q2;
pub use q3::q3;
pub use q4::q4;

pub use q6::q6;

pub use q9::q9;

pub use q13::{q13, q13_side_input};
pub use q14::q14;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

mod q0;
mod q1;
mod q2;
mod q3;
mod q4;

mod q6;

mod q9;

mod q13;
mod q14;
