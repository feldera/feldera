//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
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

pub use q13::{q13, q13_side_input};
pub use q14::q14;
pub use q15::q15;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

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

mod q13;
mod q14;
mod q15;
