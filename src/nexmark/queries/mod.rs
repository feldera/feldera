//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
pub use q0::q0;
pub use q1::q1;
pub use q2::q2;
pub use q3::q3;
pub use q4::q4;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

mod q0;
mod q1;
mod q2;
mod q3;
mod q4;
