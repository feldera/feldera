//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
pub use q0::q0;
pub use q1::q1;
pub use q2::q2;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

mod q0;
mod q1;
mod q2;
