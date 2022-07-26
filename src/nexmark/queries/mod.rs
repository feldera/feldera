//! Nexmark Queries in DBSP.
use super::model::Event;
use crate::{Circuit, OrdZSet, Stream};
pub use q0::q0;
pub use q1::q1;

type NexmarkStream = Stream<Circuit<()>, OrdZSet<Event, isize>>;

mod q0;
mod q1;
