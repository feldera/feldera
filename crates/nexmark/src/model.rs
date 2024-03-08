//! Model structs for the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink Java model classes](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model).

use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;

/// The Nexmark Person model based on the [Nexmark Java Person class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(
    Clone,
    Default,
    Debug,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Person {
    pub id: u64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: u64,
    pub extra: String,
}

/// The Nexmark Auction model based on the [Nexmark Java Auction class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Auction {
    pub id: u64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: u64,
    pub reserve: u64,
    pub date_time: u64,
    pub expires: u64,
    pub seller: u64,
    pub category: u64,
    pub extra: String,
}

/// The Nexmark Bid model based on the [Nexmark Java Bid class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(
    Clone,
    Debug,
    Default,
    Eq,
    Hash,
    Ord,
    PartialEq,
    PartialOrd,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub struct Bid {
    /// Id of auction this bid is for.
    pub auction: u64,
    /// Id of the person bidding in auction.
    pub bidder: u64,
    /// Price of bid, in cents.
    pub price: u64,
    /// The channel that introduced this bidding.
    pub channel: String,
    /// The url of this channel.
    pub url: String,
    /// Instant at which this bid was made. NOTE: This may be earlier than teh
    /// system's event time.
    pub date_time: u64,
    /// Additional arbitrary payload for performance testing.
    pub extra: String,
}

/// An event in the auction system, either a (new) `Person`, a (new) `Auction`,
/// or a `Bid`.
#[derive(
    Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, SizeOf, Archive, Serialize, Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}

impl Default for Event {
    fn default() -> Self {
        Event::Person(Default::default())
    }
}
