//! Model structs for the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink Java model classes](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model).

use std::sync::Arc;

pub type ImmString = Arc<String>;

/// The Nexmark Person model based on the [Nexmark Java Person class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Person {
    pub id: u64,
    pub name: ImmString,
    pub email_address: ImmString,
    pub credit_card: ImmString,
    pub city: ImmString,
    pub state: ImmString,
    pub date_time: u64,
    pub extra: ImmString,
}

/// The Nexmark Auction model based on the [Nexmark Java Auction class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Auction {
    pub id: u64,
    pub item_name: ImmString,
    pub description: ImmString,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: u64,
    pub expires: u64,
    pub seller: u64,
    pub category: usize,
    pub extra: ImmString,
}

/// The Nexmark Bid model based on the [Nexmark Java Bid class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Bid {
    /// Id of auction this bid is for.
    pub auction: u64,
    /// Id of the person bidding in auction.
    pub bidder: u64,
    /// Price of bid, in cents.
    pub price: usize,
    /// The channel that introduced this bidding.
    pub channel: ImmString,
    /// The url of this channel.
    pub url: ImmString,
    /// Instant at which this bid was made. NOTE: This may be earlier than teh
    /// system's event time.
    pub date_time: u64,
    /// Additional arbitrary payload for performance testing.
    pub extra: ImmString,
}

/// An event in the auction system, either a (new) `Person`, a (new) `Auction`,
/// or a `Bid`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}
