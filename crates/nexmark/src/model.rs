//! Model structs for the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink Java model classes](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model).

use arcstr::ArcStr;
use bincode::{Decode, Encode};
use size_of::SizeOf;

/// The Nexmark Person model based on the [Nexmark Java Person class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, SizeOf, Encode, Decode)]
pub struct Person {
    pub id: u64,
    #[bincode(with_serde)]
    pub name: ArcStr,
    #[bincode(with_serde)]
    pub email_address: ArcStr,
    #[bincode(with_serde)]
    pub credit_card: ArcStr,
    #[bincode(with_serde)]
    pub city: ArcStr,
    #[bincode(with_serde)]
    pub state: ArcStr,
    pub date_time: u64,
    #[bincode(with_serde)]
    pub extra: ArcStr,
}

/// The Nexmark Auction model based on the [Nexmark Java Auction class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, SizeOf, Encode, Decode)]
pub struct Auction {
    pub id: u64,
    #[bincode(with_serde)]
    pub item_name: ArcStr,
    #[bincode(with_serde)]
    pub description: ArcStr,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: u64,
    pub expires: u64,
    pub seller: u64,
    pub category: usize,
    #[bincode(with_serde)]
    pub extra: ArcStr,
}

/// The Nexmark Bid model based on the [Nexmark Java Bid class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, SizeOf, Encode, Decode)]
pub struct Bid {
    /// Id of auction this bid is for.
    pub auction: u64,
    /// Id of the person bidding in auction.
    pub bidder: u64,
    /// Price of bid, in cents.
    pub price: usize,
    /// The channel that introduced this bidding.
    #[bincode(with_serde)]
    pub channel: ArcStr,
    /// The url of this channel.
    #[bincode(with_serde)]
    pub url: ArcStr,
    /// Instant at which this bid was made. NOTE: This may be earlier than teh
    /// system's event time.
    pub date_time: u64,
    /// Additional arbitrary payload for performance testing.
    #[bincode(with_serde)]
    pub extra: ArcStr,
}

/// An event in the auction system, either a (new) `Person`, a (new) `Auction`,
/// or a `Bid`.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, SizeOf, Encode, Decode)]
pub enum Event {
    Person(Person),
    Auction(Auction),
    Bid(Bid),
}
