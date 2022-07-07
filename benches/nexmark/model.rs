//! Model structs for the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink Java model classes](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model).

pub type DateTime = std::time::SystemTime;
pub type Id = usize;

/// The Nexmark Person model based on the [Nexmark Java Person class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Person {
    pub id: Id,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: DateTime,
    pub extra: String,
}

/// The Nexmark Auction model based on the [Nexmark Java Auction class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Auction {
    pub id: Id,
    pub item_name: String,
    pub description: String,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: DateTime,
    pub expires: DateTime,
    pub seller: Id,
    pub category: Id,
}

/// The Nexmark Bid model based on the [Nexmark Java Bid class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Bid {
    pub auction: Id,
    pub bidder: Id,
    pub price: usize,
    pub date_time: DateTime,
}
