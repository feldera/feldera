//! Model structs for the Nexmark benchmark suite.
//!
//! Based on the equivalent [Nexmark Flink Java model classes](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model).

use std::time::SystemTime;

/// The Nexmark Person model based on the [Nexmark Java Person class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Person.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Person {
    pub id: u64,
    pub name: String,
    pub email_address: String,
    pub credit_card: String,
    pub city: String,
    pub state: String,
    pub date_time: SystemTime,
    pub extra: String,
}

/// The Nexmark Auction model based on the [Nexmark Java Auction class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Auction.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Auction {
    pub id: u64,
    pub item_name: String,
    pub description: String,
    pub initial_bid: usize,
    pub reserve: usize,
    pub date_time: SystemTime,
    pub expires: SystemTime,
    pub seller: u64,
    pub category: usize,
}

/// The Nexmark Bid model based on the [Nexmark Java Bid class](https://github.com/nexmark/nexmark/blob/v0.2.0/nexmark-flink/src/main/java/com/github/nexmark/flink/model/Bid.java).
///
/// Note that Rust can simply derive the equivalent methods on the Java
/// class.
#[derive(Debug, Eq, Hash, PartialEq)]
pub struct Bid {
    pub auction: u64,
    pub bidder: u64,
    pub price: usize,
    pub date_time: SystemTime,
}
