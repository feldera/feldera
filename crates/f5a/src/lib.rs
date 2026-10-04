//! f5a: a k9s-style terminal console for Feldera.
//!
//! The crate is layered so that every layer below the terminal runtime is
//! testable without a network or a TTY:
//!
//! | Layer | Modules | Responsibility |
//! |-------|---------|----------------|
//! | Domain | [`model`] | Tolerant JSON -> typed views of API payloads |
//! | Transport | [`http`], [`gateway`] | One HTTP client; reads are raw JSON, writes go through `feldera-rest-api` |
//! | State | [`app`] | Pure `Msg -> state change -> Vec<Cmd>` state machine |
//! | Runtime | [`poll`], [`runner`] | Background polling and the terminal event loop |
//! | Presentation | [`ui`] | Pure `&App -> Frame` rendering |
//!
//! Reads deliberately avoid the generated OpenAPI types: the console must
//! render whatever a server of any version returns, so it maps raw
//! `serde_json::Value` payloads into domain types field by field and treats
//! every missing field as "unknown", never as an error.

pub mod app;
pub mod browser;
pub mod gateway;
pub mod http;
pub mod model;
pub mod options;
pub mod poll;
pub mod runner;
#[cfg(test)]
pub mod testutil;
pub mod ui;
