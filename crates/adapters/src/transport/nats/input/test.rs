//! Tests of the NATS input connector that reach no engine: configuration validation, the
//! checks of resume positions and replay ranges, and authentication. The connector suites
//! `nats_input` and `nats_pipeline` cover the connector with an engine.

mod auth_tests;
mod custom_tests;
