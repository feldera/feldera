/*
MIT License
SPDX-License-Identifier: MIT

Copyright (c) $CURRENT_YEAR VMware, Inc
*/

//! Synchronous circuits over streams.
//!
//! A circuit consists of [operators](`operator_traits::Operator`) connected by [streams](`circuit_build::Stream`).
//! At every clock cycle, each operator in the circuit is triggered, consuming a single value from
//! each of its input streams and emitting a single value to the output stream.

mod circuit_builder;
pub use circuit_builder::*;

pub mod operator;
pub mod operator_traits;
pub mod schedule;
