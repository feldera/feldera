//! End-to-end tests of non-linear recursive circuits against models.
//!
//! A recursion is non-linear when one of its joins has two inputs that both
//! depend on the recursion.  Non-linear recursion exercises behaviors that
//! simple recursion doesn't.  E.g., it exercises the work that nested
//! operators schedule for later iterations, such as a join whose input
//! changes in several iterations of a run.
//!
//! Each test runs a recursive program through a workload of transactions and
//! checks, after every transaction, that the program's accumulated output
//! equals its fixed point over all input so far, computed from scratch in
//! plain Rust.  A scripted workload runs its program with both recursion APIs,
//! [`recursive`](crate::ChildCircuit::recursive) and the
//! [recursion builder](crate::ChildCircuit::recursion_builder); a random one
//! picks an API at random.  The [`bounded`] module tests recursions that the
//! recursion builder stops after a bounded number of iterations, short of the
//! fixed point.
//!
//! The [`harness`] module contains common test harness for these tests.
mod bill_of_materials;
mod bounded;
mod closure;
mod galen;
mod harness;
mod parsing;
mod points_to;
mod state_machine;
mod tree;
mod trips;
mod walk_counts;
