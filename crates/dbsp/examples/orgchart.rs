//! Simple DBSP example.
//!
//! This is similar to the motivating example in chapter 1 of the Differential
//! Dataflow book at <https://timelydataflow.github.io/differential-dataflow/chapter_0/chapter_0.html>.  It takes a
//! collection of `Manages` structs that map from an employee ID to the
//! employee's manager, and outputs a collection of `SkipLevel` structs that
//! also include the employee's second-level manager.

use anyhow::Result;
use clap::Parser;
use dbsp::{OrdZSet, OutputHandle, Runtime, Stream};
use rkyv::{Archive, Deserialize, Serialize};
use size_of::SizeOf;
use std::hash::Hash;

type EmployeeID = u64;

/// Indicates that `manager` is the immediate manager of `employee`.
///
/// If `manager == employee` then `manager` is the CEO.
#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct Manages {
    manager: EmployeeID,
    employee: EmployeeID,
}

/// Indicates that `manager` is the immediate manager of `employee` and that
/// `grandmanager` is the immedate manager of `manager`.
#[derive(
    Default,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Debug,
    SizeOf,
    Archive,
    Serialize,
    Deserialize,
)]
#[archive_attr(derive(Clone, Ord, Eq, PartialEq, PartialOrd))]
#[archive(compare(PartialEq, PartialOrd))]
struct SkipLevel {
    grandmanager: EmployeeID,
    manager: EmployeeID,
    employee: EmployeeID,
}

type SkipLevels = OrdZSet<SkipLevel>;

fn print_output(output: &OutputHandle<OrdZSet<SkipLevel>>) {
    for (key, _value, weight) in output.consolidate().iter() {
        println!(
            "    ({}, {}, {}) {:+}",
            key.grandmanager, key.manager, key.employee, weight
        );
    }
    println!();
}

#[derive(Debug, Clone, Parser)]
struct Args {
    /// Number of employees.
    #[clap(long, default_value = "10")]
    size: u64,

    /// Number of threads.
    #[clap(long, default_value = "2")]
    threads: usize,
}

fn main() -> Result<()> {
    let Args { threads, size } = Args::parse();

    let (mut dbsp, (hmanages, output)) = Runtime::init_circuit(threads, |circuit| {
        let (manages, hmanages) = circuit.add_input_zset::<Manages>();

        let manages_by_manager = manages.map_index(|m| (m.manager, m.clone()));
        let manages_by_employee = manages.map_index(|m| (m.employee, m.clone()));

        // If Manages { manager, employee: common } and Manages { manager: common,
        // employee } then SkipLevel { grandmanager: manager, manager: common,
        // employee }.
        let skiplevels: Stream<_, SkipLevels> =
            manages_by_employee.join(&manages_by_manager, |common, m1, m2| SkipLevel {
                grandmanager: m1.manager,
                manager: *common,
                employee: m2.employee,
            });

        Ok((hmanages, skiplevels.output()))
    })
    .unwrap();

    // Initially, let each manager be the employee's ID divided by 2 (yielding a
    // binary tree management structure).  Then run it through DBSP in a single
    // step.
    for employee in 0..size {
        hmanages.push(
            Manages {
                manager: employee / 2,
                employee,
            },
            1,
        );
    }
    dbsp.step().unwrap();
    println!("Initialization:");
    print_output(&output);

    // Second, replace the binary management structure by a ternary one.  This time,
    // print the changes after each step, just to show how that works.
    for employee in 1..size {
        hmanages.push(
            Manages {
                manager: employee / 2,
                employee,
            },
            -1,
        );
        hmanages.push(
            Manages {
                manager: employee / 3,
                employee,
            },
            1,
        );
        dbsp.step().unwrap();
        println!("Changes from adjusting {employee}'s manager:");
        print_output(&output);

        let profile = dbsp.retrieve_profile().unwrap();
        println!("total used bytes: {}", profile.total_used_bytes().unwrap());
        println!(
            "total allocated bytes: {}",
            profile.total_allocated_bytes().unwrap()
        );
        println!(
            "total shared bytes: {}",
            profile.total_shared_bytes().unwrap()
        );
        println!(
            "num table entries: {}",
            profile.total_relation_size().unwrap()
        );
    }

    let profile = dbsp.retrieve_profile().unwrap();

    println!(
        "used bytes profile: {:?}",
        profile.used_bytes_profile().unwrap()
    );

    dbsp.kill().unwrap();

    Ok(())
}
