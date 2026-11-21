//! Independent integer value oracle for bounded C9/C7 SQL fixtures.
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, env, fs, io::Write};

type Bag<T> = BTreeMap<T, i64>;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Equal,
    Less,
    In,
    Between,
    Case,
    Join,
    UnionAll,
    IntersectAll,
    ExceptAll,
}

#[derive(Clone, Debug, Deserialize, Ord, PartialOrd, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TextRow {
    id: i32,
    a: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Ord, PartialOrd, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct IntegerRow {
    id: i32,
    b: Option<i64>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Change<T> {
    row: T,
    weight: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Transaction {
    text: Vec<Change<TextRow>>,
    integers: Vec<Change<IntegerRow>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Fixture {
    id: String,
    operation: Operation,
    transactions: Vec<Transaction>,
}

#[derive(Clone, Debug, Ord, PartialOrd, PartialEq, Eq, Serialize)]
#[serde(untagged)]
enum Cell {
    Null,
    Boolean(bool),
    Integer(i64),
}

type Row = Vec<Cell>;

#[derive(Serialize)]
struct ResultFixture {
    #[serde(flatten)]
    fixture: Fixture,
    deltas: Vec<Vec<Change<Row>>>,
}

fn value(input: &Option<String>) -> Result<Option<i64>, String> {
    input
        .as_ref()
        .map(|string| {
            string
                .parse::<i64>()
                .map_err(|_| format!("not a signed BIGINT fixture: {string:?}"))
        })
        .transpose()
}

fn integer(result: Option<i64>) -> Cell {
    result.map_or(Cell::Null, Cell::Integer)
}

fn boolean(result: Option<bool>) -> Cell {
    result.map_or(Cell::Null, Cell::Boolean)
}

fn scalar(operation: Operation, input: Option<i64>) -> Result<Cell, String> {
    Ok(match operation {
        Operation::Equal => boolean(input.map(|a| a == i64::MAX)),
        Operation::Less => boolean(input.map(|a| a < 0)),
        // The list contains NULL. A known match wins, while a miss is unknown.
        Operation::In => {
            boolean(input.and_then(|a| [i64::MIN, 0, i64::MAX].contains(&a).then_some(true)))
        }
        Operation::Between => boolean(input.map(|a| (-1..=1).contains(&a))),
        Operation::Case => integer(input.map(|a| if a < 0 { -1 } else { a })),
        _ => return Err("expected a scalar operation".into()),
    })
}

fn add<T: Ord>(bag: &mut Bag<T>, key: T, weight: i64) -> Result<(), String> {
    let entry = bag.entry(key).or_default();
    *entry = entry.checked_add(weight).ok_or("bag weight overflow")?;
    Ok(())
}

fn apply<T: Ord + Clone>(state: &mut Bag<T>, delta: &[Change<T>]) -> Result<(), String> {
    for change in delta {
        if !matches!(change.weight, -32..=-1 | 1..=32) {
            return Err("fixture change weight outside limits".into());
        }
        add(state, change.row.clone(), change.weight)?;
    }
    if state.values().any(|count| *count < 0) {
        return Err("fixture retracts rows that are absent".into());
    }
    state.retain(|_, weight| *weight != 0);
    if state.len() > 256 {
        return Err("fixture carries too many rows".into());
    }
    Ok(())
}

fn evaluate(
    operation: Operation,
    text: &Bag<TextRow>,
    integers: &Bag<IntegerRow>,
) -> Result<Bag<Row>, String> {
    let mut result = Bag::new();
    match operation {
        Operation::Join => {
            for (left, left_count) in text {
                let Some(key) = value(&left.a)? else { continue };
                for (right, right_count) in integers {
                    if right.b == Some(key) {
                        let row = vec![
                            Cell::Integer(i64::from(left.id)),
                            Cell::Integer(i64::from(right.id)),
                        ];
                        let weight = left_count
                            .checked_mul(*right_count)
                            .ok_or("join weight overflow")?;
                        add(&mut result, row, weight)?;
                    }
                }
            }
        }
        Operation::UnionAll | Operation::IntersectAll | Operation::ExceptAll => {
            let mut left = Bag::new();
            let mut right = Bag::new();
            for (row, count) in text {
                add(&mut left, value(&row.a)?, *count)?;
            }
            for (row, count) in integers {
                add(&mut right, row.b, *count)?;
            }
            for (key, count) in left {
                let other = right.get(&key).copied().unwrap_or(0);
                let weight = match operation {
                    Operation::UnionAll => count,
                    Operation::IntersectAll => count.min(other),
                    Operation::ExceptAll => (count - other).max(0),
                    _ => unreachable!(),
                };
                add(&mut result, vec![integer(key)], weight)?;
            }
            if matches!(operation, Operation::UnionAll) {
                for (key, count) in right {
                    add(&mut result, vec![integer(key)], count)?;
                }
            }
        }
        op => {
            if !integers.is_empty() {
                return Err("scalar fixture has unexpected right rows".into());
            }
            for (row, count) in text {
                add(
                    &mut result,
                    vec![
                        Cell::Integer(i64::from(row.id)),
                        scalar(op, value(&row.a)?)?,
                    ],
                    *count,
                )?;
            }
        }
    }
    result.retain(|_, weight| *weight != 0);
    Ok(result)
}

fn run(fixture: Fixture) -> Result<ResultFixture, String> {
    let mut text = Bag::new();
    let mut integers = Bag::new();
    let mut previous = Bag::new();
    let mut deltas = Vec::new();
    if fixture.id.is_empty() || fixture.transactions.is_empty() || fixture.transactions.len() > 32 {
        return Err("invalid fixture scope".into());
    }
    for transaction in &fixture.transactions {
        apply(&mut text, &transaction.text)?;
        apply(&mut integers, &transaction.integers)?;
        let next = evaluate(fixture.operation, &text, &integers)?;
        let mut delta = next.clone();
        for (row, weight) in previous {
            add(&mut delta, row, -weight)?;
        }
        deltas.push(
            delta
                .into_iter()
                .filter(|(_, weight)| *weight != 0)
                .map(|(row, weight)| Change { row, weight })
                .collect(),
        );
        previous = next;
    }
    Ok(ResultFixture { fixture, deltas })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let [input, output] = args.as_slice() else {
        return Err("usage: type-value-vectors INPUT.json OUTPUT.json".into());
    };
    if fs::metadata(input)?.len() > 1024 * 1024 {
        return Err("fixture file exceeds 1 MiB".into());
    }
    let fixtures: Vec<Fixture> = serde_json::from_slice(&fs::read(input)?)?;
    if fixtures.is_empty() || fixtures.len() > 64 {
        return Err("invalid fixture population".into());
    }
    let mut ids = std::collections::HashSet::new();
    if fixtures.iter().any(|fixture| !ids.insert(&fixture.id)) {
        return Err("duplicate fixture IDs".into());
    }
    let results = fixtures
        .into_iter()
        .map(run)
        .collect::<Result<Vec<_>, _>>()?;
    let mut output = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(output)?;
    serde_json::to_writer_pretty(&mut output, &results)?;
    writeln!(output)?;
    Ok(())
}
