//! Compare two PassCorpusOracle streams against the population that produced them.
use crate::files::Error;
use feldera_sql_difftest::{compare_circuits, parse_json};
use serde::Deserialize;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;

struct Records<'a> {
    path: &'a Path,
    input: BufReader<File>,
    line: Vec<u8>,
    number: usize,
}

impl<'a> Records<'a> {
    fn open(path: &'a Path) -> Result<Self, Error> {
        let file =
            File::open(path).map_err(|e| Error::Input(format!("{}: {e}", path.display())))?;
        Ok(Self {
            path,
            input: BufReader::new(file),
            line: Vec::new(),
            number: 0,
        })
    }

    fn next(&mut self) -> Result<Option<Value>, Error> {
        self.line.clear();
        self.number += 1;
        let invalid = |e: &dyn std::fmt::Display| {
            Error::Input(format!("{} line {}: {e}", self.path.display(), self.number))
        };
        if self
            .input
            .read_until(b'\n', &mut self.line)
            .map_err(|e| invalid(&e))?
            == 0
        {
            return Ok(None);
        }
        parse_json(&self.line).map(Some).map_err(|e| invalid(&e))
    }
}

#[derive(Deserialize, PartialEq)]
struct Placement {
    container: String,
    index: u64,
    iteration: u64,
    instance: u64,
    names_before: u64,
    names_after: u64,
    diagnostics: Vec<Value>,
    #[serde(flatten)]
    state: BTreeMap<String, Value>,
}

#[derive(Deserialize)]
struct Invocation {
    before: Value,
    after: Value,
    #[serde(flatten)]
    placement: Placement,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Observation {
    id: String,
    input_sha256: String,
    s2_sha256: String,
    gen2: bool,
    pass: String,
    invocations: Vec<Invocation>,
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}

fn compare_case(pass: &str, source: &Value, rows: [Value; 2]) -> Result<usize, String> {
    let [control, candidate] = rows.map(serde_json::from_value::<Observation>);
    let control = control.map_err(|e| format!("invalid control observation: {e}"))?;
    let candidate = candidate.map_err(|e| format!("invalid candidate observation: {e}"))?;
    for row in [&control, &candidate] {
        if source["id"].as_str() != Some(&row.id)
            || source["input_sha256"].as_str() != Some(&row.input_sha256)
            || source
                .pointer("/options/ioOptions/gen2")
                .and_then(Value::as_bool)
                != Some(row.gen2)
            || row.pass != pass
            || !is_sha256(&row.input_sha256)
            || !is_sha256(&row.s2_sha256)
        {
            return Err("population identity, engine mode, pass name or digest differs".into());
        }
    }
    if control.s2_sha256 != candidate.s2_sha256 {
        return Err("different S2 checkpoints".into());
    }
    let count = control.invocations.len();
    if count != candidate.invocations.len() {
        return Err(format!(
            "expected {count} invocations, found {}",
            candidate.invocations.len()
        ));
    }
    for (i, (left, right)) in control
        .invocations
        .iter()
        .zip(&candidate.invocations)
        .enumerate()
    {
        if left.placement != right.placement {
            return Err(format!(
                "invocation {i}: placement, generated names, diagnostics or pass state differs"
            ));
        }
        for (boundary, a, b) in [
            ("entry", &left.before, &right.before),
            ("exit", &left.after, &right.after),
        ] {
            compare_circuits(a, b).map_err(|e| format!("invocation {i} {boundary}: {e}"))?;
        }
    }
    Ok(count)
}

pub fn compare(
    pass: &str,
    population: &Path,
    control: &Path,
    candidate: &Path,
) -> Result<String, Error> {
    let mut population = Records::open(population)?;
    let mut control = Records::open(control)?;
    let mut candidate = Records::open(candidate)?;
    let mut seen = HashSet::new();
    let mut invocations = 0;
    while let Some(row) = population.next()? {
        let id = row["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::Input(format!(
                    "missing case ID at population line {}",
                    population.number
                ))
            })?;
        if !seen.insert(id.to_owned()) {
            return Err(Error::Input(format!("duplicate case {id}")));
        }
        let required = |stream: &mut Records<'_>| {
            stream.next()?.ok_or_else(|| {
                Error::Difference(format!("{} ended before {id}", stream.path.display()))
            })
        };
        let pair = [required(&mut control)?, required(&mut candidate)?];
        invocations +=
            compare_case(pass, &row, pair).map_err(|e| Error::Difference(format!("{id}: {e}")))?;
    }
    if control.next()?.is_some() || candidate.next()?.is_some() {
        return Err(Error::Difference(
            "extra observations after the population ended".into(),
        ));
    }
    if invocations == 0 {
        return Err(Error::Difference(format!(
            "no invocations of {pass} in the population"
        )));
    }
    Ok(format!(
        "{pass} matched at {invocations} invocations across {} cases",
        seen.len()
    ))
}
