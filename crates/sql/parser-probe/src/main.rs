use feldera_parser_probe::{keywords, measure, reserved_keywords, Verdict};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashSet},
    env,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    path::Path,
    process::ExitCode,
};

#[derive(Deserialize)]
struct Case {
    id: String,
    submissions: Vec<Submission>,
}

#[derive(Deserialize, Serialize)]
struct Submission {
    sql: String,
    many: bool,
    batch: u64,
}

#[derive(Default, Serialize)]
struct Counts {
    accepted_cases: usize,
    rejected_cases: usize,
    accepted_submissions: usize,
    rejected_submissions: usize,
    statements_in_accepted_submissions: usize,
}

fn digest(path: &Path) -> io::Result<String> {
    let mut input = File::open(path)?;
    let mut hasher = Sha256::new();
    io::copy(&mut input, &mut hasher)?;
    Ok(format!("{:x}", hasher.finalize()))
}

fn record(
    case: Case,
    counts: &mut BTreeMap<String, Counts>,
    include_ast: bool,
) -> Result<Value, Box<dyn Error>> {
    if case.id.trim().is_empty() || case.submissions.is_empty() {
        return Err("each case needs an identity and SQL submissions".into());
    }
    let mut submissions = Vec::new();
    let mut accepted = BTreeMap::new();
    for submission in case.submissions {
        let mut measurements = measure(&submission.sql, include_ast);
        if !submission.many {
            for measurement in &mut measurements {
                if let Verdict::Accepted(parsed) = &measurement.result {
                    if parsed.statements.len() != 1 {
                        measurement.result = Verdict::Rejected {
                            error: format!(
                                "single-statement submission contains {} statements",
                                parsed.statements.len()
                            ),
                        };
                    }
                }
            }
        }
        for measurement in &measurements {
            let key = format!("{}/{}", measurement.version, measurement.dialect);
            let case_accepted = accepted.entry(key.clone()).or_insert(true);
            let count = counts.entry(key).or_default();
            match &measurement.result {
                Verdict::Accepted(parsed) => {
                    count.accepted_submissions += 1;
                    count.statements_in_accepted_submissions += parsed.statements.len();
                }
                Verdict::Rejected { .. } => {
                    *case_accepted = false;
                    count.rejected_submissions += 1;
                }
            }
        }
        submissions.push(json!({"source": submission, "measurements": measurements}));
    }
    for (key, case_accepted) in &accepted {
        let count = counts.get_mut(key).expect("measured dialect has counters");
        if *case_accepted {
            count.accepted_cases += 1;
        } else {
            count.rejected_cases += 1;
        }
    }
    Ok(json!({"id": case.id, "accepted": accepted, "submissions": submissions}))
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() == 1 && args[0] == "--keywords" {
        println!("{}", serde_json::to_string(&keywords())?);
        return Ok(());
    }
    if args.len() == 1 && args[0] == "--reserved-keywords" {
        println!("{}", serde_json::to_string(reserved_keywords())?);
        return Ok(());
    }
    if !(args.len() == 2 || args.len() == 3 && args[2] == "--include-ast") {
        return Err("usage: feldera-parser-probe INPUT.jsonl OUTPUT_DIR [--include-ast]".into());
    }
    let input_path = Path::new(&args[0]);
    let directory = Path::new(&args[1]);
    let input_digest = digest(input_path)?;
    let input = BufReader::new(File::open(input_path)?);
    fs::create_dir(directory)?;
    let mut output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("cases.jsonl"))?,
    );
    let mut counts = BTreeMap::new();
    let mut identities = HashSet::new();
    for (line, source) in input.lines().enumerate() {
        let case: Case = serde_json::from_str(&source?)
            .map_err(|error| format!("input line {}: {error}", line + 1))?;
        if !identities.insert(case.id.clone()) {
            return Err(
                format!("duplicate case identity on line {}: {}", line + 1, case.id).into(),
            );
        }
        serde_json::to_writer(&mut output, &record(case, &mut counts, args.len() == 3)?)?;
        writeln!(output)?;
    }
    if identities.is_empty() {
        return Err("empty input is not parser evidence".into());
    }
    output.flush()?;
    if digest(input_path)? != input_digest {
        return Err("input changed during measurement".into());
    }
    let report = json!({
        "schema_version": 1,
        "cases": identities.len(),
        "counts": counts,
        "input_sha256": input_digest,
        "executable_sha256": digest(&env::current_exe()?)?,
        "include_ast": args.len() == 3,
        "parser_implementations": {
            "0.64.0/generic": "unmodified crates.io sqlparser 0.64.0",
            "0.64.0/feldera_prototype": "vendored sqlparser 0.64.0: join/argument hooks, typed LEFT ASOF and TABLE arguments, repeated ARRAY suffixes, VALUES grammar hook, recursive type visitor and structured SQL hints; see vendor/sqlparser/FELDERA-PATCHES.md"
        },
        "token_rewrites": true,
        "token_rewrite_scope": {"0.64.0/feldera_prototype": "Unicode identifiers and adjacent charset-introducer tokens"},
        "ast_reject_pass": true,
        "ast_reject_scope": {"0.64.0/feldera_prototype": "typed recursive view and standalone LATENESS extensions with complete top-level consumption; statement-family allowlist and scoped DROP/SET/description options; qualified datetime differences misparsed as aliases; function-specific inline null treatment and FILTER ordering; reserved words in declarations, relations, references and projection aliases; LATENESS contexts and non-query expressions; literal hex validation and legacy endpoint trimming; typed NULL literals; duplicate DEFAULT/PRIMARY KEY attributes; string property lists; FROM-dependent clauses, recursive custom types, INSERT INTO, interval values/units, lambda parameters, colon and numeric field access, bitwise OR, reserved call names, table-call lookahead; typed hint grammar and attachment; explicit Query/SELECT/INSERT/UPDATE/DELETE field policies"},
        "exhaustive_ast_rejection": false,
        "s1_equivalence_checked": false,
        "dialect_scope": "fixed prototype; corpus compiler options are not translated",
        "complete": true
    });
    let mut complete = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(directory.join("complete.json"))?;
    serde_json::to_writer_pretty(&mut complete, &report)?;
    writeln!(complete)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("parser measurement failed: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_bad_submission_rejects_the_whole_case() {
        let case = Case {
            id: "mixed".into(),
            submissions: vec![
                Submission {
                    sql: "SELECT 1; SELECT 2".into(),
                    many: true,
                    batch: 0,
                },
                Submission {
                    sql: "SELECT (".into(),
                    many: false,
                    batch: 1,
                },
            ],
        };
        let mut counts = BTreeMap::new();
        let result = record(case, &mut counts, false).unwrap();
        assert_eq!(result["submissions"][1]["source"]["batch"], 1);
        assert_eq!(counts.len(), 4);
        for count in counts.values() {
            assert_eq!(count.accepted_cases, 0);
            assert_eq!(count.rejected_cases, 1);
            assert_eq!(count.accepted_submissions, 1);
            assert_eq!(count.rejected_submissions, 1);
            assert_eq!(count.statements_in_accepted_submissions, 2);
        }
    }

    #[test]
    fn single_statement_submission_rejects_a_second_statement() {
        let case = Case {
            id: "single".into(),
            submissions: vec![Submission {
                sql: "SELECT 1; SELECT 2".into(),
                many: false,
                batch: 0,
            }],
        };
        let mut counts = BTreeMap::new();
        record(case, &mut counts, false).unwrap();
        assert!(counts.values().all(|count| count.rejected_cases == 1));
    }
}
