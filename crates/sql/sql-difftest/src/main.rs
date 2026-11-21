use feldera_sql_difftest::{
    compare_circuits, compare_dumps, compare_final_s3,
    corpus::{compare_zsets, portable_output_zset},
};
use std::{env, path::Path, process::ExitCode};

mod canonical_stream;
mod corpus_files;
mod diagnostic_comparison;
mod files;
mod optimizer_trace;
mod probe_files;
mod roundtrip;
use files::{read, Error};
use probe_files::DiagnosticPolicy;

fn main() -> ExitCode {
    const USAGE: &str = "Usage: sql-difftest [--envelope|--final-s3|--zsets] EXPECTED.json ACTUAL.json\n       sql-difftest --directories EXPECTED_DIR ACTUAL_DIR\n       sql-difftest [--validate-corpus|--validate-portable-outputs] CORPUS_DIR\n       sql-difftest --portable-output-zset ZSET.json\n       sql-difftest --summarize-optimizer TRACE.jsonl\n       sql-difftest --validate-probes GOLDEN_DIR\n       sql-difftest [--compare-probes|--compare-probes-c0] EXPECTED_DIR ACTUAL_DIR\n       sql-difftest [--canonicalize-stream|--verify-ir-stream]\n       sql-difftest --roundtrip-circuit INPUT.json";
    let mut args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() == 2 && args[0] == "--roundtrip-circuit" {
        return match roundtrip::run(Path::new(&args[1])) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => failure(error),
        };
    }
    if args.len() == 1
        && matches!(
            args[0].to_str(),
            Some("--canonicalize-stream" | "--verify-ir-stream")
        )
    {
        return match canonical_stream::run(args[0] == "--verify-ir-stream") {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => failure(error),
        };
    }
    if args.len() == 1 && matches!(args[0].to_str(), Some("--help" | "-h")) {
        println!(
            "{USAGE}\nCompare raw circuits or versioned observations. Exit codes: 0 equal, 1 different or invalid dump, 2 usage or file error."
        );
        return ExitCode::SUCCESS;
    }
    if args.len() == 2
        && (args[0] == "--validate-corpus" || args[0] == "--validate-portable-outputs")
    {
        return match corpus_files::validate(
            Path::new(&args[1]),
            args[0] == "--validate-portable-outputs",
        ) {
            Ok(summary) => {
                println!("{summary}");
                ExitCode::SUCCESS
            }
            Err(error) => failure(error),
        };
    }
    if args.len() == 2 && args[0] == "--portable-output-zset" {
        let output = read(Path::new(&args[1])).and_then(|value| {
            let bag = portable_output_zset(&value)
                .map_err(|error| Error::Difference(error.to_string()))?;
            serde_json::to_string(&bag).map_err(|error| Error::Input(error.to_string()))
        });
        return match output {
            Ok(output) => {
                println!("{output}");
                ExitCode::SUCCESS
            }
            Err(error) => failure(error),
        };
    }
    if args.len() == 2 && args[0] == "--summarize-optimizer" {
        return match optimizer_trace::summarize(Path::new(&args[1])) {
            Ok(summary) => {
                println!("{summary}");
                ExitCode::SUCCESS
            }
            Err(error) => failure(error),
        };
    }
    if args.len() == 2 && args[0] == "--validate-probes" {
        return match probe_files::validate(Path::new(&args[1])) {
            Ok(summary) => {
                println!("{summary}");
                ExitCode::SUCCESS
            }
            Err(error) => failure(error),
        };
    }
    let probe_policy = match args.first().and_then(|arg| arg.to_str()) {
        Some("--compare-probes") => Some(DiagnosticPolicy::Exact),
        Some("--compare-probes-c0") => Some(DiagnosticPolicy::C0),
        _ => None,
    };
    if let Some(policy) = probe_policy.filter(|_| args.len() == 3) {
        return match probe_files::compare(Path::new(&args[1]), Path::new(&args[2]), policy) {
            Ok(summary) => {
                println!("{summary}");
                ExitCode::from(u8::from(summary["equal"] != true))
            }
            Err(error) => failure(error),
        };
    }
    let final_s3 = args.first().is_some_and(|arg| arg == "--final-s3");
    let envelope = args.first().is_some_and(|arg| arg == "--envelope");
    let directories = args.first().is_some_and(|arg| arg == "--directories");
    let zsets = args.first().is_some_and(|arg| arg == "--zsets");
    if envelope || directories || final_s3 || zsets {
        args.remove(0);
    }
    if args.len() != 2 {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    if directories {
        return match files::compare_directories(Path::new(&args[0]), Path::new(&args[1])) {
            Ok(count) => {
                println!("{count} observations are structurally equal");
                ExitCode::SUCCESS
            }
            Err(error) => failure(error),
        };
    }
    let values = read(Path::new(&args[0]))
        .and_then(|expected| read(Path::new(&args[1])).map(|actual| (expected, actual)));
    match values {
        Ok((expected, actual)) => match if final_s3 {
            compare_final_s3(&expected, &actual)
        } else if envelope {
            compare_dumps(&expected, &actual)
        } else if zsets {
            compare_zsets(&expected, &actual)
        } else {
            compare_circuits(&expected, &actual)
        } {
            Ok(()) => {
                if zsets {
                    println!("Typed Z-sets are equal");
                } else {
                    println!("Circuits are structurally equal");
                }
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{error}");
                ExitCode::from(1)
            }
        },
        Err(error) => failure(error),
    }
}

fn failure(error: Error) -> ExitCode {
    eprintln!("{error}");
    ExitCode::from(match error {
        Error::Input(_) => 2,
        Error::Difference(_) => 1,
    })
}
