use feldera_fold_probe::{Expression, Refusal, SemanticsProfile, evaluate_with_profile};
use serde::Deserialize;
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Read, Write},
};

#[derive(Deserialize)]
struct Request {
    expression: Expression,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1).peekable();
    let profile = if args.peek().is_some_and(|arg| arg == "--semantics-profile") {
        args.next();
        args.next()
            .ok_or("missing semantics profile")?
            .to_str()
            .ok_or("semantics profile must be UTF-8")?
            .parse::<SemanticsProfile>()?
    } else {
        SemanticsProfile::Runtime
    };
    let args: Vec<_> = args.collect();
    if args.len() == 1 && args[0] == "--stream" {
        return run(
            std::io::stdin().lock(),
            std::io::stdout().lock(),
            true,
            profile,
        );
    }
    if args.len() != 2 {
        return Err("usage: feldera-fold-probe [--semantics-profile PROFILE] FOLDS.jsonl OUTPUT.jsonl | --stream".into());
    }
    let input = BufReader::new(File::open(&args[0])?);
    let output = BufWriter::new(
        OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&args[1])?,
    );
    run(input, output, false, profile)
}

fn run(
    mut input: impl BufRead,
    mut output: impl Write,
    streaming: bool,
    profile: SemanticsProfile,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut index = 0usize;
    loop {
        let mut line = Vec::new();
        input
            .by_ref()
            .take(8 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if line.is_empty() {
            break;
        }
        if line.len() > 8 * 1024 * 1024 {
            return Err("fold record exceeds 8 MiB".into());
        }
        let request: Request = serde_json::from_slice(&line)?;
        let mut response = match evaluate_with_profile(&request.expression, profile) {
            Ok(value) => json!({"status": "value", "value": value}),
            Err(Refusal::Unsupported(reason)) => json!({"status": "unsupported", "reason": reason}),
            Err(Refusal::RuntimeError(reason)) => {
                json!({"status": "runtime_error", "reason": reason})
            }
            Err(Refusal::Budget) => json!({"status": "budget"}),
        };
        response["index"] = index.into();
        response["semantics_profile"] = profile.identity().into();
        serde_json::to_writer(&mut output, &response)?;
        output.write_all(b"\n")?;
        if streaming {
            output.flush()?;
        }
        index += 1;
    }
    output.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_responses_identify_the_requested_semantics_profile_even_on_refusal() {
        let double = json!({"name": "DOUBLE", "precision": 15, "scale": 0});
        let input = [
            json!({"expression": {
                "kind": "OTHER_FUNCTION", "operator": "EXP", "deterministic": true,
                "type": double,
                "arguments": [{"kind": "LITERAL", "type": double, "value": "1"}]
            }}),
            json!({"expression": {"kind": "unknown", "type": double}}),
        ]
        .iter()
        .map(|request| format!("{request}\n"))
        .collect::<String>();
        for profile in [
            SemanticsProfile::Runtime,
            SemanticsProfile::LegacyExpLog,
            SemanticsProfile::LegacyJanino,
        ] {
            let mut output = Vec::new();
            run(input.as_bytes(), &mut output, true, profile).unwrap();
            let responses: Vec<serde_json::Value> = String::from_utf8(output)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(responses.len(), 2);
            assert_eq!(responses[0]["status"], "value");
            assert_eq!(responses[1]["status"], "unsupported");
            for (index, response) in responses.iter().enumerate() {
                assert_eq!(response["index"], index);
                assert_eq!(response["semantics_profile"], profile.identity());
            }
        }
    }
}
