use feldera_correlate_probe::{Error, Fragment, decorrelate};
use serde::Serialize;
use std::{
    collections::HashSet,
    error::Error as StdError,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

#[derive(Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    Rewritten {
        id: String,
        recipe: feldera_correlate_probe::recipe::Recipe,
    },
    Refused {
        id: String,
        reason: String,
    },
}

fn main() -> Result<(), Box<dyn StdError>> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: feldera-correlate-probe FRAGMENTS.jsonl OUTPUT_DIR".into());
    }
    let directory = Path::new(&args[1]);
    fs::create_dir(directory)?;
    let mut input = BufReader::new(fs::File::open(&args[0])?);
    let mut output = BufWriter::new(fs::File::create_new(directory.join("recipes.jsonl"))?);
    let mut identities = HashSet::new();
    let mut buffer = Vec::new();
    let (mut rewritten, mut refused, mut removed) = (0_usize, 0_usize, 0_usize);
    loop {
        buffer.clear();
        let size = (&mut input)
            .take(16 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut buffer)?;
        if size == 0 {
            break;
        }
        if size > 16 * 1024 * 1024 || identities.len() >= 20_000 {
            return Err("fragment record or count limit exceeded".into());
        }
        let fragment: Fragment = serde_json::from_slice(&buffer)?;
        if fragment.id.is_empty()
            || fragment.id.len() > 4096
            || !identities.insert(fragment.id.clone())
        {
            return Err("missing, overlarge or duplicate fragment identity".into());
        }
        let outcome = match decorrelate(&fragment) {
            Ok(recipe) => {
                rewritten += 1;
                removed += recipe.removed_correlates;
                Outcome::Rewritten {
                    id: fragment.id,
                    recipe,
                }
            }
            Err(error @ Error::Unsupported(_)) | Err(error @ Error::Limit) => {
                refused += 1;
                Outcome::Refused {
                    id: fragment.id,
                    reason: error.to_string(),
                }
            }
            Err(error) => return Err(format!("{}: {error}", fragment.id).into()),
        };
        serde_json::to_writer(&mut output, &outcome)?;
        writeln!(output)?;
        output.flush()?;
    }
    if identities.is_empty() {
        return Err("empty fragment input".into());
    }
    let report = serde_json::json!({"complete":true, "fragments":identities.len(),
        "rewritten":rewritten, "refused":refused, "removed_correlates":removed,
        "runtime_executed":false, "scope":"Recipe construction, not C7 equivalence"});
    fs::write(
        directory.join("complete.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    println!("{report}");
    Ok(())
}
