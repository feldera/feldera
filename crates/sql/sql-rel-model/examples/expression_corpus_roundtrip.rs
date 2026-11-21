//! Streaming transport for contextual expression fragments, not whole-program S1.
use feldera_sql_rel_model::{
    ExpressionLimits, LiteralLimits, ValidationLimits, expressions::ExpressionTable,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    env,
    error::Error,
    fs,
    io::{BufRead, BufReader, BufWriter, Read, Write},
    path::Path,
};

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Fragment {
    relation_ordinal: u32,
    table: ExpressionTable,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    scope: CaptureScope,
    fragments: Vec<Fragment>,
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case")]
enum CaptureScope {
    OptimizedProjects,
    LocalFields,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: expression_corpus_roundtrip INPUT.jsonl OUTPUT_DIR".into());
    }
    let mut reader = BufReader::new(fs::File::open(&args[0])?);
    let output = Path::new(&args[1]);
    fs::create_dir(output)?;
    let mut writer = BufWriter::new(fs::File::create_new(output.join("cases.jsonl"))?);
    let mut identities = HashSet::new();
    let mut bytes = Vec::new();
    let mut total_bytes = 0_u64;
    let mut scope = None;
    let (mut fragments, mut expressions, mut roots) = (0_u64, 0_u64, 0_u64);
    loop {
        bytes.clear();
        let read = (&mut reader)
            .take(16 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut bytes)?;
        if read == 0 {
            break;
        }
        total_bytes += read as u64;
        if read > 16 * 1024 * 1024 || total_bytes > 1024 * 1024 * 1024 {
            return Err("expression corpus byte limit exceeded".into());
        }
        if identities.len() >= 20_000 {
            return Err("expression corpus case limit exceeded".into());
        }
        let case: Case = serde_json::from_slice(&bytes)?;
        if scope.is_some_and(|previous| previous != case.scope) {
            return Err("mixed expression capture scopes".into());
        }
        scope = Some(case.scope);
        if case.id.is_empty() || case.id.len() > 4096 || !identities.insert(case.id.clone()) {
            return Err("missing, overlarge or duplicate case identity".into());
        }
        if !case
            .fragments
            .windows(2)
            .all(|pair| pair[0].relation_ordinal < pair[1].relation_ordinal)
        {
            return Err("fragment relation ordinals must be distinct and increasing".into());
        }
        for fragment in &case.fragments {
            fragment
                .table
                .validate(
                    ValidationLimits::default(),
                    LiteralLimits::default(),
                    ExpressionLimits::default(),
                )
                .map_err(|error| {
                    format!(
                        "{} fragment {}: {error}",
                        case.id, fragment.relation_ordinal
                    )
                })?;
            expressions += fragment.table.expressions.len() as u64;
            roots += fragment.table.roots.len() as u64;
        }
        fragments += case.fragments.len() as u64;
        serde_json::to_writer(&mut writer, &case)?;
        writeln!(writer)?;
    }
    if identities.is_empty() {
        return Err("empty expression corpus".into());
    }
    writer.flush()?;
    serde_json::to_writer_pretty(
        fs::File::create_new(output.join("complete.json"))?,
        &serde_json::json!({
            "schema_version":1,"complete":true,"cases":identities.len(),
            "fragments":fragments,"expressions":expressions,"roots":roots,
            "scope":scope,"runtime_executed":false,"S1_parity_proven":false
        }),
    )?;
    Ok(())
}
