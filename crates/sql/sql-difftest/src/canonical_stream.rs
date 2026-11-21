use crate::files::Error;
use feldera_sql_difftest::{canonical_dump_bytes, parse_json};
use std::io::{self, BufRead, Write};

/// One request and response per physical line; flush each response for RPC callers.
pub fn run(verify_ir: bool) -> Result<(), Error> {
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    let mut line = Vec::new();
    loop {
        line.clear();
        if input
            .read_until(b'\n', &mut line)
            .map_err(|e| Error::Input(e.to_string()))?
            == 0
        {
            return Ok(());
        }
        let mut dump = parse_json(&line).map_err(|e| Error::Difference(e.to_string()))?;
        if verify_ir {
            crate::roundtrip::verify(&mut dump)?;
        }
        let canonical =
            canonical_dump_bytes(&dump).map_err(|e| Error::Difference(e.to_string()))?;
        output
            .write_all(&canonical)
            .and_then(|()| output.write_all(b"\n"))
            .and_then(|()| output.flush())
            .map_err(|e| Error::Input(e.to_string()))?;
    }
}
