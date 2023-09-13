#![allow(dead_code)]

use anyhow::{anyhow, Result as AnyResult};
use clap::{Args, Command, FromArgMatches, Parser};
use dbsp_adapters::server::ServerArgs;
use serde::Deserialize;
use std::{fs::File, io::BufReader};

#[derive(Parser, Debug)]
struct JitArgs {
    /// JIT IR file
    #[arg(short, long)]
    ir_file: String,
    /// Table schema file
    #[arg(short, long)]
    schema_file: String,
}

/// SQL program schema consisting of input and output relations.
#[derive(Deserialize)]
struct Schema {
    #[serde(default)]
    inputs: Vec<TableSchema>,
    #[serde(default)]
    outputs: Vec<TableSchema>,
}

/// SQL table or view schema.
#[derive(Deserialize)]
struct TableSchema {
    name: String,
    columns: Vec<Column>,
}

/// Table column schema.
#[derive(Deserialize)]
struct Column {
    name: String,
}

pub fn main() -> AnyResult<()> {
    let cli = Command::new("Run a Feldera pipeline");
    let cli = JitArgs::augment_args(cli);
    let cli = ServerArgs::augment_args(cli);

    let matches = cli.get_matches();

    let jit_args = JitArgs::from_arg_matches(&matches)?;

    let schema = File::open(&jit_args.schema_file)
        .map_err(|e| anyhow!("Error opening schema file '{}': {e}", &jit_args.schema_file))?;

    let _schema: Schema = serde_json::from_reader(BufReader::new(schema))
        .map_err(|e| anyhow!("Error parsing schema file 'args.schema_file' : {e}"))?;

    Ok(())
}
