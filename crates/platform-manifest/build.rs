//! Build-time generator for the built-in connector manifest.
//!
//! Walks [`feldera_adapterlib_meta::metadata_registry`] (populated by
//! `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` statics in
//! `dbsp_adapters` and `feldera-datagen`, pulled in via `[build-dependencies]`)
//! and emits `$OUT_DIR/platform_manifest.json`.  The lib's `include_str!`
//! reads this file at compile time so the resulting string is baked into the
//! `pipeline-manager` binary.
//!
//! The two `extern crate … as _;` lines keep `dbsp_adapters` and
//! `feldera-datagen` rlibs alive so their linkme sections fire.  One line per
//! crate is sufficient — linkme collects all sections from each linked rlib
//! automatically, so no per-connector force-link is needed.

extern crate dbsp_adapters as _;
extern crate feldera_datagen as _;

use feldera_adapterlib_meta::{ConnectorManifestEntry, metadata_registry};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Duplicate-name detection: two connectors sharing a name is a programming error.
    let mut seen: HashMap<&'static str, ()> = HashMap::new();
    let mut has_dupe = false;
    for d in metadata_registry() {
        if seen.insert(d.name, ()).is_some() {
            eprintln!(
                "cargo:warning=duplicate built-in connector name '{}' — \
                 a #[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)] static is registered twice",
                d.name
            );
            has_dupe = true;
        }
    }
    if has_dupe {
        panic!("duplicate built-in connector name(s); see warnings above");
    }

    let entries: Vec<ConnectorManifestEntry> =
        metadata_registry().map(ConnectorManifestEntry::from_descriptor).collect();
    let json = serde_json::to_string(&entries).expect("serializing manifest entries");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    let out_path = out_dir.join("platform_manifest.json");
    fs::write(&out_path, &json).expect("writing platform_manifest.json");

    // Re-run when this build script itself changes.
    println!("cargo:rerun-if-changed=build.rs");
}
