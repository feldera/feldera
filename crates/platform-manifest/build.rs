//! Build-time generator for the built-in connector manifest.
//!
//! Walks [`feldera_adapterlib::connector::registered_connectors`] against
//! the built-in connector tree (`dbsp_adapters` + `feldera-datagen`,
//! pulled in via `[build-dependencies]`) and emits
//! `$OUT_DIR/platform_manifest.json`.  The lib's `include_str!` reads this
//! file at compile time so the resulting string is baked into the
//! `pipeline-manager` binary.
//!
//! The two `extern crate ... as _;` lines below force the linker to keep
//! the rlibs reachable: rustc drops "unused" deps and `inventory::submit!`
//! only fires for crates whose code is actually linked in.  This is the
//! same trick the per-pipeline globals crate uses (see
//! `rust_compiler::generate_force_link_rs`).
//!
//! `dbsp_adapters` itself force-links `feldera-datagen` from its own
//! `lib.rs`, so the explicit datagen line below is technically redundant
//! through the adapters chain — but kept defensive in case adapters'
//! force-link is ever removed during a future cleanup PR.

extern crate dbsp_adapters as _;
extern crate feldera_datagen as _;

use feldera_adapterlib::connector::{registered_connectors, ConnectorManifestEntry};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

fn main() {
    // Duplicate-name detection — the same defensive check the runtime
    // describer used to do.  Two connectors registering the same name
    // is a programming error in `dbsp_adapters`; fail the build rather
    // than ship an ambiguous manifest.
    let mut seen: HashMap<&'static str, &'static str> = HashMap::new();
    let mut has_dupe = false;
    for d in registered_connectors() {
        if let Some(prior) = seen.insert(d.name, "") {
            let _ = prior;
            eprintln!(
                "cargo:warning=duplicate built-in connector name '{}' — \
                 a `register_connector!` macro is registered twice",
                d.name
            );
            has_dupe = true;
        }
    }
    if has_dupe {
        panic!("duplicate built-in connector name(s); see warnings above");
    }

    let entries: Vec<ConnectorManifestEntry> = registered_connectors()
        .map(ConnectorManifestEntry::from_descriptor)
        .collect();
    let json = serde_json::to_string(&entries).expect("serializing manifest entries");

    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR must be set by Cargo"));
    let out_path = out_dir.join("platform_manifest.json");
    fs::write(&out_path, &json).expect("writing platform_manifest.json");

    // Re-run when this build script itself changes.  Cargo already
    // invalidates on build-dep changes (which covers `dbsp_adapters` and
    // friends transitively), so an explicit `cargo:rerun-if-changed` per
    // connector source file is unnecessary.
    println!("cargo:rerun-if-changed=build.rs");
}
