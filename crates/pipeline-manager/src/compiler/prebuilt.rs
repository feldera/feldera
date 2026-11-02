//! Canonical names for generated crates in prebuilt binary cache keys.
//!
//! SQL literals can contain the pipeline namespace. Normalize Rust identifiers
//! and Cargo's crate references structurally; never replace text in literals.

use super::util::{UtilError, crate_name_pipeline_base};
use crate::db::types::pipeline::PipelineId;
use proc_macro2::{Group, Ident, TokenStream, TokenTree};
use uuid::Uuid;

pub(super) struct GeneratedCrateNames {
    prefix: String,
    canonical_prefix: String,
}

impl GeneratedCrateNames {
    pub(super) fn new(pipeline_id: PipelineId) -> Self {
        Self {
            prefix: format!("feldera_pipe_{}_", crate_name_pipeline_base(pipeline_id)),
            canonical_prefix: format!(
                "feldera_pipe_{}_",
                crate_name_pipeline_base(PipelineId(Uuid::nil()))
            ),
        }
    }

    fn name(&self, name: &str) -> String {
        match name.strip_prefix(&self.prefix) {
            Some(suffix) => format!("{}{suffix}", self.canonical_prefix),
            None => name.to_owned(),
        }
    }

    fn path(&self, path: &str) -> String {
        path.split('/')
            .map(|part| self.name(part))
            .collect::<Vec<_>>()
            .join("/")
    }

    fn tokens(&self, stream: TokenStream) -> TokenStream {
        stream
            .into_iter()
            .map(|token| match token {
                TokenTree::Ident(ident) if ident.to_string().starts_with(&self.prefix) => {
                    TokenTree::Ident(Ident::new(&self.name(&ident.to_string()), ident.span()))
                }
                TokenTree::Group(group) => {
                    TokenTree::Group(Group::new(group.delimiter(), self.tokens(group.stream())))
                }
                literal_or_punctuation => literal_or_punctuation,
            })
            .collect()
    }

    pub(super) fn normalize_rust(&self, source: &str) -> Result<String, UtilError> {
        let tokens = source.parse::<TokenStream>().map_err(|error| {
            UtilError::InvalidContent(format!(
                "tokenizing generated Rust for prebuilt cache: {error}"
            ))
        })?;
        Ok(self.tokens(tokens).to_string())
    }

    fn manifest(&self, source: &str) -> Result<String, UtilError> {
        let mut manifest: toml::Table = toml::from_str(source).map_err(|error| {
            UtilError::InvalidContent(format!(
                "parsing generated Cargo manifest for prebuilt cache: {error}"
            ))
        })?;
        for table in ["package", "lib"] {
            if let Some(name) = manifest
                .get(table)
                .and_then(|value| value.get("name"))
                .and_then(toml::Value::as_str)
                .map(str::to_owned)
            {
                manifest[table]["name"] = self.name(&name).into();
            }
        }
        if let Some(bins) = manifest.get_mut("bin").and_then(toml::Value::as_array_mut) {
            for bin in bins {
                if let Some(name) = bin.get("name").and_then(toml::Value::as_str) {
                    bin["name"] = self.name(name).into();
                }
            }
        }
        self.dependencies(&mut manifest);
        if let Some(workspace) = manifest
            .get_mut("workspace")
            .and_then(toml::Value::as_table_mut)
        {
            for key in ["members", "default-members", "exclude"] {
                if let Some(paths) = workspace.get_mut(key).and_then(toml::Value::as_array_mut) {
                    for path in paths {
                        if let Some(value) = path.as_str() {
                            *path = self.path(value).into();
                        }
                    }
                }
            }
            self.dependencies(workspace);
        }
        if let Some(targets) = manifest
            .get_mut("target")
            .and_then(toml::Value::as_table_mut)
        {
            for target in targets
                .iter_mut()
                .filter_map(|(_, value)| value.as_table_mut())
            {
                self.dependencies(target);
            }
        }
        toml::to_string(&manifest).map_err(|error| {
            UtilError::InvalidContent(format!(
                "serializing generated Cargo manifest for prebuilt cache: {error}"
            ))
        })
    }

    fn dependencies(&self, manifest: &mut toml::Table) {
        for kind in ["dependencies", "dev-dependencies", "build-dependencies"] {
            if let Some(dependencies) = manifest.get_mut(kind).and_then(toml::Value::as_table_mut) {
                *dependencies = std::mem::take(dependencies)
                    .into_iter()
                    .map(|(name, mut dependency)| {
                        if let Some(path) = dependency.get("path").and_then(toml::Value::as_str) {
                            dependency["path"] = self.path(path).into();
                        }
                        (self.name(&name), dependency)
                    })
                    .collect();
            }
        }
    }

    pub(super) fn normalize_file(
        &self,
        path: &str,
        content: &str,
    ) -> Result<(String, String), UtilError> {
        let content = if path.ends_with(".rs") {
            self.normalize_rust(content)?
        } else if path.rsplit('/').next() == Some("Cargo.toml") {
            self.manifest(content)?
        } else {
            content.to_owned()
        };
        Ok((self.path(path), content))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prebuilt_binary_normalization_preserves_literals_and_other_identifiers() {
        let id = PipelineId(Uuid::now_v7());
        let names = GeneratedCrateNames::new(id);
        let generated = format!("{}globals", names.prefix);
        let source = format!(
            "use {generated}::*; fn f() {{ let r#type = (\"{generated}\", r###\"{generated}\"###, b\"{generated}\"); let user_{generated} = 1; }}"
        );
        let normalized = names.normalize_rust(&source).unwrap();
        assert!(normalized.contains(&format!("{}globals", names.canonical_prefix)));
        assert!(normalized.contains(&format!("\"{generated}\"")));
        assert!(normalized.contains(&format!("r###\"{generated}\"###")));
        assert!(normalized.contains(&format!("b\"{generated}\"")));
        assert!(normalized.contains(&format!("user_{generated}")));
        assert!(normalized.contains("r#type"));
        assert_ne!(
            normalized,
            names
                .normalize_rust(
                    &source.replace(&generated, &format!("{}globals", names.canonical_prefix))
                )
                .unwrap()
        );
        assert!(names.normalize_rust("fn broken( {").is_err());
    }

    #[test]
    fn prebuilt_binary_normalizes_manifest_crate_references_only() {
        let id = PipelineId(Uuid::now_v7());
        let names = GeneratedCrateNames::new(id);
        let manifest = |prefix: &str| {
            format!(
                r#"
            [workspace]
            members = ["crates/{prefix}main"]
            [workspace.dependencies]
            {prefix}globals = {{ path = "crates/{prefix}globals" }}
            [package]
            name = "{prefix}main"
            description = "{}globals"
            [[bin]]
            name = "{prefix}main"
            path = "src/main.rs"
            [target.'cfg(unix)'.dependencies]
            {prefix}globals = {{ path = "../{prefix}globals" }}
            serde = "1.0"
        "#,
                names.prefix
            )
        };
        assert_eq!(
            names.manifest(&manifest(&names.prefix)).unwrap(),
            names.manifest(&manifest(&names.canonical_prefix)).unwrap()
        );
        assert!(
            names
                .manifest(&manifest(&names.prefix))
                .unwrap()
                .contains(&format!("description = \"{}globals\"", names.prefix))
        );
        assert!(names.manifest("[package").is_err());
    }
}
