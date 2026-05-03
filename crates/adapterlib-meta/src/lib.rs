//! # `feldera-adapterlib-meta`
//!
//! Lightweight connector metadata registry for Feldera connectors.
//!
//! This crate provides only data-only descriptor types and a
//! [`linkme::distributed_slice`] registry.  It has **no** build-fn pointers and
//! **no** heavy runtime dependencies, so it can be compiled in a metadata-only
//! build of `dbsp_adapters` (i.e., `--no-default-features`).
//!
//! Connector implementors register a [`ConnectorDescriptor`] by declaring a
//! `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]` static in their crate.
//! The registry is then queryable at runtime via [`metadata_registry`] and
//! [`descriptor_by_name`].

use feldera_types::config::FtModel;
pub use feldera_types::config::FormatConfig;
use serde_json::Value as JsonValue;

// ─── Direction ───────────────────────────────────────────────────────────────

/// Which direction(s) this connector supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    Input,
    Output,
    InputOutput,
}

impl Direction {
    /// Returns `true` when this direction allows use as an input connector.
    #[inline]
    pub fn allows_input(self) -> bool {
        matches!(self, Direction::Input | Direction::InputOutput)
    }

    /// Returns `true` when this direction allows use as an output connector.
    #[inline]
    pub fn allows_output(self) -> bool {
        matches!(self, Direction::Output | Direction::InputOutput)
    }
}

// ─── ConnectorKind ───────────────────────────────────────────────────────────

/// High-level category of the connector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectorKind {
    /// Regular transport + format connector.
    Regular,
    /// Integrated connector (transport and format are combined, e.g. Delta Lake, Iceberg).
    Integrated,
    /// Transient connector: created on pipeline start, does not survive restarts.
    Transient,
}

// ─── ConnectorFlags ──────────────────────────────────────────────────────────

bitflags::bitflags! {
    /// Boolean capability flags for a connector.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub struct ConnectorFlags: u32 {
        /// The connector is reached directly via HTTP (e.g. `http_input`/`http_output`).
        const HTTP_DIRECT = 0x01;
        /// The controller automatically re-creates this connector after a pipeline restart.
        const AUTO_RECREATED_ON_RESTART = 0x02;
    }
}

impl ConnectorFlags {
    /// Empty flags (no capabilities set).
    pub const EMPTY: ConnectorFlags = ConnectorFlags::empty();
}

// ─── ConnectorDescriptor ─────────────────────────────────────────────────────

/// Data-only metadata descriptor for a Feldera connector.
///
/// Unlike a build-fn-carrying descriptor, this type is purely data and lives
/// in `feldera-adapterlib-meta` so a metadata-only binary (the describer) can
/// enumerate all registered connectors without pulling in any heavy runtime
/// deps.
///
/// Register your connector by declaring a static. Use `env!("CARGO_CRATE_NAME")`
/// for `crate_name` so it auto-resolves to your crate's Rust identifier:
///
/// ```rust,ignore
/// #[linkme::distributed_slice(feldera_adapterlib_meta::CONNECTOR_METADATA_REGISTRY)]
/// static MY_CONNECTOR_META: feldera_adapterlib_meta::ConnectorDescriptor =
///     feldera_adapterlib_meta::ConnectorDescriptor {
///         name: "my_connector",
///         crate_name: env!("CARGO_CRATE_NAME"),
///         direction: feldera_adapterlib_meta::Direction::Input,
///         kind: feldera_adapterlib_meta::ConnectorKind::Regular,
///         fault_tolerance: None,
///         config_schema: || serde_json::Value::Object(Default::default()),
///         default_format: None,
///         flags: feldera_adapterlib_meta::ConnectorFlags::EMPTY,
///     };
/// ```
///
/// # Builder symbol convention
///
/// pipeline-manager's per-pipeline `connector_dispatch.rs` codegen calls
/// `<crate_name>::build_<slot>_<name>(…)` for each referenced connector, where
/// `<slot>` is one of `input`, `output`, `integrated_input`,
/// `integrated_output` and `<name>` is the connector's `name` field. So a
/// `Direction::Input` + `ConnectorKind::Regular` connector named
/// `"hello_lines"` in crate `connector_example` is reached via
/// `connector_example::build_input_hello_lines`. Connector authors must name
/// their builder fns to match.
pub struct ConnectorDescriptor {
    /// Unique string name used to look up this connector (e.g. `"kafka_input"`).
    pub name: &'static str,
    /// Rust crate identifier of the crate that exports this connector's
    /// builder fns.
    ///
    /// Conventionally set to `env!("CARGO_CRATE_NAME")` so it auto-resolves
    /// at compile time. pipeline-manager's codegen uses this to write the
    /// fully-qualified builder symbol path (`<crate_name>::build_<slot>_<name>`).
    /// Built-in connectors carry `"dbsp_adapters"`; built-ins registered from
    /// a separate crate but re-exported through `dbsp_adapters` (currently
    /// only `feldera-datagen`) hardcode `"dbsp_adapters"` rather than using
    /// `env!`.
    pub crate_name: &'static str,
    /// The direction(s) this connector supports.
    pub direction: Direction,
    /// High-level category of the connector.
    pub kind: ConnectorKind,
    /// Fault-tolerance level advertised by this connector.
    pub fault_tolerance: Option<FtModel>,
    /// Returns the JSON schema for this connector's configuration.
    pub config_schema: fn() -> JsonValue,
    /// Returns the default [`FormatConfig`] for this connector, if any.
    pub default_format: Option<fn() -> FormatConfig>,
    /// Capability flags.
    pub flags: ConnectorFlags,
}

// SAFETY: `ConnectorDescriptor` contains only `'static` references and
// function pointers, all of which are inherently `Send + Sync`.
unsafe impl Send for ConnectorDescriptor {}
unsafe impl Sync for ConnectorDescriptor {}

// ─── ConnectorManifestEntry ──────────────────────────────────────────────────

/// Serializable snapshot of a [`ConnectorDescriptor`], produced by the describer
/// binary and consumed by pipeline-manager for direction validation and
/// per-pipeline `connector_dispatch.rs` codegen.
///
/// This type lives in `feldera-adapterlib-meta` so the describer binary can
/// enumerate connectors without depending on the heavy `feldera-adapterlib` tree.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ConnectorManifestEntry {
    /// `feldera-adapterlib` plugin API version at which this connector was compiled.
    pub adapterlib_api_version: u32,
    /// Unique connector name matching `TransportConfig::name()`.
    pub name: String,
    /// Direction(s) supported.
    pub direction: Direction,
    /// Implementation style.
    pub kind: ConnectorKind,
    /// Best-case fault tolerance; `None` = no FT.
    pub fault_tolerance: Option<FtModel>,
    /// Raw capability-flag bits (see [`ConnectorFlags`]).
    pub flags_bits: u32,
    /// Whether a regular-input factory is available.
    pub has_build_input: bool,
    /// Whether a regular-output factory is available.
    pub has_build_output: bool,
    /// Whether an integrated-input factory is available.
    pub has_build_integrated_input: bool,
    /// Whether an integrated-output factory is available.
    pub has_build_integrated_output: bool,
    /// Rust crate name supplying the builder functions.
    ///
    /// Always populated from the descriptor's `crate_name` field; this is
    /// the value pipeline-manager's codegen uses for the qualified call
    /// path (`<builder_crate>::build_<slot>_<name>`). Built-in entries
    /// (`crate_name == "dbsp_adapters"`) are invoked via the same codegen
    /// path as external plugins.
    pub builder_crate: String,
}

/// Default builder fn name for a connector: `build_<connector_name>`.
///
/// The manifest's `has_build_*` flags determine which call signature the
/// codegen uses when calling this fn; the fn name itself encodes only the
/// connector's unique transport name.
///
/// The only constraint on connector authors: a connector with
/// `Direction::InputOutput` (both input and output) cannot use a single
/// name — it must register two separate named connectors, since the
/// input and output builder fns would otherwise share a name but require
/// different return types.
pub fn default_builder_fn(connector_name: &str) -> String {
    format!("build_{connector_name}")
}

impl ConnectorManifestEntry {
    /// Convert a [`ConnectorDescriptor`] to a manifest entry.
    ///
    /// Build capabilities are derived from `direction` and `kind`:
    /// - Regular / Transient + allows input  → `has_build_input`.
    /// - Regular / Transient + allows output → `has_build_output`.
    /// - Integrated + allows input           → `has_build_integrated_input`.
    /// - Integrated + allows output          → `has_build_integrated_output`.
    ///
    /// `builder_crate` is taken directly from `descriptor.crate_name`.
    pub fn from_descriptor(d: &ConnectorDescriptor) -> Self {
        let is_integrated = d.kind == ConnectorKind::Integrated;
        Self {
            adapterlib_api_version: feldera_types::constants::ADAPTERLIB_API_VERSION,
            name: d.name.to_owned(),
            direction: d.direction,
            kind: d.kind,
            fault_tolerance: d.fault_tolerance,
            flags_bits: d.flags.bits(),
            has_build_input: d.direction.allows_input() && !is_integrated,
            has_build_output: d.direction.allows_output() && !is_integrated,
            has_build_integrated_input: d.direction.allows_input() && is_integrated,
            has_build_integrated_output: d.direction.allows_output() && is_integrated,
            builder_crate: d.crate_name.to_owned(),
        }
    }
}

// ─── Registry ────────────────────────────────────────────────────────────────

/// Global registry of all [`ConnectorDescriptor`]s contributed via
/// `#[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]`.
#[linkme::distributed_slice]
pub static CONNECTOR_METADATA_REGISTRY: [ConnectorDescriptor];

/// Returns an iterator over every [`ConnectorDescriptor`] in the registry.
pub fn metadata_registry() -> impl Iterator<Item = &'static ConnectorDescriptor> {
    CONNECTOR_METADATA_REGISTRY.iter()
}

/// Looks up a [`ConnectorDescriptor`] by its `name` field.
///
/// Returns `None` if no connector with that name is registered.
pub fn descriptor_by_name(name: &str) -> Option<&'static ConnectorDescriptor> {
    CONNECTOR_METADATA_REGISTRY.iter().find(|d| d.name == name)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_schema() -> JsonValue {
        JsonValue::Object(Default::default())
    }

    #[linkme::distributed_slice(CONNECTOR_METADATA_REGISTRY)]
    static TEST_DESCRIPTOR: ConnectorDescriptor = ConnectorDescriptor {
        name: "test_meta_connector",
        crate_name: env!("CARGO_CRATE_NAME"),
        direction: Direction::InputOutput,
        kind: ConnectorKind::Regular,
        fault_tolerance: None,
        config_schema: dummy_schema,
        default_format: None,
        flags: ConnectorFlags::EMPTY,
    };

    #[test]
    fn registry_is_accessible() {
        let names: Vec<_> = metadata_registry().map(|d| d.name).collect();
        assert!(
            names.contains(&"test_meta_connector"),
            "test_meta_connector not found in registry; found: {names:?}"
        );
    }

    #[test]
    fn descriptor_by_name_found() {
        let d = descriptor_by_name("test_meta_connector")
            .expect("test_meta_connector not found via descriptor_by_name");
        assert_eq!(d.name, "test_meta_connector");
        assert_eq!(d.direction, Direction::InputOutput);
        assert_eq!(d.kind, ConnectorKind::Regular);
        assert!(d.fault_tolerance.is_none());
        assert!(d.default_format.is_none());
        assert_eq!(d.flags, ConnectorFlags::EMPTY);
    }

    #[test]
    fn descriptor_by_name_missing() {
        assert!(descriptor_by_name("nonexistent_connector").is_none());
    }

    #[test]
    fn direction_allows_input() {
        assert!(Direction::Input.allows_input());
        assert!(!Direction::Input.allows_output());
        assert!(!Direction::Output.allows_input());
        assert!(Direction::Output.allows_output());
        assert!(Direction::InputOutput.allows_input());
        assert!(Direction::InputOutput.allows_output());
    }

    #[test]
    fn connector_flags_http_direct() {
        let flags = ConnectorFlags::HTTP_DIRECT;
        assert!(flags.contains(ConnectorFlags::HTTP_DIRECT));
        assert!(!flags.contains(ConnectorFlags::AUTO_RECREATED_ON_RESTART));
    }
}
