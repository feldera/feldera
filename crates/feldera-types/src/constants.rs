/// Connector plugin API version.
///
/// Embedded in every [`feldera_adapterlib::connector::ConnectorManifestEntry`]
/// by the describer binary and validated by the pipeline-manager when it reads
/// a manifest. A mismatch means a connector crate was compiled against an
/// incompatible version of `feldera-adapterlib` and must be recompiled.
///
/// Increment this constant (with a `CHANGELOG.md` entry) on any breaking
/// change to the connector plugin ABI: trait method signature changes, removed
/// or renamed types that connector authors are expected to implement, etc.
pub const ADAPTERLIB_API_VERSION: u32 = 0;

/// Name of the checkpoint list file.
///
/// File will be stored inside the runtime storage directory with this
/// name.
pub const CHECKPOINT_FILE_NAME: &str = "checkpoints.feldera";

/// State of the pipeline.
pub const STATE_FILE: &str = "state.json";

/// Desired status (running, paused, etc.) of the pipeline.
pub const STATUS_FILE: &str = "status.json";

pub const STEPS_FILE: &str = "steps.bin";

pub const CHECKPOINT_DEPENDENCIES: &str = "dependencies.json";

pub const ADHOC_TEMP_DIR: &str = "adhoc-tmp";

/// A slice of all file-extension the system can create.
pub const DBSP_FILE_EXTENSION: &[&str] = &["mut", "feldera"];

/// Extension for batch files used by the engine.
pub const CREATE_FILE_EXTENSION: &str = ".feldera";

/// File that marks the activation of a pipeline.
pub const ACTIVATION_MARKER_FILE: &str = "activated.feldera";
