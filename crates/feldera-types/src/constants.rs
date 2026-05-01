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

/// Single file inside a checkpoint dir that records what the checkpoint
/// owns: the batch files it references at the storage root and the
/// per-operator state files inside its own dir. See
/// `CheckpointDependencies` for the on-disk shape.
pub const CHECKPOINT_DEPENDENCIES: &str = "dependencies.json";

/// Subdirectory under the pipeline's storage path where DataFusion writes
/// spill files for the ad-hoc query engine and every integrated connector
/// that uses DataFusion (Delta Lake, Iceberg).
///
/// One pipeline-wide directory keeps the on-disk layout discoverable in a
/// single place and lets `gc_startup` allowlist it as a single entry. If
/// you change this value, audit `gc_startup` in `dbsp::circuit::checkpointer`
/// — the GC's allowlist must keep matching the directory the runtime
/// actually creates.
pub const DATAFUSION_TEMP_DIR: &str = "datafusion-tmp";

/// A slice of all file-extension the system can create.
pub const DBSP_FILE_EXTENSION: &[&str] = &["mut", "feldera"];

/// Extension for batch files used by the engine.
pub const CREATE_FILE_EXTENSION: &str = ".feldera";

/// File that marks the activation of a pipeline.
pub const ACTIVATION_MARKER_FILE: &str = "activated.feldera";
