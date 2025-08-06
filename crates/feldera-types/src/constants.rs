/// Name of the checkpoint list file.
///
/// File will be stored inside the runtime storage directory with this
/// name.
pub const CHECKPOINT_FILE_NAME: &str = "checkpoints.feldera";

pub const STATE_FILE: &str = "state.json";

pub const STEPS_FILE: &str = "steps.bin";

pub const CHECKPOINT_DEPENDENCIES: &str = "dependencies.json";

pub const ADHOC_TEMP_DIR: &str = "adhoc-tmp";

/// A slice of all file-extension the system can create.
pub const DBSP_FILE_EXTENSION: &[&str] = &["mut", "feldera"];

/// Extension for batch files used by the engine.
pub const CREATE_FILE_EXTENSION: &str = ".feldera";

/// File that marks the activation of a pipeline.
pub const ACTIVATION_MARKER_FILE: &str = "activated.feldera";
