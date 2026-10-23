//! The storage configuration that a pipeline's controller passes to the engine that runs its
//! circuit.

use std::sync::Arc;

use feldera_types::config::{StorageConfig, StorageOptions};
use uuid::Uuid;

use crate::StorageBackend;
use crate::error::StorageError;

/// Configuration for the storage of a circuit that a `Runtime` hosts.
#[derive(Clone, derive_more::Debug)]
pub struct CircuitStorageConfig {
    /// Runner configuration.
    pub config: StorageConfig,

    /// User options.
    pub options: StorageOptions,

    /// Storage backend.
    ///
    /// Presumably opened according to `config` and `options`.
    #[debug(skip)]
    pub backend: Arc<dyn StorageBackend>,

    /// The initial checkpoint to start the circuit from, or `None` to start
    /// fresh from a new circuit.
    pub init_checkpoint: Option<Uuid>,

    /// Suppress the automatic stop-the-world restore from `init_checkpoint`
    /// during `Runtime::init_circuit`, so the caller can drive a concurrent
    /// bootstrap via `DBSPHandle::start_concurrent_bootstrap` instead.
    ///
    /// `init_checkpoint` is still carried (the caller needs it as the
    /// concurrent-bootstrap base), but the circuit comes up un-restored.
    pub defer_restore: bool,
}

impl CircuitStorageConfig {
    /// Opens a backend with `config` and `options` and returns a
    /// [CircuitStorageConfig] with that backend.
    pub fn for_config(
        config: StorageConfig,
        options: StorageOptions,
    ) -> Result<Self, StorageError> {
        let backend = <dyn StorageBackend>::new(&config, &options)?;
        Ok(Self {
            config,
            options,
            backend,
            init_checkpoint: None,
            defer_restore: false,
        })
    }

    pub fn with_init_checkpoint(self, init_checkpoint: Option<Uuid>) -> Self {
        Self {
            init_checkpoint,
            ..self
        }
    }

    pub fn with_defer_restore(self, defer_restore: bool) -> Self {
        Self {
            defer_restore,
            ..self
        }
    }
}
