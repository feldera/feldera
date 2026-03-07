//! Data preprocessing layer for connectors.
//!
//! This module provides a preprocessing framework that allows data transformation
//! before it reaches the parser.
//!
//! The preprocessing layer fits between transport and parsing in the data pipeline:
//!
//! ```text
//! Transport → Preprocessor → Parser → Circuit
//! ```

use crate::format::{ParseError, Splitter};
use feldera_types::preprocess::PreprocessorConfig;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter, Result as FmtResult};
use std::sync::Arc;

// Errors that can occur during creation of a preprocessor
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreprocessorCreateError {
    /// Preprocessing configuration is invalid.
    ConfigurationError(String),
    /// Implementation for factory generating Preprocessor not found
    FactoryNotFound(String),
}

impl Display for PreprocessorCreateError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            PreprocessorCreateError::ConfigurationError(msg) => {
                write!(f, "Configuration error: {}", msg)
            }
            PreprocessorCreateError::FactoryNotFound(msg) => {
                write!(
                    f,
                    "Could not locate factory generating preprocessor: {}",
                    msg
                )
            }
        }
    }
}

impl std::error::Error for PreprocessorCreateError {}

/// Trait for preprocessing raw data before parsing.
pub trait Preprocessor: Send + Sync {
    /// Process raw input data and return transformed data.
    ///
    /// # Arguments
    /// * `data` - Raw input data bytes
    ///
    /// # Returns
    /// A `PreprocessResult` containing the transformed data or errors.
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>);

    /// Create a new preprocessor with the same configuration as `self`.
    ///
    /// Used by multithreaded transport endpoints to create multiple parallel
    /// input pipelines.
    fn fork(&self) -> Box<dyn Preprocessor>;

    /// Returns an object that can be used to break a stream of incoming data
    /// into complete records to pass to [Preprocessor::process].  If the object
    /// is None, the parser's splitter object will actually be used.
    fn splitter(&self) -> Option<Box<dyn Splitter>>;
}

/// A factory that can create a new Preprocessor object.
pub trait PreprocessorFactory: Send + Sync {
    /// Create a new preprocessor based on the supplied configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - Preprocessor-specific configuration.
    fn create(
        &self,
        config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError>;
}

/// A registry where all factories that can create Preprocessors are registered
#[derive(Default)]
pub struct PreprocessorRegistry {
    registered: BTreeMap<&'static str, Arc<dyn PreprocessorFactory>>,
}

impl PreprocessorRegistry {
    pub fn new() -> Self {
        Self {
            registered: BTreeMap::new(),
        }
    }

    /// Register a new factory under the specified name
    pub fn register(&mut self, name: &'static str, factory: Box<dyn PreprocessorFactory>) {
        self.registered.insert(name, Arc::from(factory));
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn PreprocessorFactory>> {
        self.registered.get(name).cloned()
    }
}
