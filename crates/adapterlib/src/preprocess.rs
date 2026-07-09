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

use crate::ConnectorMetadata;
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
    /// The default implementation forwards to `process_with_metadata` with no
    /// metadata.
    ///
    /// # Arguments
    /// * `data` - Raw input data bytes
    ///
    /// # Returns
    /// The transformed data and any errors that occurred.
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        self.process_with_metadata(data, None)
    }

    /// Process raw input data and return transformed data using connector metadata.
    ///
    /// Users should implement exactly one of the two processing methods: `process` or
    /// `process_with_metadata`.
    ///
    /// WARNING: a preprocessor that implements neither processing method
    /// compiles, but will panic at runtime with a stack overflow.
    ///
    /// # Arguments
    /// * `data` - Raw input data bytes
    /// * `_metadata` - Connector metadata
    ///
    /// # Returns
    /// The transformed data and any errors that occurred.
    fn process_with_metadata(
        &mut self,
        data: &[u8],
        _metadata: Option<&ConnectorMetadata>,
    ) -> (Vec<u8>, Vec<ParseError>) {
        self.process(data)
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_sqllib::{SqlString, Variant};
    use serde_json::json;

    /// Preprocessor whose output depends on connector metadata: it prepends
    /// the value of the `topic` attribute to each record.  Reads the attribute
    /// through `ConnectorMetadata::get_by_name`, the same path a user-defined
    /// preprocessor must use.
    struct TopicPrefixPreprocessor;

    impl Preprocessor for TopicPrefixPreprocessor {
        fn process_with_metadata(
            &mut self,
            data: &[u8],
            metadata: Option<&ConnectorMetadata>,
        ) -> (Vec<u8>, Vec<ParseError>) {
            let Some(metadata) = metadata else {
                // No metadata attached: pass the record through unchanged.
                return (data.to_vec(), vec![]);
            };
            let mut output = Vec::new();
            if let Some(Variant::String(topic)) = metadata.get_by_name("topic") {
                output.extend_from_slice(topic.str().as_bytes());
                output.push(b':');
            }
            output.extend_from_slice(data);
            (output, vec![])
        }

        fn fork(&self) -> Box<dyn Preprocessor> {
            Box::new(TopicPrefixPreprocessor)
        }

        fn splitter(&self) -> Option<Box<dyn Splitter>> {
            None
        }
    }

    /// Preprocessor with an observable transformation, for registry tests.
    struct UppercasePreprocessor;

    impl Preprocessor for UppercasePreprocessor {
        fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
            (data.to_ascii_uppercase(), vec![])
        }

        fn fork(&self) -> Box<dyn Preprocessor> {
            Box::new(UppercasePreprocessor)
        }

        fn splitter(&self) -> Option<Box<dyn Splitter>> {
            None
        }
    }

    struct UppercasePreprocessorFactory;

    impl PreprocessorFactory for UppercasePreprocessorFactory {
        fn create(
            &self,
            _config: &PreprocessorConfig,
        ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
            Ok(Box::new(UppercasePreprocessor))
        }
    }

    fn make_config(name: &str) -> PreprocessorConfig {
        PreprocessorConfig {
            name: name.to_string(),
            message_oriented: false,
            config: json!({}),
        }
    }

    fn make_metadata() -> ConnectorMetadata {
        let mut metadata = ConnectorMetadata::new();
        metadata.insert("topic", Variant::String(SqlString::from("events")));
        metadata
    }

    #[test]
    fn test_process_metadata_transforms_using_metadata() {
        let mut preprocessor = TopicPrefixPreprocessor;

        // `make_metadata` sets the `topic` attribute to `events`.
        let metadata = make_metadata();
        let (output, errors) = preprocessor.process_with_metadata(b"payload", Some(&metadata));
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(output, b"events:payload");

        // Without metadata the record passes through unchanged.
        let (output, errors) = preprocessor.process_with_metadata(b"payload", None);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(output, b"payload");
    }

    /// A preprocessor that implements only `process_with_metadata` is still
    /// callable through `process`, whose default implementation forwards with
    /// no metadata.  This test compiles only because `process` has a default;
    /// it pins the backward-compatible surface of the trait.
    #[test]
    fn test_process_defaults_to_process_with_metadata() {
        let mut preprocessor = TopicPrefixPreprocessor;

        let (output, errors) = preprocessor.process(b"payload");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(output, b"payload");
    }

    #[test]
    fn test_registry_register_and_get() {
        let mut registry = PreprocessorRegistry::new();
        registry.register("upper", Box::new(UppercasePreprocessorFactory));

        let factory = registry.get("upper").expect("factory must be registered");
        let mut preprocessor = factory.create(&make_config("upper")).unwrap();
        let (output, errors) = preprocessor.process(b"xyz");
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(output, b"XYZ");

        assert!(registry.get("missing").is_none());
    }
}
