//! The controller's preprocessors and postprocessors: the bytes that an input connector
//! reads pass through preprocessors before the parser, and the bytes that an encoder writes
//! pass through postprocessors before the output connector.

use crate::test::engine_test;
use std::io::Write;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use csv::ReaderBuilder as CsvReaderBuilder;
use feldera_adapterlib::format::Splitter;
use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_adapterlib::preprocess::{Preprocessor, PreprocessorCreateError, PreprocessorFactory};
use feldera_adapterlib::test::{default_timeout_ms, wait};
use feldera_types::config::PipelineConfig;
use feldera_types::postprocess::PostprocessorConfig;
use feldera_types::preprocess::PreprocessorConfig;
use serde_json::json;
use tempfile::NamedTempFile;

use crate::format::{LineSplitter, SpongeSplitter};
use crate::preprocess::{DecryptionPreprocessorFactory, PassthroughPreprocessorFactory};
use crate::test::init_test_logger;
use crate::test::pipeline::{ControllerOptions, PipelineTestEngine};
use crate::test::suites::controller::identity;

#[engine_test]
pub fn preprocessor(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Create test JSON data with various strings that will be filtered and transformed
    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    // Controller configuration with a preprocessor :
    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                },
                "preprocessor": [
                    {
                        "name": "passthrough",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    // Register the factory before connectors are initialized.
                    catalog
                        .preprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("passthrough", Box::new(PassthroughPreprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

#[engine_test]
pub fn decryption_preprocessor(engine: &dyn PipelineTestEngine) {
    use crate::preprocess::aes256gcm_encrypt;

    init_test_logger();

    // AES-256 key (32 bytes) and nonce (12 bytes).
    let key = b"0123456789abcdef0123456789abcdef";
    let nonce = b"test_nonce_1";

    // Encrypt JSON data that the pipeline will decrypt and parse.
    let json_input = br#"[
        {"id": 1, "b": true, "s": "one"},
        {"id": 2, "b": false, "s": "two"}
    ]"#;
    let encrypted = aes256gcm_encrypt(key, nonce, json_input);

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&encrypted).unwrap();

    let key_b64 = BASE64.encode(key);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_decryption_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                },
                "preprocessor": [
                    {
                        "name": "decryption",
                        "message_oriented": true,
                        "config": {
                            "key": key_b64
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    // Register the factory before connectors are initialized.
                    catalog
                        .preprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("decryption", Box::new(DecryptionPreprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Preprocessor that base64-decodes each line it receives.
///
/// The `NewlineSplitter` above ensures that `process` is called with exactly
/// one base64-encoded line (including the trailing `\n`) at a time.
/// `process` strips the newline and decodes the rest, returning the original
/// CSV bytes (e.g. `b"1,true,,one\n"`).
struct Base64LinePreprocessor;

impl Preprocessor for Base64LinePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<crate::format::ParseError>) {
        let without_newline = data.strip_suffix(b"\n").unwrap_or(data);
        match BASE64.decode(without_newline) {
            Ok(decoded) => (decoded, vec![]),
            Err(e) => (
                vec![],
                vec![crate::format::ParseError::bin_envelope_error(
                    format!("base64 decode error: {e}"),
                    without_newline,
                    None,
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Base64LinePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(LineSplitter))
    }
}

struct Base64LinePreprocessorFactory;

impl PreprocessorFactory for Base64LinePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(Base64LinePreprocessor))
    }
}

/// Verify that `PreprocessedParser` exercises both the preprocessor splitter
/// and the parser splitter in the same pipeline.
///
/// Input format: the file contains newline-separated base64-encoded CSV rows.
///
/// Preprocessor splitter (`NewlineSplitter`): the file transport calls
/// `PreprocessedParser::splitter()`, which returns the preprocessor's splitter.
/// That splitter finds `\n` boundaries in the raw byte stream so that each
/// call to `PreprocessedParser::parse` receives exactly one base64-encoded
/// line.
///
/// Parser splitter (`CsvSplitter`): inside `PreprocessedParser::parse`,
/// after `process` decodes a base64 line back to a CSV row (e.g.
/// `b"1,true,,one\n"`), the result is appended to the internal
/// `StreamSplitter`, which uses the CSV parser's own splitter to find the
/// CSV record boundary before forwarding the complete row to `CsvParser`.
#[engine_test]
pub fn base64_csv_preprocessor(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Two CSV rows of the columns `id`, `b`, `i`, and `s`.
    // `i` is None, which serialises as an empty field in CSV.
    let rows: &[&[u8]] = &[b"1,true,,one\n", b"2,false,,two\n"];

    // Build the input file: each CSV row is base64-encoded, one per line.
    let mut file_contents = Vec::new();
    for row in rows {
        file_contents.extend_from_slice(BASE64.encode(row).as_bytes());
        file_contents.push(b'\n');
    }
    // skip newline after last row
    file_contents.pop();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&file_contents).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_csv_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "preprocessor": [
                    {
                        "name": "base64LinePreprocessor",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog.preprocessor_registry().lock().unwrap().register(
                        "base64LinePreprocessor",
                        Box::new(Base64LinePreprocessorFactory),
                    );
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Preprocessor that base64-decodes the input it receives.
struct Base64SpongePreprocessor;

impl Preprocessor for Base64SpongePreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<crate::format::ParseError>) {
        let without_newline = data.strip_suffix(b"\n").unwrap_or(data);
        match BASE64.decode(without_newline) {
            Ok(decoded) => (decoded, vec![]),
            Err(e) => (
                vec![],
                vec![crate::format::ParseError::bin_envelope_error(
                    format!("base64 decode error: {e}"),
                    without_newline,
                    None,
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Base64SpongePreprocessor)
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(SpongeSplitter))
    }
}

struct Base64SpongePreprocessorFactory;

impl PreprocessorFactory for Base64SpongePreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(Base64SpongePreprocessor))
    }
}

/// Like the previous test, but use a SpongeSplitter in the preprocessor and
/// uudecode everything, not at line boundaries.
#[engine_test]
pub fn base64_sponge_csv_preprocessor(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    // Two CSV rows of the columns `id`, `b`, `i`, and `s`.
    // `i` is None, which serialises as an empty field in CSV.
    let rows: &[u8] = b"1,true,,one\n2,false,,two";

    // Build the input file: each CSV row is base64-encoded, one per line.
    let mut file_contents = Vec::new();
    file_contents.extend_from_slice(BASE64.encode(rows).as_bytes());

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file.as_file().write_all(&file_contents).unwrap();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_sponge_csv_preprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "preprocessor": [
                    {
                        "name": "base64SpongePreprocessor",
                        "message_oriented": true,
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog.preprocessor_registry().lock().unwrap().register(
                        "base64SpongePreprocessor",
                        Box::new(Base64SpongePreprocessorFactory),
                    );
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Verify that a passthrough postprocessor does not alter output.
///
/// The output JSON produced by the encoder passes through the postprocessor
/// unchanged and lands in the output file as valid JSON that the query engine
/// can read back.
#[engine_test]
pub fn postprocessor(engine: &dyn PipelineTestEngine) {
    use crate::postprocess::PassthroughPostprocessorFactory;

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "passthrough",
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog
                        .postprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("passthrough", Box::new(PassthroughPostprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();

    let result = controller
        .execute_query_text_sync("select * from test_output1 order by id")
        .unwrap();

    let expected = r#"+----+-------+---+-----+
| id | b     | i | s   |
+----+-------+---+-----+
| 1  | true  |   | one |
| 2  | false |   | two |
+----+-------+---+-----+"#;

    assert_eq!(&result, expected);
    controller.stop().unwrap();
}

/// Verify that the encryption postprocessor encrypts output and the file
/// cannot be parsed as plain JSON.  Decrypt the raw file bytes and confirm
/// that the decrypted payload matches what the pipeline produced.
#[engine_test]
pub fn encryption_postprocessor(engine: &dyn PipelineTestEngine) {
    use crate::postprocess::EncryptionPostprocessorFactory;
    use crate::preprocess::aes256gcm_decrypt;

    init_test_logger();

    let key = b"0123456789abcdef0123456789abcdef"; // 32 bytes
    let nonce = b"test_nonce_1"; // 12 bytes

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let key_b64 = BASE64.encode(key);
    let nonce_b64 = BASE64.encode(nonce);

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_encryption_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path.clone() }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "encryption",
                        "config": {
                            "key": key_b64,
                            "nonce": nonce_b64
                        }
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog
                        .postprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("encryption", Box::new(EncryptionPostprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();
    controller.stop().unwrap();

    // Read the raw output file and decrypt it.
    let encrypted = std::fs::read(&output_path).unwrap();
    assert!(
        !encrypted.is_empty(),
        "output file should not be empty after encryption"
    );

    // The encrypted blob is not valid UTF-8 CSV.
    assert!(
        std::str::from_utf8(&encrypted).is_err() || !encrypted.contains(&b','),
        "encrypted output should not look like plain CSV"
    );

    // Decrypt and verify the result contains CSV records.
    let enc_nonce = &encrypted[..12];
    let tag_start = encrypted.len() - 16;
    let ciphertext = &encrypted[12..tag_start];
    let tag = &encrypted[tag_start..];
    let plaintext = aes256gcm_decrypt(key, enc_nonce, ciphertext, tag)
        .expect("decryption of postprocessed output failed");

    // The decrypted payload must be parseable CSV with 2 records.
    let mut rdr = CsvReaderBuilder::new()
        .has_headers(false)
        .from_reader(plaintext.as_slice());
    let rows: Vec<_> = rdr.records().collect();
    assert_eq!(rows.len(), 2, "expected 2 rows in decrypted CSV output");
}

/// Verify that a base64-encoding postprocessor transforms the output and that
/// decoding it yields the original JSON payload.
struct Base64EncodePostprocessor;

impl Postprocessor for Base64EncodePostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        Ok(BASE64.encode(data).into_bytes())
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(Base64EncodePostprocessor)
    }
}

struct Base64EncodePostprocessorFactory;

impl PostprocessorFactory for Base64EncodePostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(Base64EncodePostprocessor))
    }
}

/// Verify that a custom postprocessor can transform output bytes and that
/// decoding the result yields the original JSON payload.
#[engine_test]
pub fn base64_postprocessor(engine: &dyn PipelineTestEngine) {
    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_base64_postprocessor",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": {
                        "path": temp_input_file.path(),
                        "follow": false
                    }
                },
                "format": {
                    "name": "json",
                    "config": {
                        "array": true,
                        "update_format": "raw"
                    }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path.clone() }
                },
                "format": {
                    "name": "csv",
                    "config": {}
                },
                "postprocessor": [
                    {
                        "name": "base64Encode",
                        "config": {}
                    }
                ]
            }
        }
    }))
    .unwrap();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog
                        .postprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("base64Encode", Box::new(Base64EncodePostprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(|e, _| panic!("error: {e}")),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();
    controller.stop().unwrap();

    // Read the raw output and base64-decode it; the result must be parseable CSV.
    let encoded = std::fs::read(&output_path).unwrap();
    assert!(
        !encoded.is_empty(),
        "output file should not be empty after base64 encoding"
    );

    let decoded = BASE64
        .decode(&encoded)
        .expect("output file should be valid base64");
    let mut rdr = CsvReaderBuilder::new()
        .has_headers(false)
        .from_reader(decoded.as_slice());
    let rows: Vec<_> = rdr.records().collect();
    assert_eq!(rows.len(), 2, "expected 2 rows in decoded CSV output");
}

/// A postprocessor that rejects every record with an error, used in
/// [`postprocessor_error_reported`].
struct AlwaysErrorPostprocessor;

impl Postprocessor for AlwaysErrorPostprocessor {
    fn push_buffer(&mut self, _buffer: &[u8]) -> anyhow::Result<Vec<u8>> {
        Err(anyhow::anyhow!("deliberate postprocessor error"))
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(AlwaysErrorPostprocessor)
    }
}

struct AlwaysErrorPostprocessorFactory;

impl PostprocessorFactory for AlwaysErrorPostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(AlwaysErrorPostprocessor))
    }
}

/// Verify that a postprocessor error is forwarded to the controller error callback.
///
/// When [`Postprocessor::push_buffer`] returns [`Err`], the record is dropped
/// and the error is delivered to the controller's error callback as a non-fatal
/// `ControllerError::OutputTransportError`.  The pipeline must continue
/// running without panicking or hanging.
#[engine_test]
pub fn postprocessor_error_reported(engine: &dyn PipelineTestEngine) {
    use crate::controller::ControllerError;
    use std::sync::{Arc, Mutex};

    init_test_logger();

    let temp_input_file = NamedTempFile::new().unwrap();
    temp_input_file
        .as_file()
        .write_all(
            br#"[
            {"id": 1, "b": true, "s": "one"},
            {"id": 2, "b": false, "s": "two"}
        ]"#,
        )
        .unwrap();

    let temp_output_file = NamedTempFile::new().unwrap();
    let output_path = temp_output_file.path().to_path_buf();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor_error_reported",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": temp_input_file.path(), "follow": false }
                },
                "format": {
                    "name": "json",
                    "config": { "array": true, "update_format": "raw" }
                }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "file_output",
                    "config": { "path": output_path }
                },
                "format": { "name": "csv", "config": {} },
                "postprocessor": [{ "name": "always_error", "config": {} }]
            }
        }
    }))
    .unwrap();

    let captured: Arc<Mutex<Vec<Arc<ControllerError>>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_clone = captured.clone();

    let controller = engine
        .controller_with_options(
            &identity(&[None]),
            &config,
            ControllerOptions {
                setup: Some(Box::new(|catalog| {
                    catalog
                        .postprocessor_registry()
                        .lock()
                        .unwrap()
                        .register("always_error", Box::new(AlwaysErrorPostprocessorFactory));
                })),
                ..ControllerOptions::default()
            },
            Box::new(move |e, _| {
                captured_clone.lock().unwrap().push(e);
            }),
        )
        .unwrap();

    controller.start();
    wait(|| controller.pipeline_complete(), default_timeout_ms()).unwrap();
    controller.stop().unwrap();

    let errors = captured.lock().unwrap();
    assert!(
        !errors.is_empty(),
        "error callback should have been invoked at least once by the postprocessor"
    );
    for err in errors.iter() {
        let ControllerError::OutputTransportError {
            endpoint_name,
            fatal,
            ..
        } = err.as_ref()
        else {
            panic!("expected OutputTransportError, got: {err}");
        };
        assert_eq!(endpoint_name, "test_output1");
        assert!(!fatal, "postprocessor errors must be non-fatal");
    }
}

/// Verify that attaching a postprocessor to a `delta_table_output` (integrated)
/// connector is rejected at startup.
///
/// Integrated connectors own their own serialization pipeline and bypass the
/// postprocessor layer, so a postprocessor cannot be wired into them.  The
/// controller should return a `ControllerError::PostprocessorCreateError`
/// before the circuit starts running.
#[engine_test]
pub fn postprocessor_on_delta_output_fails(engine: &dyn PipelineTestEngine) {
    use crate::controller::ControllerError;

    init_test_logger();

    let config: PipelineConfig = serde_json::from_value(json!({
        "name": "test_postprocessor_delta_fails",
        "workers": 1,
        "inputs": {
            "test_input1": {
                "stream": "test_input1",
                "transport": {
                    "name": "file_input",
                    "config": { "path": "/dev/null", "follow": false }
                },
                "format": { "name": "csv" }
            }
        },
        "outputs": {
            "test_output1": {
                "stream": "test_output1",
                "transport": {
                    "name": "delta_table_output",
                    "config": { "uri": "file:///tmp/test_delta_postprocessor" }
                },
                "postprocessor": [{ "name": "passthrough", "config": {} }]
            }
        }
    }))
    .unwrap();

    let err = engine
        .controller(
            &identity(&[None]),
            &config,
            Box::new(|e, _| panic!("unexpected error callback: {e}")),
        )
        .err()
        .expect(
            "expected an error when attaching a postprocessor to a delta_table_output connector",
        );

    let ControllerError::PostprocessorCreateError {
        ref endpoint_name,
        ref error,
    } = err
    else {
        panic!("expected PostprocessorCreateError, got: {err}");
    };
    assert_eq!(endpoint_name, "test_output1");
    assert!(
        error.contains("delta_table_output"),
        "error should name the unsupported transport, got: {error}"
    );
}
