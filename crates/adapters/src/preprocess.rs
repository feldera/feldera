//! Preprocessor implementations.

use crate::format::SpongeSplitter;
use std::borrow::Cow;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use feldera_adapterlib::preprocess::{Preprocessor, PreprocessorCreateError, PreprocessorFactory};
use feldera_types::preprocess::PreprocessorConfig;
use openssl::symm::{Cipher, decrypt_aead};
use serde::Deserialize;

use crate::format::{ParseError, Splitter};

/// A preprocessor that performs no transformation.
///
/// This is useful as a default preprocessor for testing and
/// as an example for showing the APIs that need to be implemented by users.
pub struct PassthroughPreprocessor;

impl PassthroughPreprocessor {
    fn new() -> Self {
        Self {}
    }
}

impl Preprocessor for PassthroughPreprocessor {
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        (data.to_vec(), vec![])
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Self::new())
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        None
    }
}

// A factory which knows how to manufacture a PassthroughPreprocessor
pub struct PassthroughPreprocessorFactory;

impl PreprocessorFactory for PassthroughPreprocessorFactory {
    fn create(
        &self,
        _config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        Ok(Box::new(PassthroughPreprocessor::new()))
    }
}

/// Configuration for [DecryptionPreprocessor].
#[derive(Debug, Deserialize, Clone)]
struct DecryptionPreprocessorConfig {
    /// Base64-encoded AES-256-GCM key (must decode to exactly 32 bytes).
    key: String,
}

/// A preprocessor that decrypts each chunk of data using AES-256-GCM.
///
/// The transport delivers the entire encrypted blob in a single [Preprocessor::process]
/// call (enforced by [`SpongeSplitter`]).  The blob format is:
///
/// ```text
/// [ 12-byte nonce ][ ciphertext ][ 16-byte GCM authentication tag ]
/// ```
///
/// The decryption key is supplied via the preprocessor configuration as a
/// base64-encoded string.
pub struct DecryptionPreprocessor {
    /// The raw 32-byte AES-256 key.
    key: Vec<u8>,
}

impl DecryptionPreprocessor {
    /// Size of the GCM nonce in bytes.
    const NONCE_SIZE: usize = 12;
    /// Size of the GCM authentication tag in bytes.
    const TAG_SIZE: usize = 16;
    /// Minimum input length: nonce + at least one ciphertext byte + tag.
    const MIN_INPUT_LEN: usize = Self::NONCE_SIZE + Self::TAG_SIZE;

    fn new(key: Vec<u8>) -> Self {
        Self { key }
    }
}

impl Preprocessor for DecryptionPreprocessor {
    /// Decrypt `data` using AES-256-GCM.
    ///
    /// Returns the plaintext on success, or a [ParseError] if the data is too
    /// short or if decryption / authentication fails.
    fn process(&mut self, data: &[u8]) -> (Vec<u8>, Vec<ParseError>) {
        if data.len() < Self::MIN_INPUT_LEN {
            return (
                vec![],
                vec![ParseError::bin_envelope_error(
                    format!(
                        "DecryptionPreprocessor: input too short ({} bytes); \
                         expected at least {} bytes (nonce + tag)",
                        data.len(),
                        Self::MIN_INPUT_LEN
                    ),
                    data,
                    None,
                )],
            );
        }

        let nonce = &data[..Self::NONCE_SIZE];
        let tag_start = data.len() - Self::TAG_SIZE;
        let ciphertext = &data[Self::NONCE_SIZE..tag_start];
        let tag = &data[tag_start..];

        match decrypt_aead(
            Cipher::aes_256_gcm(),
            &self.key,
            Some(nonce),
            &[],
            ciphertext,
            tag,
        ) {
            Ok(plaintext) => (plaintext, vec![]),
            Err(e) => (
                vec![],
                vec![ParseError::bin_envelope_error(
                    format!("DecryptionPreprocessor: AES-256-GCM decryption failed: {e}"),
                    data,
                    Some(Cow::Borrowed(
                        "Verify that the correct key is configured and that the data \
                         was encrypted with AES-256-GCM",
                    )),
                )],
            ),
        }
    }

    fn fork(&self) -> Box<dyn Preprocessor> {
        Box::new(Self::new(self.key.clone()))
    }

    fn splitter(&self) -> Option<Box<dyn Splitter>> {
        Some(Box::new(SpongeSplitter))
    }
}

/// Factory that creates [DecryptionPreprocessor] instances from configuration.
pub struct DecryptionPreprocessorFactory;

impl PreprocessorFactory for DecryptionPreprocessorFactory {
    fn create(
        &self,
        config: &PreprocessorConfig,
    ) -> Result<Box<dyn Preprocessor>, PreprocessorCreateError> {
        let cfg: DecryptionPreprocessorConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PreprocessorCreateError::ConfigurationError(format!(
                    "invalid DecryptionPreprocessor config: {e}"
                ))
            })?;

        let key = BASE64.decode(&cfg.key).map_err(|e| {
            PreprocessorCreateError::ConfigurationError(format!(
                "DecryptionPreprocessor: 'key' is not valid base64: {e}"
            ))
        })?;

        if key.len() != 32 {
            return Err(PreprocessorCreateError::ConfigurationError(format!(
                "DecryptionPreprocessor: key must be 32 bytes (AES-256); got {} bytes",
                key.len()
            )));
        }

        Ok(Box::new(DecryptionPreprocessor::new(key)))
    }
}

#[cfg(test)]
use openssl::symm::encrypt_aead;

#[cfg(test)]
/// Encrypt `plaintext` with AES-256-GCM, returning `[nonce || ciphertext || tag]`.
pub fn aes256gcm_encrypt(key: &[u8], nonce: &[u8; 12], plaintext: &[u8]) -> Vec<u8> {
    let mut tag = vec![0u8; 16];
    let ciphertext = encrypt_aead(
        Cipher::aes_256_gcm(),
        key,
        Some(nonce),
        &[],
        plaintext,
        &mut tag,
    )
    .expect("AES-256-GCM encryption failed");

    let mut out = Vec::with_capacity(12 + ciphertext.len() + 16);
    out.extend_from_slice(nonce);
    out.extend_from_slice(&ciphertext);
    out.extend_from_slice(&tag);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_adapterlib::preprocess::PreprocessorCreateError;
    use serde_json::json;

    fn make_config(key_bytes: &[u8]) -> PreprocessorConfig {
        PreprocessorConfig {
            name: "decryption".to_string(),
            message_oriented: true,
            config: json!({ "key": BASE64.encode(key_bytes) }),
        }
    }

    #[test]
    fn test_decryption_roundtrip() {
        let key = b"0123456789abcdef0123456789abcdef"; // 32 bytes
        let nonce = b"test_nonce_1";
        let plaintext = b"hello, decrypted world!";

        let encrypted = aes256gcm_encrypt(key, nonce, plaintext);

        let factory = DecryptionPreprocessorFactory;
        let mut preprocessor = factory.create(&make_config(key)).unwrap();

        let (decrypted, errors) = preprocessor.process(&encrypted);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decryption_fork() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"fork test data";

        let encrypted = aes256gcm_encrypt(key, nonce, plaintext);

        let factory = DecryptionPreprocessorFactory;
        let preprocessor = factory.create(&make_config(key)).unwrap();
        let mut forked = preprocessor.fork();

        let (decrypted, errors) = forked.process(&encrypted);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decryption_empty_plaintext() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"";

        let encrypted = aes256gcm_encrypt(key, nonce, plaintext);
        let factory = DecryptionPreprocessorFactory;
        let mut preprocessor = factory.create(&make_config(key)).unwrap();

        let (decrypted, errors) = preprocessor.process(&encrypted);
        assert!(errors.is_empty(), "unexpected errors: {errors:?}");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_decryption_input_too_short() {
        let key = b"0123456789abcdef0123456789abcdef";
        let factory = DecryptionPreprocessorFactory;
        let mut preprocessor = factory.create(&make_config(key)).unwrap();

        // Fewer bytes than NONCE_SIZE + TAG_SIZE = 28.
        let (out, errors) = preprocessor.process(b"short");
        assert!(out.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("too short"));
    }

    #[test]
    fn test_decryption_wrong_key() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let wrong_key = b"ffffffffffffffffffffffffffffffff";
        let plaintext = b"secret data";

        let encrypted = aes256gcm_encrypt(key, nonce, plaintext);

        let factory = DecryptionPreprocessorFactory;
        let mut preprocessor = factory.create(&make_config(wrong_key)).unwrap();

        let (out, errors) = preprocessor.process(&encrypted);
        assert!(out.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("decryption failed"));
    }

    #[test]
    fn test_decryption_tampered_ciphertext() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"tamper me";

        let mut encrypted = aes256gcm_encrypt(key, nonce, plaintext);
        // Flip a bit in the ciphertext region (after the 12-byte nonce).
        encrypted[12] ^= 0xFF;

        let factory = DecryptionPreprocessorFactory;
        let mut preprocessor = factory.create(&make_config(key)).unwrap();

        let (out, errors) = preprocessor.process(&encrypted);
        assert!(out.is_empty());
        assert_eq!(errors.len(), 1);
        assert!(errors[0].to_string().contains("decryption failed"));
    }

    #[test]
    fn test_config_key_wrong_length() {
        // 16 bytes is AES-128, not AES-256.
        let short_key = b"0123456789abcdef";
        let result = DecryptionPreprocessorFactory.create(&make_config(short_key));
        assert!(matches!(
            result,
            Err(PreprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_config_key_invalid_base64() {
        let config = PreprocessorConfig {
            name: "decryption".to_string(),
            message_oriented: true,
            config: json!({ "key": "not-valid-base64!!!" }),
        };
        let result = DecryptionPreprocessorFactory.create(&config);
        assert!(matches!(
            result,
            Err(PreprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_config_missing_key() {
        let config = PreprocessorConfig {
            name: "decryption".to_string(),
            message_oriented: true,
            config: json!({}),
        };
        let result = DecryptionPreprocessorFactory.create(&config);
        assert!(matches!(
            result,
            Err(PreprocessorCreateError::ConfigurationError(_))
        ));
    }
}
