//! Example postprocessor implementations.

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use feldera_adapterlib::postprocess::{
    Postprocessor, PostprocessorCreateError, PostprocessorFactory,
};
use feldera_types::postprocess::PostprocessorConfig;
use openssl::symm::{Cipher, encrypt_aead};
use serde::Deserialize;

/// A postprocessor that performs no transformation.
///
/// This is useful as a default postprocessor for testing and as an example
/// for showing the APIs that need to be implemented by users.
pub struct PassthroughPostprocessor;

impl PassthroughPostprocessor {
    fn new() -> Self {
        Self {}
    }
}

impl Postprocessor for PassthroughPostprocessor {
    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(Self::new())
    }
}

/// A factory which creates [`PassthroughPostprocessor`] instances.
pub struct PassthroughPostprocessorFactory;

impl PostprocessorFactory for PassthroughPostprocessorFactory {
    fn create(
        &self,
        _config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        Ok(Box::new(PassthroughPostprocessor::new()))
    }
}

/// Configuration for [`EncryptionPostprocessor`].
#[derive(Debug, Deserialize, Clone)]
struct EncryptionPostprocessorConfig {
    /// Base64-encoded AES-256-GCM key (must decode to exactly 32 bytes).
    key: String,
    /// Base64-encoded 12-byte nonce.
    nonce: String,
}

/// A postprocessor that encrypts each chunk of data using AES-256-GCM.
/// Note: this is not a production-quality implementation, it is here to
/// illustrate the implementation of user-defined postprocessors.
/// A strong encryption implementation would rotate the salt nonce
/// periodically.
///
/// Each call to [`Postprocessor::push_buffer`] encrypts the supplied bytes and
/// returns a blob in the format:
///
/// ```text
/// [ 12-byte nonce ][ ciphertext ][ 16-byte GCM authentication tag ]
/// ```
///
/// The same nonce is used for every call.  Callers that require per-message
/// nonces should implement a custom postprocessor.
pub struct EncryptionPostprocessor {
    key: Vec<u8>,
    nonce: Vec<u8>,
}

impl EncryptionPostprocessor {
    fn new(key: Vec<u8>, nonce: Vec<u8>) -> Self {
        Self { key, nonce }
    }
}

impl Postprocessor for EncryptionPostprocessor {
    fn push_buffer(&mut self, data: &[u8]) -> anyhow::Result<Vec<u8>> {
        let mut tag = vec![0u8; 16];
        match encrypt_aead(
            Cipher::aes_256_gcm(),
            &self.key,
            Some(&self.nonce),
            &[],
            data,
            &mut tag,
        ) {
            Ok(ciphertext) => {
                let mut out = Vec::with_capacity(self.nonce.len() + ciphertext.len() + 16);
                out.extend_from_slice(&self.nonce);
                out.extend_from_slice(&ciphertext);
                out.extend_from_slice(&tag);
                Ok(out)
            }
            Err(e) => {
                tracing::error!("EncryptionPostprocessor: AES-256-GCM encryption failed: {e}");
                Err(e.into())
            }
        }
    }

    fn fork(&self) -> Box<dyn Postprocessor> {
        Box::new(Self::new(self.key.clone(), self.nonce.clone()))
    }
}

/// Factory that creates [`EncryptionPostprocessor`] instances from configuration.
pub struct EncryptionPostprocessorFactory;

impl PostprocessorFactory for EncryptionPostprocessorFactory {
    fn create(
        &self,
        config: &PostprocessorConfig,
    ) -> Result<Box<dyn Postprocessor>, PostprocessorCreateError> {
        let cfg: EncryptionPostprocessorConfig = serde_json::from_value(config.config.clone())
            .map_err(|e| {
                PostprocessorCreateError::ConfigurationError(format!(
                    "invalid EncryptionPostprocessor config: {e}"
                ))
            })?;

        let key = BASE64.decode(&cfg.key).map_err(|e| {
            PostprocessorCreateError::ConfigurationError(format!(
                "EncryptionPostprocessor: 'key' is not valid base64: {e}"
            ))
        })?;

        if key.len() != 32 {
            return Err(PostprocessorCreateError::ConfigurationError(format!(
                "EncryptionPostprocessor: key must be 32 bytes (AES-256); got {} bytes",
                key.len()
            )));
        }

        let nonce = BASE64.decode(&cfg.nonce).map_err(|e| {
            PostprocessorCreateError::ConfigurationError(format!(
                "EncryptionPostprocessor: 'nonce' is not valid base64: {e}"
            ))
        })?;

        if nonce.len() != 12 {
            return Err(PostprocessorCreateError::ConfigurationError(format!(
                "EncryptionPostprocessor: nonce must be 12 bytes (AES-256-GCM); got {} bytes",
                nonce.len()
            )));
        }

        Ok(Box::new(EncryptionPostprocessor::new(key, nonce)))
    }
}

#[cfg(test)]
pub use openssl::symm::decrypt_aead;

#[cfg(test)]
mod tests {
    use super::*;
    use feldera_adapterlib::postprocess::PostprocessorCreateError;
    use serde_json::json;

    fn make_config(key_bytes: &[u8], nonce_bytes: &[u8]) -> PostprocessorConfig {
        PostprocessorConfig {
            name: "encryption".to_string(),
            config: json!({
                "key": BASE64.encode(key_bytes),
                "nonce": BASE64.encode(nonce_bytes),
            }),
        }
    }

    fn decrypt(key: &[u8], blob: &[u8]) -> Vec<u8> {
        let nonce = &blob[..12];
        let tag_start = blob.len() - 16;
        let ciphertext = &blob[12..tag_start];
        let tag = &blob[tag_start..];
        decrypt_aead(
            Cipher::aes_256_gcm(),
            key,
            Some(nonce),
            &[],
            ciphertext,
            tag,
        )
        .expect("decryption failed")
    }

    #[test]
    fn test_encryption_roundtrip() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"hello, encrypted world!";

        let factory = EncryptionPostprocessorFactory;
        let mut postprocessor = factory.create(&make_config(key, nonce)).unwrap();

        let encrypted = postprocessor
            .push_buffer(plaintext)
            .expect("Encryption failed");
        let decrypted = decrypt(key, &encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_fork() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"fork test data";

        let factory = EncryptionPostprocessorFactory;
        let postprocessor = factory.create(&make_config(key, nonce)).unwrap();
        let mut forked = postprocessor.fork();

        let encrypted = forked.push_buffer(plaintext).expect("Encryption failed");
        let decrypted = decrypt(key, &encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_empty_plaintext() {
        let key = b"0123456789abcdef0123456789abcdef";
        let nonce = b"test_nonce_1";
        let plaintext = b"";

        let factory = EncryptionPostprocessorFactory;
        let mut postprocessor = factory.create(&make_config(key, nonce)).unwrap();

        let encrypted = postprocessor
            .push_buffer(plaintext)
            .expect("Encryption failed");
        let decrypted = decrypt(key, &encrypted);
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_config_key_wrong_length() {
        let short_key = b"0123456789abcdef"; // 16 bytes is AES-128, not AES-256
        let nonce = b"test_nonce_1";
        let result = EncryptionPostprocessorFactory.create(&make_config(short_key, nonce));
        assert!(matches!(
            result,
            Err(PostprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_config_nonce_wrong_length() {
        let key = b"0123456789abcdef0123456789abcdef";
        let short_nonce = b"short"; // not 12 bytes
        let result = EncryptionPostprocessorFactory.create(&make_config(key, short_nonce));
        assert!(matches!(
            result,
            Err(PostprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_config_key_invalid_base64() {
        let config = PostprocessorConfig {
            name: "encryption".to_string(),
            config: json!({
                "key": "not-valid-base64!!!",
                "nonce": BASE64.encode(b"test_nonce_1"),
            }),
        };
        let result = EncryptionPostprocessorFactory.create(&config);
        assert!(matches!(
            result,
            Err(PostprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_config_missing_key() {
        let config = PostprocessorConfig {
            name: "encryption".to_string(),
            config: json!({}),
        };
        let result = EncryptionPostprocessorFactory.create(&config);
        assert!(matches!(
            result,
            Err(PostprocessorCreateError::ConfigurationError(_))
        ));
    }

    #[test]
    fn test_passthrough_buffer() {
        let mut pp = PassthroughPostprocessor::new();
        let data = b"hello, world!";
        let result = pp.push_buffer(data).expect("Passthrough failed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_passthrough_key() {
        let mut pp = PassthroughPostprocessor::new();
        let key = b"my-key";
        let val = b"my-value";
        let headers = [("h1", Some(b"v1".as_ref())), ("h2", None)];

        let (out_key, out_val, out_headers) = pp
            .push_key(Some(key), Some(val), &headers)
            .expect("Passthrough key failed");

        assert_eq!(out_key.as_deref(), Some(key.as_ref()));
        assert_eq!(out_val.as_deref(), Some(val.as_ref()));
        assert_eq!(out_headers.len(), 2);
        assert_eq!(out_headers[0], ("h1".to_string(), Some(b"v1".to_vec())));
        assert_eq!(out_headers[1], ("h2".to_string(), None));
    }

    #[test]
    fn test_passthrough_key_none_fields() {
        let mut pp = PassthroughPostprocessor::new();
        let (out_key, out_val, out_headers) = pp
            .push_key(None, None, &[])
            .expect("Passthrough key failed");
        assert!(out_key.is_none());
        assert!(out_val.is_none());
        assert!(out_headers.is_empty());
    }
}
