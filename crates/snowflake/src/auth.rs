use anyhow::{bail, Context, Result as AnyResult};
use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use feldera_types::transport::snowflake::{SnowflakeAuthenticator, SnowflakeReaderConfig};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use pkcs8::{DecodePrivateKey, EncodePrivateKey, EncodePublicKey, LineEnding};
use rsa::{RsaPrivateKey, RsaPublicKey};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    time::{SystemTime, UNIX_EPOCH},
};

const JWT_LIFETIME_SECONDS: u64 = 60;

#[derive(Debug, Serialize)]
struct JwtClaims {
    iss: String,
    sub: String,
    iat: u64,
    exp: u64,
}

pub(crate) fn jwt_token(config: &SnowflakeReaderConfig) -> AnyResult<String> {
    match config.authenticator {
        SnowflakeAuthenticator::SnowflakeJwt => {}
    }

    let pem = fs::read(&config.private_key_file).with_context(|| {
        format!(
            "error reading Snowflake private key {}",
            config.private_key_file
        )
    })?;
    let key = parse_private_key(&pem, config.private_key_file_pwd.as_deref())?;
    let public_key_fingerprint = public_key_fingerprint(&key)?;

    let account = login_account_name(&config.account).to_ascii_uppercase();
    let user = config.user.to_ascii_uppercase();
    let subject = format!("{account}.{user}");
    let now = epoch_seconds();
    let claims = JwtClaims {
        iss: format!("{subject}.SHA256:{public_key_fingerprint}"),
        sub: subject,
        iat: now,
        exp: now + JWT_LIFETIME_SECONDS,
    };
    let mut header = Header::new(Algorithm::RS256);
    header.typ = Some("JWT".to_string());
    let signing_key = key
        .to_pkcs8_pem(LineEnding::LF)
        .context("error serializing Snowflake RSA private key")?;
    let signing_key = EncodingKey::from_rsa_pem(signing_key.as_bytes())
        .context("error parsing Snowflake RSA private key")?;
    encode(&header, &claims, &signing_key).context("error signing Snowflake JWT")
}

pub(crate) fn login_account_name(account: &str) -> &str {
    if account.to_ascii_lowercase().contains(".global") {
        let account = account.split('.').next().unwrap_or(account);
        account
            .rsplit_once('-')
            .map_or(account, |(account, _)| account)
    } else {
        account.split('.').next().unwrap_or(account)
    }
}

fn parse_private_key(pem: &[u8], passphrase: Option<&str>) -> AnyResult<RsaPrivateKey> {
    let pem =
        std::str::from_utf8(pem).context("Snowflake RSA private key is not valid UTF-8 PEM")?;

    if let Some(passphrase) = passphrase {
        if let Ok(key) = RsaPrivateKey::from_pkcs8_encrypted_pem(pem, passphrase) {
            return Ok(key);
        }
    }
    if let Ok(key) = RsaPrivateKey::from_pkcs8_pem(pem) {
        return Ok(key);
    }

    bail!(
        "error parsing Snowflake PKCS#8 RSA private key; provide 'private_key_file_pwd' if the key is encrypted"
    )
}

fn public_key_fingerprint(key: &RsaPrivateKey) -> AnyResult<String> {
    let public_key_der = RsaPublicKey::from(key)
        .to_public_key_der()
        .context("error extracting Snowflake public key DER")?;
    Ok(BASE64_STANDARD.encode(Sha256::digest(public_key_der.as_bytes())))
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use pkcs8::{
        der::Decode,
        pkcs5::pbes2::{EncryptionScheme, Parameters, Pbkdf2Params},
        PrivateKeyInfo,
    };
    use rand::rngs::OsRng;

    #[test]
    fn normalizes_account_name_for_login_and_jwt() {
        assert_eq!(login_account_name("org-account"), "org-account");
        assert_eq!(login_account_name("xy12345.us-east-1"), "xy12345");
        assert_eq!(login_account_name("xy12345-external.global"), "xy12345");
    }

    #[test]
    fn parses_encrypted_private_key() {
        let key = RsaPrivateKey::new(&mut OsRng, 512).unwrap();
        let key_der = key.to_pkcs8_der().unwrap();
        let key_info = PrivateKeyInfo::from_der(key_der.as_bytes()).unwrap();
        let salt = b"test salt";
        let iv = b"test iv!";
        let encryption = Parameters {
            kdf: Pbkdf2Params::hmac_with_sha256(10, salt).unwrap().into(),
            encryption: EncryptionScheme::DesEde3Cbc { iv },
        };
        let encrypted = key_info
            .encrypt_with_params(encryption, "secret")
            .unwrap()
            .to_pem("ENCRYPTED PRIVATE KEY", LineEnding::LF)
            .unwrap();

        assert!(parse_private_key(encrypted.as_bytes(), None).is_err());
        assert!(parse_private_key(encrypted.as_bytes(), Some("wrong password")).is_err());
        assert!(parse_private_key(encrypted.as_bytes(), Some("secret")).is_ok());
    }
}
