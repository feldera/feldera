use anyhow::{Result as AnyResult, anyhow, bail};
use async_nats::jetstream::consumer as nats;
use aws_lc_rs::signature::Ed25519KeyPair;
use feldera_types::transport::nats as cfg;
use std::sync::Arc;
use std::time::Duration;

/// Decodes a NATS seed ("SU..." for users) to the raw 32-byte Ed25519 seed.
///
/// The container format — RFC 4648 base32 without padding over two prefix
/// bytes, the 32-byte seed, and a trailing little-endian CRC-16/XMODEM — is
/// data framing, not cryptography; decoding it here lets the Ed25519 signing
/// itself go through aws-lc-rs.
fn decode_seed(seed: &str) -> AnyResult<[u8; 32]> {
    let raw = data_encoding::BASE32_NOPAD
        .decode(seed.as_bytes())
        .map_err(|e| anyhow!("invalid NATS nkey seed: {e}"))?;
    // Two prefix bytes + 32-byte seed + two CRC bytes.
    if raw.len() != 36 {
        bail!(
            "invalid NATS nkey seed: decoded to {} bytes, expected 36",
            raw.len()
        );
    }
    let (payload, crc) = raw.split_at(raw.len() - 2);
    if crc16_xmodem(payload) != u16::from_le_bytes([crc[0], crc[1]]) {
        bail!("invalid NATS nkey seed: checksum mismatch");
    }
    // The seed prefix (18 << 3) occupies the top five bits of the first
    // byte; the remaining bits carry the public-key prefix, which does not
    // matter for signing.
    if payload[0] & 0xF8 != 0x90 {
        bail!("invalid NATS nkey seed: not a seed");
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&payload[2..34]);
    Ok(out)
}

fn crc16_xmodem(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc
}

pub async fn translate_connect_options(
    config: &cfg::ConnectOptions,
) -> AnyResult<async_nats::ConnectOptions> {
    let connection_timeout = Duration::from_secs(config.connection_timeout_secs);
    let request_timeout = Duration::from_secs(config.request_timeout_secs);

    let mut options = async_nats::ConnectOptions::new()
        .connection_timeout(connection_timeout)
        .request_timeout(Some(request_timeout));

    options = apply_auth(options, &config.auth).await?;

    if let Some(path) = config.tls.root_certificates_file.as_ref() {
        options = options.add_root_certificates(path.clone());
    }
    if config.tls.require_tls {
        options = options.require_tls(true);
    }

    Ok(options)
}

/// Applies the configured authentication method to the connect options.
///
/// Exactly one method may be configured: `credentials`, `jwt` (which
/// requires `nkey` for nonce signing), bare `nkey`, `token`, or
/// `user_and_password`. Configuring none is valid (unauthenticated
/// connection); configuring more than one is rejected rather than silently
/// picking a winner.
async fn apply_auth(
    options: async_nats::ConnectOptions,
    auth: &cfg::Auth,
) -> AnyResult<async_nats::ConnectOptions> {
    let configured = [
        auth.credentials.is_some(),
        auth.jwt.is_some(),
        // A bare nkey is its own method; alongside jwt it is part of the
        // jwt method (the seed signs the connection nonce).
        auth.nkey.is_some() && auth.jwt.is_none(),
        auth.token.is_some(),
        auth.user_and_password.is_some(),
    ]
    .iter()
    .filter(|&&set| set)
    .count();
    if configured > 1 {
        bail!(
            "multiple NATS authentication methods configured; set exactly one of `credentials`, `jwt` (with `nkey`), `nkey`, `token`, or `user_and_password`"
        );
    }

    if let Some(creds) = auth.credentials.as_ref() {
        return Ok(match creds {
            cfg::Credentials::FromFile(path) => options.credentials_file(path).await?,
            cfg::Credentials::FromString(c) => options.credentials(c)?,
        });
    }

    if let Some(jwt) = auth.jwt.as_ref() {
        let Some(seed) = auth.nkey.as_ref() else {
            bail!(
                "NATS `jwt` authentication requires `nkey` (the seed that signs the connection nonce)"
            );
        };
        // The Ed25519 signing goes through aws-lc-rs so that FIPS builds
        // (`--features fips`) execute only AWS-LC primitives on this path
        // (EdDSA is FIPS 186-5, included in the AWS-LC FIPS module).
        let seed = decode_seed(seed)?;
        let key_pair = Arc::new(
            Ed25519KeyPair::from_seed_unchecked(&seed)
                .map_err(|e| anyhow!("invalid NATS nkey seed: {e}"))?,
        );
        return Ok(options.jwt(jwt.clone(), move |nonce| {
            let key_pair = key_pair.clone();
            async move { Ok(key_pair.sign(&nonce).as_ref().to_vec()) }
        }));
    }

    if let Some(seed) = auth.nkey.as_ref() {
        // Validated here for a precise error; the nonce signing itself
        // happens inside async-nats (which uses the nkeys crate).
        decode_seed(seed)?;
        return Ok(options.nkey(seed.clone()));
    }

    if let Some(token) = auth.token.as_ref() {
        return Ok(options.token(token.clone()));
    }

    if let Some(cfg::UserAndPassword { user, password }) = auth.user_and_password.as_ref() {
        return Ok(options.user_and_password(user.clone(), password.clone()));
    }

    Ok(options)
}

pub fn translate_consumer_options(config: &cfg::ConsumerConfig) -> nats::pull::OrderedConfig {
    nats::pull::OrderedConfig {
        name: config.name.clone(),
        description: config.description.clone(),
        filter_subject: Default::default(),
        filter_subjects: config.filter_subjects.clone(),
        replay_policy: translate_replay_policy(&config.replay_policy),
        rate_limit: config.rate_limit,
        sample_frequency: Default::default(),
        headers_only: false,
        deliver_policy: translate_deliver_policy(&config.deliver_policy),
        max_waiting: config.max_waiting,
        metadata: config.metadata.clone(),
        max_batch: config.max_batch.unwrap_or_default(),
        max_bytes: config.max_bytes.unwrap_or_default(),
        max_expires: config.max_expires.unwrap_or_default(),
    }
}

fn translate_replay_policy(p: &cfg::ReplayPolicy) -> nats::ReplayPolicy {
    match p {
        cfg::ReplayPolicy::Instant => nats::ReplayPolicy::Instant,
        cfg::ReplayPolicy::Original => nats::ReplayPolicy::Original,
    }
}

fn translate_deliver_policy(p: &cfg::DeliverPolicy) -> nats::DeliverPolicy {
    match p {
        cfg::DeliverPolicy::All => nats::DeliverPolicy::All,
        cfg::DeliverPolicy::Last => nats::DeliverPolicy::Last,
        cfg::DeliverPolicy::New => nats::DeliverPolicy::New,
        cfg::DeliverPolicy::ByStartSequence { start_sequence } => {
            nats::DeliverPolicy::ByStartSequence {
                start_sequence: *start_sequence,
            }
        }
        cfg::DeliverPolicy::ByStartTime { start_time } => nats::DeliverPolicy::ByStartTime {
            start_time: *start_time,
        },
        cfg::DeliverPolicy::LastPerSubject => nats::DeliverPolicy::LastPerSubject,
    }
}

#[cfg(test)]
mod test {
    use super::decode_seed;

    #[test]
    fn decode_seed_roundtrip() {
        // A seed framing the 32-byte sequence 00..1f, generated with the
        // reference nkeys encoding (user public prefix).
        let seed = "SUAAAAICAMCAKBQHBAEQUCYMBUHA6EARCIJRIFIWC4MBSGQ3DQOR4H776Y";
        let raw = decode_seed(seed).unwrap();
        assert_eq!(raw, core::array::from_fn(|i| i as u8));
        // Agrees with the nkeys crate's own decoding: same seed, same key.
        let ours = aws_lc_rs::signature::Ed25519KeyPair::from_seed_unchecked(&raw).unwrap();
        let theirs = nkeys::KeyPair::from_seed(seed).unwrap();
        let msg = b"nonce";
        theirs
            .verify(msg, ours.sign(msg).as_ref())
            .expect("aws-lc-rs signature must verify under the nkeys-derived public key");
    }

    #[test]
    fn decode_seed_rejects() {
        assert!(decode_seed("not base32!").is_err());
        assert!(decode_seed("SUAAAAICAMCAKBQHBAEQUCYMBUHA6EARCIJRIFIWC4MBSGQ3DQOR4H777Y").is_err()); // bad crc
        // A public key (prefix 'U', not a seed) must be rejected.
        let public = nkeys::KeyPair::new_user().public_key();
        assert!(decode_seed(&public).is_err());
    }
}
