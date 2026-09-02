use anyhow::{Result as AnyResult, anyhow, bail};
use async_nats::jetstream::consumer as nats;
use feldera_types::transport::nats as cfg;
use std::sync::Arc;
use std::time::Duration;

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
        let key_pair = Arc::new(
            nkeys::KeyPair::from_seed(seed).map_err(|e| anyhow!("invalid NATS nkey seed: {e}"))?,
        );
        return Ok(options.jwt(jwt.clone(), move |nonce| {
            let key_pair = key_pair.clone();
            async move { key_pair.sign(&nonce).map_err(async_nats::AuthError::new) }
        }));
    }

    if let Some(seed) = auth.nkey.as_ref() {
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
