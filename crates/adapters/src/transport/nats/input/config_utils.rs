use anyhow::Result as AnyResult;
use async_nats::jetstream::consumer as nats;
use feldera_types::transport::nats as cfg;

pub async fn translate_connect_options(
    config: &cfg::ConnectOptions,
) -> AnyResult<async_nats::ConnectOptions> {
    let mut options = async_nats::ConnectOptions::new();
    // TODO Handle the rest of the auth options
    if let Some(creds) = config.auth.credentials.as_ref() {
        match creds {
            cfg::Credentials::FromFile(path) => options = options.credentials_file(path).await?,
            cfg::Credentials::FromString(c) => options = options.credentials(c)?,
        }
    }
    Ok(options)
}

pub fn translate_consumer_options(config: &cfg::ConsumerConfig) -> nats::pull::OrderedConfig {
    nats::pull::OrderedConfig {
        name: config.name.clone(),
        description: config.description.clone(),
        filter_subject: Default::default(),
        filter_subjects: config.filter_subjects.clone(),
        replay_policy: nats::ReplayPolicy::Instant,
        rate_limit: config.rate_limit.clone(),
        sample_frequency: config.sample_frequency.clone(),
        headers_only: false,
        deliver_policy: translate_deliver_policy(&config.deliver_policy),
        max_waiting: config.max_waiting.clone(),
        metadata: config.metadata.clone(),
        max_batch: config.max_batch.unwrap_or_default(),
        max_bytes: config.max_bytes.unwrap_or_default(),
        max_expires: config.max_expires.unwrap_or_default(),
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
    }
}
