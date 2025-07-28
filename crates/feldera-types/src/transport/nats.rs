use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use async_nats::jetstream::consumer::pull::OrderedConfig;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize, ToSchema)]
// TODO Expose more/all of async_nats::ConnectOptions here
pub struct NatsInputConfig {
    pub server_url: String,
    pub credentials: Option<String>,
    pub stream_name: String,
    // TODO: Having feldera-types depend on async_nats is probably undesirable.
    //
    // TODO: Currently we have to supply all those settings which are not tagged with
    // `serde(default)`, which seems a bit arbitrary.
    //
    // TODO: For the `max_*` settings, judging by the nats-server source code, it
    // seems like -1 represents an "unset" value. I'd prefer to use strong types instead of
    // sentinel values here, and not have this API be tainted by Golang's ways.
    // Also, any of these settings which are set to their `Default::default()`
    // value will not be serialized (by async_nats), so `0` is also
    // interpreted as "unset". For some reason though, the deserializer for `OrderedConfig`
    // does not tag them with `serde(default)`, so they can't be omitted in the user config.
    pub consumer_config: OrderedConfig,
}
