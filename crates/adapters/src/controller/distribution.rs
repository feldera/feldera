//! Distributed input connectors.
//!
//! A distributed input connector runs on every host of a multihost pipeline,
//! and each host reads a different part of its input.  The coordinator decides
//! which connectors to distribute and tells each host through
//! `CoordinationActivate::input_distribution`.

use std::collections::BTreeMap;

use dbsp::circuit::Layout;
use feldera_adapterlib::errors::controller::ConfigError;
use feldera_types::{
    config::ConnectorConfig,
    coordination::{InputDistribution, InputShard},
};
use serde_json::Value as JsonValue;
use tracing::{info, warn};

/// The coordinator's instructions for one distributed input connector at
/// activation.
#[derive(Clone, Debug)]
pub struct ActivationInput {
    pub distribution: InputDistribution,

    /// See `CoordinationActivate::input_choices`.
    pub choice: Option<JsonValue>,
}

/// Returns the distribution to use when this host resumes from a checkpoint
/// whose distribution was `checkpointed`, given the coordinator's current
/// distribution `current`.
///
/// A connector that both distribute keeps its checkpointed home host, because
/// each host's resume state covers only the part of the input that the old
/// home host gave it, and the coordinator may now compute a different home (for
/// example, because input connectors were added or removed).  Every host
/// resumes from the same checkpoint step, so every host makes the same choice.
/// A connector that the checkpoint did not distribute uses the coordinator's
/// home host.
pub fn resume_distribution(
    current: BTreeMap<String, InputDistribution>,
    checkpointed: &BTreeMap<String, InputDistribution>,
) -> BTreeMap<String, InputDistribution> {
    current
        .into_iter()
        .map(|(name, distribution)| match checkpointed.get(&name) {
            Some(&old) if old != distribution => {
                info!(
                    "{name}: keeping home host {} from the checkpoint instead of home host {} from the coordinator",
                    old.home, distribution.home
                );
                (name, old)
            }
            _ => (name, distribution),
        })
        .collect()
}

/// Returns an error if `config` sets `distributed` but its transport cannot
/// divide its input among hosts.
pub fn validate_distribution(
    endpoint_name: &str,
    config: &ConnectorConfig,
) -> Result<(), ConfigError> {
    if config.distributed && !config.transport.supports_distribution() {
        return Err(ConfigError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "the '{}' transport does not support the 'distributed' property",
                config.transport.name()
            ),
        ));
    }
    Ok(())
}

/// Returns the part of the input that this host reads for input connector
/// `endpoint_name`, or `None` if this host reads all of it as an ordinary
/// connector.
///
/// `distribution` is the coordinator's instruction for the connector, if any.
///
/// A distributed connector in a single-host pipeline gets
/// [InputShard::ALL], so that it runs the same code as on multiple hosts.  In
/// a multihost pipeline, a distributed connector that the coordinator did not
/// distribute (for example, because the coordinator predates distribution)
/// reads all of its input on the one host that has it.
pub fn input_shard(
    endpoint_name: &str,
    config: &ConnectorConfig,
    layout: &Layout,
    distribution: Option<&InputDistribution>,
) -> Result<Option<InputShard>, ConfigError> {
    validate_distribution(endpoint_name, config)?;
    match (config.distributed, distribution) {
        (false, None) => Ok(None),
        (false, Some(_)) => Err(ConfigError::invalid_transport_configuration(
            endpoint_name,
            "the coordinator distributed a connector that does not set the 'distributed' property",
        )),
        (true, _) if layout.is_solo() => Ok(Some(InputShard::ALL)),
        (true, Some(distribution)) => {
            InputShard::new(layout.local_host_idx(), layout.n_hosts(), *distribution)
                .map(Some)
                .map_err(|error| {
                    // The coordinator computes homes for the current hosts, so
                    // an out-of-range home comes from a checkpoint (see
                    // [resume_distribution]).
                    ConfigError::invalid_transport_configuration(
                        endpoint_name,
                        &format!(
                            "invalid input distribution ({error}): the pipeline's checkpoint was probably taken with a different number of hosts, which is not supported; restart the pipeline with the number of hosts that it had when it took the checkpoint, or without the checkpoint"
                        ),
                    )
                })
        }
        (true, None) => {
            warn!(
                "{endpoint_name}: the coordinator did not distribute this connector, so this host reads all of its input"
            );
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use dbsp::circuit::Layout;
    use feldera_types::{
        config::{ConnectorConfig, TransportConfig},
        coordination::{InputDistribution, InputShard},
        transport::{datagen::DatagenInputConfig, kafka::KafkaInputConfig},
    };

    use std::collections::BTreeMap;

    use super::{input_shard, resume_distribution, validate_distribution};

    fn kafka(distributed: bool) -> ConnectorConfig {
        let config: KafkaInputConfig = serde_json::from_value(serde_json::json!({
            "topic": "t",
            "bootstrap.servers": "localhost:9092",
        }))
        .unwrap();
        ConnectorConfig {
            distributed,
            ..ConnectorConfig::new(TransportConfig::KafkaInput(config), None)
        }
    }

    fn datagen(distributed: bool) -> ConnectorConfig {
        ConnectorConfig {
            distributed,
            ..ConnectorConfig::new(
                TransportConfig::Datagen(DatagenInputConfig::default()),
                None,
            )
        }
    }

    /// Host `local` of a three-host layout.
    fn multihost(local: usize) -> Layout {
        let addresses = (0..3)
            .map(|i| (SocketAddr::from(([127, 0, 0, 1], 1000 + i)), 1))
            .collect::<Vec<_>>();
        Layout::new_multihost(&addresses, addresses[local].0).unwrap()
    }

    const HOME_1: InputDistribution = InputDistribution { home: 1 };

    #[test]
    fn ordinary_connectors_read_everything() {
        for layout in [Layout::new_solo(4), multihost(2)] {
            assert_eq!(
                input_shard("c", &kafka(false), &layout, None).unwrap(),
                None
            );
        }
    }

    #[test]
    fn single_host_gets_the_whole_shard() {
        let shard = input_shard("c", &kafka(true), &Layout::new_solo(4), None).unwrap();
        assert_eq!(shard, Some(InputShard::ALL));
    }

    #[test]
    fn multihost_follows_the_coordinator() {
        let shard = input_shard("c", &kafka(true), &multihost(2), Some(&HOME_1)).unwrap();
        assert_eq!(shard, Some(InputShard::new(2, 3, HOME_1).unwrap()));
    }

    /// An old coordinator puts a distributed connector on one host and sends
    /// no distribution.  That host must read everything, or input is lost.
    #[test]
    fn undistributed_by_coordinator_reads_everything() {
        assert_eq!(
            input_shard("c", &kafka(true), &multihost(0), None).unwrap(),
            None
        );
    }

    #[test]
    fn inconsistent_instructions_are_rejected() {
        // Distributing a connector that did not ask for it would make every
        // host read all of its input.
        assert!(input_shard("c", &kafka(false), &multihost(0), Some(&HOME_1)).is_err());

        // A home host that does not exist.
        let bad = InputDistribution { home: 3 };
        assert!(input_shard("c", &kafka(true), &multihost(0), Some(&bad)).is_err());
    }

    /// A checkpoint taken with more hosts can give a connector a home host
    /// that no longer exists.  The error says why, and what to do.
    #[test]
    fn resume_on_fewer_hosts_blames_the_checkpoint() {
        let resumed = resume_distribution(
            BTreeMap::from([("c".to_string(), InputDistribution { home: 0 })]),
            &BTreeMap::from([("c".to_string(), InputDistribution { home: 3 })]),
        );
        let error = input_shard("c", &kafka(true), &multihost(0), resumed.get("c")).unwrap_err();
        assert!(error.to_string().contains("checkpoint"), "{error}");
        assert!(error.to_string().contains("number of hosts"), "{error}");
    }

    #[test]
    fn unsupported_transports_are_rejected() {
        assert!(validate_distribution("c", &datagen(true)).is_err());
        assert!(validate_distribution("c", &datagen(false)).is_ok());
        assert!(validate_distribution("c", &kafka(true)).is_ok());
        assert!(input_shard("c", &datagen(true), &Layout::new_solo(1), None).is_err());
    }

    #[test]
    fn resume_keeps_checkpointed_homes() {
        let home = |home| InputDistribution { home };
        let current = BTreeMap::from([("a".to_string(), home(0)), ("new".to_string(), home(1))]);
        let checkpointed =
            BTreeMap::from([("a".to_string(), home(2)), ("gone".to_string(), home(1))]);
        assert_eq!(
            resume_distribution(current, &checkpointed),
            BTreeMap::from([("a".to_string(), home(2)), ("new".to_string(), home(1))])
        );
    }
}
