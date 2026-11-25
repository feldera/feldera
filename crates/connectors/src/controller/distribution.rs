//! Distributed input and output connectors.
//!
//! A distributed input connector runs on every host of a multihost pipeline,
//! and each host reads a different part of its input.  The coordinator decides
//! which connectors to distribute and tells each host through
//! `CoordinationActivate::input_distribution`.

use std::collections::{BTreeMap, BTreeSet};

use feldera_adapterlib::errors::controller::ConfigError;
use feldera_types::circuit::Layout;
use feldera_types::{
    config::{ConnectorConfig, TransportConfig},
    coordination::{InputDistribution, InputShard, legacy_output_distribution},
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

/// Returns the distribution of each distributed connector when this host
/// resumes from a checkpoint.  `current` is the distribution that the
/// coordinator computed now, and `checkpointed` is the one in the checkpoint.
///
/// The home host sets which part of the input each host reads (see
/// [InputDistribution::home]), and each host's checkpoint has its position
/// only in its own part.  The coordinator can now compute a different home
/// host than when the hosts wrote the checkpoint, for example because input
/// connectors were added or removed.  A new home host would give each host a
/// different part, and its checkpoint has no position for that part.
///
/// Thus, a connector that both distribute keeps the home host from the
/// checkpoint.  A connector that only `current` distributes uses the
/// coordinator's home host.  Every host resumes from the same checkpoint
/// step, so all of the hosts make the same choice.
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

/// Returns whether output connector `endpoint_name`, with configuration `config`, runs as a
/// distributed connector, with a part on every host.
///
/// On one host, no connector is distributed.  On multihost, the coordinator decides, from what
/// every host reported (see `CoordinationStatus::distributable_outputs`), and names the
/// connectors that it distributes in `distributed_outputs` (see
/// `CoordinationActivate::distributed_outputs`).  A coordinator that predates that list sends
/// `None`, and then a connector is distributed if [legacy_output_distribution] accepts it.
///
/// Returns an error if the coordinator distributes a connector that this pipeline cannot run as
/// a distributed connector, which it does not report as distributable.
pub fn output_is_distributed(
    endpoint_name: &str,
    config: &ConnectorConfig,
    multihost: bool,
    distributed_outputs: Option<&BTreeSet<String>>,
) -> Result<bool, ConfigError> {
    if !multihost {
        return Ok(false);
    }
    let Some(distributed_outputs) = distributed_outputs else {
        return Ok(legacy_output_distribution(config));
    };
    if !distributed_outputs.contains(endpoint_name) {
        return Ok(false);
    }
    let refusal = if config.distributed {
        config.output_distribution_refusal()
    } else {
        Some("the connector does not set 'distributed'".into())
    };
    match refusal {
        None => Ok(true),
        Some(refusal) => Err(ConfigError::invalid_transport_configuration(
            endpoint_name,
            &format!(
                "the coordinator runs this connector on every host, but this host cannot: {refusal}"
            ),
        )),
    }
}

/// Returns an error if output connector `config` can run as a distributed connector in a way
/// that its transport does not support.
pub fn validate_output_distribution(
    endpoint_name: &str,
    config: &ConnectorConfig,
    multihost: bool,
) -> Result<(), ConfigError> {
    // The parts commit through a leader one transaction at a time, but output buffering flushes
    // each host's buffer at its own time, so the hosts' batches would not line up.
    if config.distributed
        && config.supports_output_distribution()
        && multihost
        && matches!(config.transport, TransportConfig::DeltaTableOutput(_))
        && config.output_buffer_config.enable_output_buffer
    {
        return Err(ConfigError::invalid_transport_configuration(
            endpoint_name,
            "a distributed 'delta_table_output' connector does not support output buffering ('enable_output_buffer')",
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

    use feldera_types::circuit::Layout;
    use feldera_types::{
        config::{ConnectorConfig, TransportConfig},
        coordination::{InputDistribution, InputShard},
        transport::{datagen::DatagenInputConfig, kafka::KafkaInputConfig},
    };

    use std::collections::{BTreeMap, BTreeSet};

    use super::{
        input_shard, output_is_distributed, resume_distribution, validate_distribution,
        validate_output_distribution,
    };

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

    /// An output connector with `config` as its transport config.
    fn output(transport: TransportConfig) -> ConnectorConfig {
        ConnectorConfig {
            distributed: true,
            ..ConnectorConfig::new(transport, None)
        }
    }

    /// A Delta Lake output connector config with `config` added to its URI.
    fn delta(config: serde_json::Value) -> TransportConfig {
        let mut json = serde_json::json!({"uri": "memory://"});
        json.as_object_mut()
            .unwrap()
            .extend(config.as_object().unwrap().clone());
        TransportConfig::DeltaTableOutput(serde_json::from_value(json).unwrap())
    }

    /// Returns whether output connector `c`, with `config`, is distributed on multihost, with
    /// `list` as the coordinator's list of distributed connectors.
    fn distributed(config: &ConnectorConfig, list: Option<&[&str]>) -> bool {
        let list = list.map(|list| list.iter().map(|name| name.to_string()).collect());
        output_is_distributed("c", config, true, list.as_ref()).unwrap()
    }

    /// The null output and the Delta Lake output that appends a change log support distribution.
    /// The coordinator's list decides whether they are distributed.  Without a list, they are
    /// distributed, as they were before the list.  Elsewhere `distributed` has no effect and is
    /// accepted, as it was before output connectors could be distributed.  The coordinator
    /// distributes only connectors that every host can distribute, so one that this host cannot
    /// means that the hosts disagree.
    #[test]
    fn output_distribution_depends_on_the_transport() {
        let legacy = [
            TransportConfig::NullOutput,
            delta(serde_json::json!({"mode": "append"})),
            delta(serde_json::json!({"mode": "append", "update_mode": "cdc"})),
        ];
        for transport in legacy {
            let config = output(transport);
            assert!(config.supports_output_distribution());
            assert!(distributed(&config, None));
            assert!(distributed(&config, Some(&["c"])));
            assert!(!distributed(&config, Some(&[])));
            assert!(!output_is_distributed("c", &config, false, None).unwrap());
            validate_output_distribution("c", &config, true).unwrap();
        }

        let unsupported = [
            delta(serde_json::json!({"mode": "truncate"})),
            delta(serde_json::json!({"mode": "error_if_exists"})),
            delta(serde_json::json!({"mode": "append", "update_mode": "merge"})),
            TransportConfig::KafkaOutput(
                serde_json::from_value(serde_json::json!({
                    "topic": "t",
                    "bootstrap.servers": "localhost:9092",
                }))
                .unwrap(),
            ),
            TransportConfig::FileOutput(
                serde_json::from_value(serde_json::json!({"path": "f"})).unwrap(),
            ),
        ];
        for transport in unsupported {
            let config = output(transport);
            assert!(config.output_distribution_refusal().is_some());
            assert!(!distributed(&config, None));
            assert!(!distributed(&config, Some(&[])));
            let list = BTreeSet::from(["c".to_string()]);
            let error = output_is_distributed("c", &config, true, Some(&list)).unwrap_err();
            assert!(error.to_string().contains("this host cannot"), "{error}");
            validate_output_distribution("c", &config, true).unwrap();
        }

        // Output buffering is refused on a distributed Delta Lake output on multihost, but not on
        // one host, where the connector is not distributed, and not on the null output.
        let mut buffered = output(delta(serde_json::json!({"mode": "append"})));
        buffered.output_buffer_config.enable_output_buffer = true;
        let error = validate_output_distribution("c", &buffered, true).unwrap_err();
        assert!(error.to_string().contains("output buffering"), "{error}");
        validate_output_distribution("c", &buffered, false).unwrap();
        let mut buffered = output(TransportConfig::NullOutput);
        buffered.output_buffer_config.enable_output_buffer = true;
        validate_output_distribution("c", &buffered, true).unwrap();
    }
}
