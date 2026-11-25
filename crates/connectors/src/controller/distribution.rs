//! Distributed input and output connectors.
//!
//! A distributed input connector runs on every host of a multihost pipeline,
//! and each host reads a different part of its input.  The coordinator decides
//! which connectors to distribute and tells each host through
//! `CoordinationActivate::input_distribution`.

use std::collections::BTreeMap;

use feldera_adapterlib::errors::controller::ConfigError;
use feldera_types::circuit::Layout;
use feldera_types::{
    config::{ConnectorConfig, TransportConfig},
    coordination::{InputDistribution, InputShard},
    program_schema::Relation,
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

    /// Whether the coordinator synchronizes the connector (see
    /// `CoordinationActivate::synchronized_inputs`).
    pub synchronized: bool,
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

/// Returns whether output connector `config` runs as a distributed connector, with a part on
/// every host: it sets `distributed`, its transport supports that (see
/// `TransportConfig::supports_output_distribution`), and the pipeline is `multihost`.
/// Otherwise, `distributed` has no effect, as before output connectors could be distributed.
pub fn output_is_distributed(config: &ConnectorConfig, multihost: bool) -> bool {
    config.distributed && config.transport.supports_output_distribution() && multihost
}

/// Returns an error if output connector `config` runs as a distributed connector (see
/// [output_is_distributed]) in a way that its transport does not support.
pub fn validate_output_distribution(
    endpoint_name: &str,
    config: &ConnectorConfig,
    multihost: bool,
) -> Result<(), ConfigError> {
    // The parts commit through a leader one transaction at a time, but output buffering flushes
    // each host's buffer at its own time, so the hosts' batches would not line up.
    if output_is_distributed(config, multihost)
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

/// Where a synchronized input connector gets its global bound (see the
/// `feldera_types::coordination` module).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoundSync {
    /// This host reads all of the connector's input, so the host's own bound
    /// is the global bound.
    Local,

    /// The hosts divide the connector's input, so the coordinator computes the
    /// global bound from the bounds of all of the hosts.
    Coordinated,
}

/// Returns how input connector `endpoint_name`, with `config`, which reads
/// `shard`, synchronizes its input, or `None` if it does not.
/// `coordinator_synchronizes` says whether the coordinator listed the
/// connector in `CoordinationActivate::synchronized_inputs`.
///
/// Returns an error if the hosts divide the input but the coordinator does
/// not synchronize the connector, because then the connector would never get
/// a bound.
pub fn bound_sync(
    endpoint_name: &str,
    config: &ConnectorConfig,
    shard: Option<&InputShard>,
    coordinator_synchronizes: bool,
) -> Result<Option<BoundSync>, ConfigError> {
    if !config.transport.synchronizes_input() {
        Ok(None)
    } else if shard.is_none_or(|shard| shard.n_hosts() <= 1) {
        Ok(Some(BoundSync::Local))
    } else if coordinator_synchronizes {
        Ok(Some(BoundSync::Coordinated))
    } else {
        Err(ConfigError::invalid_transport_configuration(
            endpoint_name,
            "the coordinator does not synchronize this distributed connector across the hosts, probably because it is too old; upgrade the coordinator, or remove the 'distributed' property",
        ))
    }
}

/// Returns an error if input connector `endpoint_name`, which reads `shard`
/// and synchronizes its input as `sync` says, feeds `table` and `table` has a
/// column with `LATENESS`.
///
/// The waterline of a table is global across the hosts, but each host reads
/// its part of the input at its own pace.  If one host gets ahead, the
/// waterline passes the input that the other hosts did not read yet, and the
/// pipeline drops that input as late.  A connector that the coordinator
/// synchronizes avoids that.
pub fn validate_lateness(
    endpoint_name: &str,
    shard: Option<&InputShard>,
    sync: Option<BoundSync>,
    table: &Relation,
) -> Result<(), ConfigError> {
    if shard.is_none_or(|shard| shard.n_hosts() <= 1) || sync == Some(BoundSync::Coordinated) {
        return Ok(());
    }
    let columns = table
        .fields
        .iter()
        .filter(|field| field.lateness.is_some())
        .map(|field| field.name.sql_name())
        .collect::<Vec<_>>();
    if columns.is_empty() {
        return Ok(());
    }
    Err(ConfigError::invalid_transport_configuration(
        endpoint_name,
        &format!(
            "a distributed input connector does not support table {}, because its column(s) {} have LATENESS: the hosts read their parts of the input independently, so the pipeline could drop input as late; synchronize the connector (with 'synchronize_partitions' for Kafka, or 'timestamp_column' for Delta Lake and Iceberg), remove the 'distributed' property, or run the pipeline on one host",
            table.name.sql_name(),
            columns.join(", "),
        ),
    ))
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use feldera_types::circuit::Layout;
    use feldera_types::{
        config::{ConnectorConfig, TransportConfig},
        coordination::{InputDistribution, InputShard},
        program_schema::{ColumnType, Field, Relation, SqlIdentifier},
        transport::{datagen::DatagenInputConfig, kafka::KafkaInputConfig},
    };

    use std::collections::BTreeMap;

    use super::{
        BoundSync, bound_sync, input_shard, output_is_distributed, resume_distribution,
        validate_distribution, validate_lateness, validate_output_distribution,
    };

    /// Table `t` with an `id` column and a `ts` column, with `LATENESS` on
    /// `ts` if `lateness` is true.
    fn table(lateness: bool) -> Relation {
        let ts = Field::new(SqlIdentifier::from("ts"), ColumnType::timestamp(false));
        let ts = if lateness {
            ts.with_lateness("INTERVAL '1' HOUR")
        } else {
            ts
        };
        Relation::new(
            SqlIdentifier::from("t"),
            vec![
                Field::new(SqlIdentifier::from("id"), ColumnType::bigint(false)),
                ts,
            ],
            false,
            BTreeMap::new(),
        )
    }

    /// Only a connector that more than one host reads is refused, only for a
    /// table with `LATENESS`, and only if the coordinator does not synchronize
    /// it.
    #[test]
    fn distributed_lateness_is_rejected() {
        let multihost = InputShard::new(0, 3, HOME_1).unwrap();
        let error = validate_lateness("c", Some(&multihost), None, &table(true)).unwrap_err();
        assert!(error.to_string().contains("LATENESS"), "{error}");
        assert!(error.to_string().contains("ts"), "{error}");

        validate_lateness("c", Some(&multihost), None, &table(false)).unwrap();
        validate_lateness("c", Some(&InputShard::ALL), None, &table(true)).unwrap();
        validate_lateness("c", None, None, &table(true)).unwrap();
        validate_lateness(
            "c",
            Some(&multihost),
            Some(BoundSync::Coordinated),
            &table(true),
        )
        .unwrap();
        assert!(
            validate_lateness("c", Some(&multihost), Some(BoundSync::Local), &table(true)).is_err()
        );
    }

    /// A Kafka connector with `synchronize_partitions`.
    fn synchronized_kafka() -> ConnectorConfig {
        let config: KafkaInputConfig = serde_json::from_value(serde_json::json!({
            "topic": "t",
            "bootstrap.servers": "localhost:9092",
            "synchronize_partitions": true,
        }))
        .unwrap();
        ConnectorConfig {
            distributed: true,
            ..ConnectorConfig::new(TransportConfig::KafkaInput(config), None)
        }
    }

    /// A connector that synchronizes is local on one host, coordinated on
    /// several, and refused on several if the coordinator does not synchronize
    /// it.
    #[test]
    fn bound_sync_depends_on_hosts_and_coordinator() {
        let multihost = InputShard::new(0, 3, HOME_1).unwrap();
        let sync = synchronized_kafka();
        assert_eq!(
            bound_sync("c", &kafka(true), Some(&multihost), true).unwrap(),
            None
        );
        assert_eq!(
            bound_sync("c", &sync, None, false).unwrap(),
            Some(BoundSync::Local)
        );
        assert_eq!(
            bound_sync("c", &sync, Some(&InputShard::ALL), false).unwrap(),
            Some(BoundSync::Local)
        );
        assert_eq!(
            bound_sync("c", &sync, Some(&multihost), true).unwrap(),
            Some(BoundSync::Coordinated)
        );
        let error = bound_sync("c", &sync, Some(&multihost), false).unwrap_err();
        assert!(error.to_string().contains("coordinator"), "{error}");
    }

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

    /// Only the null output, and the Delta Lake output that appends a change log, run as
    /// distributed connectors, and only on multihost.  Elsewhere `distributed` has no effect and is
    /// accepted, as it was before output connectors could be distributed.
    #[test]
    fn output_distribution_depends_on_the_transport() {
        let supported = [
            TransportConfig::NullOutput,
            delta(serde_json::json!({"mode": "append"})),
            delta(serde_json::json!({"mode": "append", "update_mode": "cdc"})),
        ];
        for transport in supported {
            let config = output(transport);
            assert!(output_is_distributed(&config, true));
            assert!(!output_is_distributed(&config, false));
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
        ];
        for transport in unsupported {
            let config = output(transport);
            assert!(!output_is_distributed(&config, true));
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
