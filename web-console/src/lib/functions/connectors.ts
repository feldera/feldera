import { EditorSchema, KafkaInputSchema, KafkaOutputSchema, UrlSchema } from '$lib/components/connectors/dialogs'
import { DebeziumInputSchema } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { SnowflakeOutputSchema } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { assertUnion } from '$lib/functions/common/array'
import { parseAuthParams } from '$lib/functions/kafka/authParamsSchema'
import { ConnectorDescr } from '$lib/services/manager'
import { ConnectorType, Direction } from '$lib/types/connectors'
import assert from 'assert'
import debeziumIcon from 'public/icons/vendors/debezium-icon-color.svg'
import debeziumLogo from 'public/icons/vendors/debezium-logo-color.svg'
import snowflakeIcon from 'public/icons/vendors/snowflake-icon.svg'
import snowflakeLogo from 'public/icons/vendors/snowflake-logo.svg'
import { match, P } from 'ts-pattern'

// Determine the type of a connector from its config entries.
export const connectorDescrToType = (cd: ConnectorDescr): ConnectorType => {
  return match(cd.config)
    .with(
      { transport: { name: 'kafka', config: { topics: P._ } }, format: { config: { update_format: 'debezium' } } },
      () => {
        return ConnectorType.DEBEZIUM_IN
      }
    )
    .with(
      { transport: { name: 'kafka', config: { topic: P._ } }, format: { config: { update_format: 'snowflake' } } },
      () => {
        return ConnectorType.SNOWFLAKE_OUT
      }
    )
    .with({ transport: { name: 'kafka', config: { topics: P._ } } }, () => {
      return ConnectorType.KAFKA_IN
    })
    .with({ transport: { name: 'kafka', config: { topic: P._ } } }, () => {
      return ConnectorType.KAFKA_OUT
    })
    .with({ transport: { name: 'url' } }, () => {
      return ConnectorType.URL
    })
    .otherwise(() => {
      return ConnectorType.UNKNOWN
    })
}

// Given an existing ConnectorDescr return the KafkaInputSchema
// if connector is of type KAFKA_IN.
export const parseKafkaInputSchema = (connector: ConnectorDescr): KafkaInputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.KAFKA_IN)
  const config = connector.config
  assert(config.transport.config)

  const authConfig = parseAuthParams(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    bootstrap_servers: config.transport.config['bootstrap.servers'],
    auto_offset_reset: config.transport.config['auto.offset.reset'],
    group_id: config.transport.config['group.id'] || '',
    topics: config.transport.config.topics,
    format_name: assertUnion(['json', 'csv'] as const, config.format.name),
    update_format: config.format.config?.update_format || 'raw',
    json_array: config.format.config?.array || false,
    ...authConfig
  }
}

// Given an existing ConnectorDescr return the KafkaOutputSchema
// if connector is of type KAFKA_OUT.
export const parseKafkaOutputSchema = (connector: ConnectorDescr): KafkaOutputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.KAFKA_OUT)
  const config = connector.config
  assert(config.transport.config)
  const authConfig = parseAuthParams(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    bootstrap_servers: config.transport.config['bootstrap.servers'],
    topic: config.transport.config.topic,
    format_name: assertUnion(['json', 'csv'] as const, config.format.name),
    json_array: config.format.config?.array || false,
    ...authConfig
  }
}

// Given an existing ConnectorDescr return the DebeziumInputSchema
// if connector is of type DEBEZIUM_IN.
export const parseDebeziumInputSchema = (connector: ConnectorDescr): DebeziumInputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.DEBEZIUM_IN)
  const config = connector.config
  assert(config.transport.config)

  const authConfig = parseAuthParams(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    bootstrap_servers: config.transport.config['bootstrap.servers'],
    auto_offset_reset: config.transport.config['auto.offset.reset'],
    group_id: config.transport.config['group.id'] || '',
    topics: config.transport.config.topics,
    format_name: assertUnion(['json'] as const, config.format.name),
    update_format: assertUnion(['debezium'] as const, config.format!.config!.update_format),
    json_flavor: assertUnion(['debezium_mysql'] as const, config.format!.config!.json_flavor),
    ...authConfig
  }
}

export const parseSnowflakeOutputSchema = (connector: ConnectorDescr): SnowflakeOutputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.SNOWFLAKE_OUT)
  const config = connector.config
  assert(config.transport.config)

  const authConfig = parseAuthParams(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    bootstrap_servers: config.transport.config['bootstrap.servers'],
    topic: config.transport.config.topic,
    format_name: assertUnion(['json', 'avro'] as const, config.format.name),
    update_format: assertUnion(['snowflake'] as const, config.format!.config!.update_format),
    ...authConfig
  }
}

// Given an existing ConnectorDescr return the CsvFileSchema
// if connector is of type FILE.
export const parseUrlSchema = (connector: ConnectorDescr): UrlSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.URL)
  const config = connector.config
  assert(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    url: config.transport.config.path,
    format_name: assertUnion(['json', 'csv'] as const, config.format.name),
    update_format: config.format.config?.update_format || 'raw',
    json_array: config.format.config?.array || false
  }
}

// Given an existing ConnectorDescr return EditorSchema for it.
export const parseEditorSchema = (connector: ConnectorDescr): EditorSchema => {
  assert(connector.config)
  return {
    name: connector.name,
    description: connector.description,
    config: JSON.stringify(connector.config, null, 2)
  }
}

// Given a ConnectorType determine for what it can be used, inputs, outputs or
// both.
export const connectorTypeToDirection = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return Direction.OUTPUT
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return Direction.OUTPUT
    })
    .with(ConnectorType.URL, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.UNKNOWN, () => {
      return Direction.INPUT_OUTPUT
    })
    .exhaustive()

/// Given a connector type return to which name in the config it corresponds to.
export const connectorTransportName = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'kafka'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'kafka'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return 'kafka'
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return 'kafka'
    })
    .with(ConnectorType.URL, () => {
      return 'url'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return ''
    })
    .exhaustive()

// Return the title of a connector (for display in components).
export const connectorTypeToTitle = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'Kafka Input'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'Kafka Output'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return 'Debezium Input'
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return 'Snowflake Output'
    })
    .with(ConnectorType.URL, () => {
      return 'HTTP URL'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'Connector'
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToLogo = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'logos:kafka'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'logos:kafka'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return debeziumLogo
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return snowflakeLogo
    })
    .with(ConnectorType.URL, () => {
      return 'tabler:http-get'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'file-icons:test-generic'
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToIcon = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'logos:kafka-icon'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'logos:kafka-icon'
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return debeziumIcon
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return snowflakeIcon
    })
    .with(ConnectorType.URL, () => {
      return 'tabler:http-get'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'file-icons:test-generic'
    })
    .exhaustive()

// Return name and color (for display of the table' chip) of a connector.
export const getStatusObj = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return { title: 'Kafka In', color: 'secondary' as const }
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return { title: 'Kafka Out', color: 'secondary' as const }
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return { title: 'Debezium In', color: 'secondary' as const }
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return { title: 'Snowflake Out', color: 'secondary' as const }
    })
    .with(ConnectorType.URL, () => {
      return { title: 'HTTP GET', color: 'secondary' as const }
    })
    .with(ConnectorType.UNKNOWN, () => {
      return { title: 'Editor', color: 'secondary' as const }
    })
    .exhaustive()
