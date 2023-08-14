import { EditorSchema, KafkaInputSchema, KafkaOutputSchema, UrlSchema } from '$lib/components/connectors/dialogs'
import assert from 'assert'
import { match, P } from 'ts-pattern'

import { ConnectorType, Direction } from '$lib/types/connectors'
import { ConnectorDescr } from '$lib/types/manager'

// Determine the type of a connector from its config entries.
export const connectorDescrToType = (cd: ConnectorDescr): ConnectorType => {
  return match(cd.config)
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

  return {
    name: connector.name,
    description: connector.description,
    host: config.transport.config['bootstrap.servers'],
    auto_offset: config.transport.config['auto.offset.reset'],
    topics: config.transport.config.topics
  }
}

// Given an existing ConnectorDescr return the KafkaOutputSchema
// if connector is of type KAFKA_OUT.
export const parseKafkaOutputSchema = (connector: ConnectorDescr): KafkaOutputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.KAFKA_OUT)
  const config = connector.config
  assert(config.transport.config)

  return {
    name: connector.name,
    description: connector.description,
    host: config.transport.config['bootstrap.servers'],
    auto_offset: config.transport.config['auto.offset.reset'],
    topic: config.transport.config.topic
  }
}

// Given an existing ConnectorDescr return the CsvFileSchema
// if connector is of type FILE.
export const parseUrlSchema = (connector: ConnectorDescr): UrlSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.URL)
  const config = connector.config
  assert(config.transport.config)
  assert(config.format.name == 'json' || config.format.name == 'csv')
  return {
    name: connector.name,
    description: connector.description,
    url: config.transport.config.path,
    format: config.format.name
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
    .with(ConnectorType.URL, () => {
      return Direction.INPUT
    })
    .with(ConnectorType.UNKNOWN, () => {
      return Direction.INPUT_OUTPUT
    })
    .exhaustive()

/// Given a connector type return to which name in the config it corresponds to.
export const connectorTypeToConfig = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'kafka'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
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
    .with(ConnectorType.URL, () => {
      return 'HTTP URL'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'Connector'
    })
    .exhaustive()

// Return the icon of a connector (for display in components).
export const connectorTypeToIcon = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return 'logos:kafka'
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return 'logos:kafka'
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
    .with(ConnectorType.URL, () => {
      return { title: 'HTTP GET', color: 'secondary' as const }
    })
    .with(ConnectorType.UNKNOWN, () => {
      return { title: 'Editor', color: 'secondary' as const }
    })
    .exhaustive()
