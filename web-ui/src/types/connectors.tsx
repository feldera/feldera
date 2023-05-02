import { Dispatch, SetStateAction } from 'react'
import { match, P } from 'ts-pattern'
import YAML from 'yaml'
import assert from 'assert'

import { Direction, ConnectorDescr } from './manager'
import {
  ConfigEditorDialog,
  CsvFileConnectorDialog,
  CsvFileSchema,
  EditorSchema,
  KafkaInputConnectorDialog,
  KafkaInputSchema,
  KafkaOutputSchema,
  KafkaOutputConnectorDialog
} from 'src/connectors/dialogs'

export enum ConnectorType {
  KAFKA_IN = 'KafkaIn',
  KAFKA_OUT = 'KafkaOut',
  FILE = 'File',
  UNKNOWN = 'Unknown'
}

// Determine the type of a connector from its config entries.
export const connectorDescrToType = (cd: ConnectorDescr): ConnectorType => {
  const config = YAML.parse(cd.config)
  return match(config)
    .with({ transport: { name: 'kafka', config: { topics: P._ } } }, () => {
      return ConnectorType.KAFKA_IN
    })
    .with({ transport: { name: 'kafka', config: { topic: P._ } } }, () => {
      return ConnectorType.KAFKA_OUT
    })
    .with({ transport: { name: 'file' } }, () => {
      return ConnectorType.FILE
    })
    .otherwise(() => {
      return ConnectorType.UNKNOWN
    })
}

// Given a connector return the right dialog component for updating it.
export const ConnectorDialog = (props: {
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  connector: ConnectorDescr
}) =>
  match(connectorDescrToType(props.connector))
    .with(ConnectorType.KAFKA_IN, () => {
      return <KafkaInputConnectorDialog {...props} />
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return <KafkaOutputConnectorDialog {...props} />
    })
    .with(ConnectorType.FILE, () => {
      return <CsvFileConnectorDialog {...props} />
    })
    .with(ConnectorType.UNKNOWN, () => {
      return <ConfigEditorDialog {...props} />
    })
    .exhaustive()

// Given an existing ConnectorDescr return the KafkaInputSchema
// if connector is of type KAFKA_IN.
export const parseKafkaInputSchema = (connector: ConnectorDescr): KafkaInputSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.KAFKA_IN)
  const config = YAML.parse(connector.config)
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
  const config = YAML.parse(connector.config)
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
export const parseCsvFileSchema = (connector: ConnectorDescr): CsvFileSchema => {
  assert(connectorDescrToType(connector) === ConnectorType.FILE)
  const config = YAML.parse(connector.config)
  return {
    name: connector.name,
    description: connector.description,
    url: config.transport.config.path,
    has_headers: true // TODO: this isn't represented by the connector
  }
}

// Given an existing ConnectorDescr return EditorSchema for it.
export const parseEditorSchema = (connector: ConnectorDescr): EditorSchema => {
  return {
    name: connector.name,
    description: connector.description,
    config: connector.config
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
    .with(ConnectorType.FILE, () => {
      return Direction.INPUT_OUTPUT
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
    .with(ConnectorType.FILE, () => {
      return 'file'
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
    .with(ConnectorType.FILE, () => {
      return 'CSV'
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
    .with(ConnectorType.FILE, () => {
      return 'ph:file-csv'
    })
    .with(ConnectorType.UNKNOWN, () => {
      return 'file-icons:test-generic'
    })
    .exhaustive()

// Return name and color (for display of the table' chip) of a connector.
export const getStatusObj = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return { title: 'Kafka', color: 'secondary' as const }
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return { title: 'Kafka', color: 'secondary' as const }
    })
    .with(ConnectorType.FILE, () => {
      return { title: 'CSV', color: 'secondary' as const }
    })
    .with(ConnectorType.UNKNOWN, () => {
      return { title: 'Editor', color: 'secondary' as const }
    })
    .exhaustive()
