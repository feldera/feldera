import { Dispatch, SetStateAction } from 'react'
import { match, P } from 'ts-pattern'
import YAML from 'yaml'

import { Direction, ConnectorType, ConnectorDescr } from './manager'
import { CsvFileSchema, KafkaInputConnectorDialog, KafkaInputSchema, KafkaOutputSchema } from 'src/connectors/dialogs'
import { KafkaOutputConnectorDialog } from 'src/connectors/dialogs'
import { CsvFileConnectorDialog } from 'src/connectors/dialogs'

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
      console.log('unknown generic connector')
      return ConnectorType.FILE
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
    .exhaustive()

// Given an existing ConnectorDescr return an object with the right values for
// the dialog form.
//
// e.g., The ConnectorType.FILE will return an object corresponding to the yup
// schema defined in CsvFileConnector.tsx.
export const connectorToFormSchema = (
  connector: ConnectorDescr
): KafkaInputSchema | KafkaOutputSchema | CsvFileSchema => {
  const config = YAML.parse(connector.config)
  return match(connectorDescrToType(connector))
    .with(ConnectorType.KAFKA_IN, () => {
      return {
        name: connector.name,
        description: connector.description,
        host: config.transport.config['bootstrap.servers'],
        auto_offset: config.transport.config['auto.offset.reset'],
        topics: config.transport.config.topics
      } as KafkaInputSchema
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return {
        name: connector.name,
        description: connector.description,
        host: config.transport.config['bootstrap.servers'],
        auto_offset: config.transport.config['auto.offset.reset'],
        topic: config.transport.config.topic
      } as KafkaOutputSchema
    })
    .with(ConnectorType.FILE, () => {
      return {
        name: connector.name,
        description: connector.description,
        url: config.transport.config.path,
        has_headers: true // TODO: this isn't represented by the connector
      } as CsvFileSchema
    })
    .exhaustive()
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
    .exhaustive()
