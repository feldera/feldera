import { match } from 'ts-pattern'
import { Direction } from './manager'
import { ConnectorType } from './manager/models/ConnectorType'

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

export const getStatusObj = (status: ConnectorType) =>
  match(status)
    .with(ConnectorType.KAFKA_IN, () => {
      return { title: 'Kafka', color: 'error' as const }
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return { title: 'Kafka', color: 'error' as const }
    })
    .with(ConnectorType.FILE, () => {
      return { title: 'CSV', color: 'error' as const }
    })
    .exhaustive()
