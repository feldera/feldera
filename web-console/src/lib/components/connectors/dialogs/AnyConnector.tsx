import { DebeziumInputConnectorDialog } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { DeltaLakeInputConnectorDialog } from '$lib/components/connectors/dialogs/DeltaLakeInputConnector'
import { DeltaLakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/DeltaLakeOutputConnector'
import { ConfigEditorDialog } from '$lib/components/connectors/dialogs/GenericEditorConnector'
import { KafkaInputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaInputConnector'
import { KafkaOutputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaOutputConnector'
import { S3InputConnectorDialog } from '$lib/components/connectors/dialogs/S3InputConnector'
import { SnowflakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { UrlConnectorDialog } from '$lib/components/connectors/dialogs/UrlConnector'
import { connectorDescrToType } from '$lib/functions/connectors'
import { ConnectorDescr } from '$lib/services/manager'
import { ConnectorType } from '$lib/types/connectors'
import { Dispatch, SetStateAction } from 'react'
import { match } from 'ts-pattern'

export const getConnectorDialogComponent = (type: ConnectorType) =>
  match(type)
    .with(ConnectorType.KAFKA_IN, () => KafkaInputConnectorDialog)
    .with(ConnectorType.KAFKA_OUT, () => KafkaOutputConnectorDialog)
    .with(ConnectorType.DEBEZIUM_IN, () => DebeziumInputConnectorDialog)
    .with(ConnectorType.SNOWFLAKE_OUT, () => SnowflakeOutputConnectorDialog)
    .with(ConnectorType.DELTALAKE_IN, () => DeltaLakeInputConnectorDialog)
    .with(ConnectorType.DELTALAKE_OUT, () => DeltaLakeOutputConnectorDialog)
    .with(ConnectorType.S3_IN, () => S3InputConnectorDialog)
    .with(ConnectorType.URL_IN, () => UrlConnectorDialog)
    .with(ConnectorType.UNKNOWN, () => ConfigEditorDialog)
    .exhaustive()

// Given a connector return the right dialog component for updating it.
export const AnyConnectorDialog = (props: {
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  connector: ConnectorDescr
  existingTitle: (name: string) => string
  submitButton: JSX.Element
  disabled?: boolean
  onSuccess?: (connector: ConnectorDescr, oldConnectorName: string) => void
}) =>
  (Dialog => <Dialog {...props}></Dialog>)(getConnectorDialogComponent(connectorDescrToType(props.connector.config)))
