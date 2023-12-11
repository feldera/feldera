import {
  ConfigEditorDialog,
  KafkaInputConnectorDialog,
  KafkaOutputConnectorDialog,
  UrlConnectorDialog
} from '$lib/components/connectors/dialogs'
import { connectorDescrToType } from '$lib/functions/connectors'
import { ConnectorDescr } from '$lib/services/manager'
import { ConnectorType } from '$lib/types/connectors'
import { Dispatch, SetStateAction } from 'react'
import { match } from 'ts-pattern'

import { DebeziumInputConnectorDialog } from './DebeziumInputConnector'
import { SnowflakeOutputConnectorDialog } from './SnowflakeOutputConnector'

// Given a connector return the right dialog component for updating it.
export const AnyConnectorDialog = (props: {
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  connector: ConnectorDescr
  existingTitle: (name: string) => string
  submitButton: JSX.Element
  disabled?: boolean
}) =>
  match(connectorDescrToType(props.connector))
    .with(ConnectorType.KAFKA_IN, () => {
      return <KafkaInputConnectorDialog {...props} />
    })
    .with(ConnectorType.KAFKA_OUT, () => {
      return <KafkaOutputConnectorDialog {...props} />
    })
    .with(ConnectorType.DEBEZIUM_IN, () => {
      return <DebeziumInputConnectorDialog {...props} />
    })
    .with(ConnectorType.SNOWFLAKE_OUT, () => {
      return <SnowflakeOutputConnectorDialog {...props} />
    })
    .with(ConnectorType.URL, () => {
      return <UrlConnectorDialog {...props} />
    })
    .with(ConnectorType.UNKNOWN, () => {
      return <ConfigEditorDialog {...props} />
    })
    .exhaustive()
