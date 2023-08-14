import {
  ConfigEditorDialog,
  KafkaInputConnectorDialog,
  KafkaOutputConnectorDialog,
  UrlConnectorDialog
} from '$lib/components/connectors/dialogs'
import { Dispatch, SetStateAction } from 'react'
import { match } from 'ts-pattern'

import { connectorDescrToType } from '$lib/functions/connectors'
import { ConnectorType } from '$lib/types/connectors'
import { ConnectorDescr } from '$lib/services/manager'

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
    .with(ConnectorType.URL, () => {
      return <UrlConnectorDialog {...props} />
    })
    .with(ConnectorType.UNKNOWN, () => {
      return <ConfigEditorDialog {...props} />
    })
    .exhaustive()
