// Submit handlers for the connector forms.
//
// Sends either update or create requests to the backend.

import { useMutation, useQueryClient } from '@tanstack/react-query'
import { FieldValues, SubmitHandler } from 'react-hook-form'
import {
  NewConnectorRequest,
  NewConnectorResponse,
  CancelError,
  ConnectorService,
  UpdateConnectorRequest,
  UpdateConnectorResponse,
  ConnectorDescr
} from 'src/types/manager'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { connectorTypeToDirection } from 'src/types/connectors'

// Sends the request to create a new connector.
//
// Display success or error status message on completion.
export const ConnectorFormNewRequest = <TData extends FieldValues>(
  onFormSubmitted: (connector: ConnectorDescr | undefined) => void,
  toNewConnectorRequest: (data: TData) => NewConnectorRequest
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: newConnector, isLoading: newIsLoading } = useMutation<
    NewConnectorResponse,
    CancelError,
    NewConnectorRequest
  >(ConnectorService.newConnector)

  return (data: TData) => {
    const source_desc = toNewConnectorRequest(data)
    console.log('ConnectorFormNewRequest')
    console.log(source_desc)
    console.log(data)

    if (!newIsLoading) {
      newConnector(source_desc, {
        onSuccess: resp => {
          console.log('onSuccess')

          console.log(source_desc)
          console.log(data)

          queryClient.invalidateQueries(['connector'])
          pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: resp.connector_id,
            name: source_desc.name,
            description: source_desc.description,
            config: source_desc.config,
            direction: connectorTypeToDirection(source_desc.typ),
            typ: source_desc.typ
          })
        },
        onError: error => {
          pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          onFormSubmitted(undefined)
        }
      })
    }
  }
}

// Sends the request to update a connector.
//
// Display success or error status message on completion.
export const ConnectorFormUpdateRequest = <TData extends FieldValues>(
  onFormSubmitted: (connector: ConnectorDescr | undefined) => void,
  toConnectorFormUpdateRequest: (data: TData) => UpdateConnectorRequest
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: updateConnector, isLoading: updateIsLoading } = useMutation<
    UpdateConnectorResponse,
    CancelError,
    UpdateConnectorRequest
  >(ConnectorService.updateConnector)

  return (data: TData) => {
    const source_desc = toConnectorFormUpdateRequest(data)

    if (!updateIsLoading) {
      updateConnector(source_desc, {
        onSettled: () => {
          queryClient.invalidateQueries(['connector'])
          queryClient.invalidateQueries(['connectorStatus', { connector_id: source_desc.connector_id }])
        },
        onSuccess: () => {
          pushMessage({ message: 'Connector updated successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: source_desc.connector_id,
            name: source_desc.name,
            config: data.config,
            description: data.description,
            direction: connectorTypeToDirection(data.typ),
            typ: data.typ
          })
        },
        onError: error => {
          pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          onFormSubmitted(undefined)
        }
      })
    }
  }
}
