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
  UpdateConnectorResponse
} from 'src/types/manager'
import useStatusNotification from 'src/components/errors/useStatusNotification'

// Sends the request to create a new connector.
//
// Display success or error status message on completion.
export const ConnectorFormNewRequest = <TData extends FieldValues>(
  onFormSubmitted: (connector_id: number | undefined) => void,
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

    if (!newIsLoading) {
      newConnector(source_desc, {
        onSuccess: resp => {
          queryClient.invalidateQueries(['connector'])
          pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted(resp.connector_id)
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
  onFormSubmitted: (connector_id: number | undefined) => void,
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
          onFormSubmitted(source_desc.connector_id)
        },
        onError: error => {
          pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          onFormSubmitted(undefined)
        }
      })
    }
  }
}
