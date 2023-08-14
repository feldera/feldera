// Submit handlers for the connector forms.
//
// Sends either update or create requests to the backend.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import {
  ApiError,
  ConnectorDescr,
  ConnectorId,
  ConnectorsService,
  NewConnectorRequest,
  NewConnectorResponse,
  UpdateConnectorRequest,
  UpdateConnectorResponse
} from '$lib/services/manager'
import { FieldValues, SubmitHandler } from 'react-hook-form'

import { useMutation, useQueryClient } from '@tanstack/react-query'

// Sends the request to create a new connector.
//
// Display success or error status message on completion.
export const ConnectorFormNewRequest = <TData extends FieldValues>(
  onFormSubmitted: (connector: ConnectorDescr | undefined) => void,
  toNewConnectorRequest: (data: TData) => [undefined, NewConnectorRequest]
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: newConnector, isLoading: newIsLoading } = useMutation<
    NewConnectorResponse,
    ApiError,
    NewConnectorRequest
  >(ConnectorsService.newConnector)

  return (data: TData) => {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, source_desc] = toNewConnectorRequest(data)

    if (!newIsLoading) {
      newConnector(source_desc, {
        onSuccess: resp => {
          queryClient.invalidateQueries(['connector'])
          pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: resp.connector_id,
            name: source_desc.name,
            description: source_desc.description,
            config: source_desc.config
          })
        },
        onError: error => {
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
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
  toConnectorFormUpdateRequest: (data: TData) => [ConnectorId, UpdateConnectorRequest]
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: updateConnector, isLoading: updateIsLoading } = useMutation<
    UpdateConnectorResponse,
    ApiError,
    { connector_id: ConnectorId; request: UpdateConnectorRequest }
  >(args => {
    return ConnectorsService.updateConnector(args.connector_id, args.request)
  })

  return (data: TData) => {
    const [connector_id, request] = toConnectorFormUpdateRequest(data)

    if (!updateIsLoading) {
      updateConnector(
        { connector_id, request },
        {
          onSettled: () => {
            queryClient.invalidateQueries(['connector'])
            queryClient.invalidateQueries(['connectorStatus', { connector_id }])
          },
          onSuccess: () => {
            pushMessage({ message: 'Connector updated successfully!', key: new Date().getTime(), color: 'success' })
            onFormSubmitted({
              connector_id: connector_id,
              name: request.name,
              config: data.config,
              description: data.description
            })
          },
          onError: error => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            onFormSubmitted(undefined)
          }
        }
      )
    }
  }
}
