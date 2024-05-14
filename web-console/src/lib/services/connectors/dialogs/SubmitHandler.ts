// Submit handlers for the connector forms.
//
// Sends either update or create requests to the backend.

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { tuple } from '$lib/functions/common/tuple'
import {
  ApiError,
  ConnectorDescr,
  ConnectorId,
  ConnectorsService,
  NewConnectorRequest,
  NewConnectorResponse,
  UpdateConnectorRequest
} from '$lib/services/manager'
import { mutationUpdateConnector, PipelineManagerQueryKey } from '$lib/services/pipelineManagerQuery'
import { FieldValues, SubmitHandler } from 'react-hook-form'
import invariant from 'tiny-invariant'

import { useMutation, useQueryClient } from '@tanstack/react-query'

// Sends the request to create a new connector.
//
// Display success or error status message on completion.
export const useNewConnectorRequest = <TData extends FieldValues>(
  onSuccess: (connector: ConnectorDescr, oldConnectorName: string) => void,
  onSettled: () => void,
  getRequestData: (data: TData) => NewConnectorRequest
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: newConnector, isPending } = useMutation<NewConnectorResponse, ApiError, NewConnectorRequest>({
    mutationFn: ConnectorsService.newConnector
  })

  return (data: TData) => {
    if (isPending) {
      return
    }
    const sourceDesc = getRequestData(data)
    invariant(sourceDesc.name, 'Client error: cannot create connector - empty name!')
    newConnector(sourceDesc, {
      onSettled,
      onSuccess: resp => {
        invalidateQuery(queryClient, PipelineManagerQueryKey.connectors())
        pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
        onSuccess(
          {
            connector_id: resp.connector_id,
            ...sourceDesc
          },
          sourceDesc.name
        )
      },
      onError: error => {
        pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
      }
    })
  }
}

// Sends the request to update a connector.
//
// Display success or error status message on completion.
export const useUpdateConnectorRequest = <TData extends FieldValues>(
  oldConnector: ConnectorDescr,
  onSuccess: (connector: ConnectorDescr, oldConnectorName: string) => void,
  onSettled: () => void,
  getRequestData: (data: TData) => [{ connectorId: ConnectorId; connectorName: string }, UpdateConnectorRequest]
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: updateConnector, isPending } = useMutation(mutationUpdateConnector(queryClient))

  return (data: TData) => {
    if (isPending) {
      return
    }
    const [{ connectorId, connectorName }, request] = getRequestData(data)
    updateConnector(
      { connectorName, request },
      {
        onSettled,
        onSuccess: () => {
          pushMessage({ message: 'Connector updated successfully!', key: new Date().getTime(), color: 'success' })
          onSuccess(
            {
              connector_id: connectorId,
              name: request.name ?? oldConnector.name,
              description: request.description ?? oldConnector.description,
              config: request.config ?? oldConnector.config
            },
            connectorName
          )
        },
        onError: error => {
          pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
        }
      }
    )
  }
}

export const useConnectorRequest = <T extends FieldValues, R>(
  connector: ConnectorDescr | undefined,
  prepareData: (t: T) => R,
  onSuccess: ((connector: ConnectorDescr, oldConnectorName: string) => void) | undefined,
  handleClose: () => void
) => {
  // error: React Hook "..." is called conditionally.
  //        React Hooks must be called in the exact same order in every component render
  // TODO: connector won't change during lifetime. Figure out how to refactor to avoid the warning
  const onSubmit =
    connector === undefined
      ? /* eslint-disable react-hooks/rules-of-hooks */
        useNewConnectorRequest(onSuccess ?? (() => {}), handleClose, prepareData as (data: T) => NewConnectorRequest)
      : /* eslint-disable react-hooks/rules-of-hooks */
        useUpdateConnectorRequest(connector, onSuccess ?? (() => {}), handleClose, (data: T) =>
          tuple(
            { connectorId: connector.connector_id, connectorName: connector.name },
            prepareData(data) as UpdateConnectorRequest
          )
        )

  return onSubmit
}
