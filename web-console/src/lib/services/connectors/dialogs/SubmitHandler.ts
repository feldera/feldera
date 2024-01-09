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
  UpdateConnectorRequest,
  UpdateConnectorResponse
} from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { Dispatch } from 'react'
import { FieldValues, SubmitHandler } from 'react-hook-form'
import invariant from 'tiny-invariant'

import { useMutation, useQueryClient } from '@tanstack/react-query'

// Sends the request to create a new connector.
//
// Display success or error status message on completion.
export const useNewConnectorRequest = <TData extends FieldValues>(
  onFormSubmitted: (connector: ConnectorDescr | undefined) => void,
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
      onSuccess: resp => {
        invalidateQuery(queryClient, PipelineManagerQuery.connectors())
        pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
        onFormSubmitted({
          connector_id: resp.connector_id,
          ...sourceDesc
        })
      },
      onError: error => {
        pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
        onFormSubmitted(undefined)
      }
    })
  }
}

// Sends the request to update a connector.
//
// Display success or error status message on completion.
export const useUpdateConnectorRequest = <
  TData extends /*{
  name: string,
  description: string,
  config: ConnectorConfig
}*/ FieldValues
>(
  onFormSubmitted: (connector: ConnectorDescr | undefined) => void,
  getRequestData: (data: TData) => [{ connectorId: ConnectorId; connectorName: string }, UpdateConnectorRequest]
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: updateConnector, isPending } = useMutation<
    UpdateConnectorResponse,
    ApiError,
    { connectorName: string; request: UpdateConnectorRequest }
  >({
    mutationFn: args => ConnectorsService.updateConnector(args.connectorName, args.request)
  })

  return (data: TData) => {
    if (isPending) {
      return
    }
    const [{ connectorId, connectorName }, request] = getRequestData(data)
    updateConnector(
      { connectorName, request },
      {
        onSettled: () => {
          invalidateQuery(queryClient, PipelineManagerQuery.connectors())
          invalidateQuery(queryClient, PipelineManagerQuery.connectorStatus(connectorName))
        },
        onSuccess: () => {
          pushMessage({ message: 'Connector updated successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: connectorId,
            name: request.name,
            description: request.description,
            config: request.config!
          })
        },
        onError: error => {
          pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
          onFormSubmitted(undefined)
        }
      }
    )
  }
}

export const useConnectorRequest = <T extends FieldValues, R>(
  connector: ConnectorDescr | undefined,
  prepareData: (t: T) => R,
  onSuccess: Dispatch<ConnectorDescr> | undefined,
  handleClose: () => void
) => {
  const onFormSubmitted = (connector: ConnectorDescr | undefined) => {
    handleClose()
    if (connector !== undefined && onSuccess !== undefined) {
      onSuccess(connector)
    }
  }

  // error: React Hook "..." is called conditionally.
  //        React Hooks must be called in the exact same order in every component render
  // TODO: connector won't change during lifetime. Figure out how to refactor to avoid the warning
  const onSubmit =
    connector === undefined
      ? /* eslint-disable react-hooks/rules-of-hooks */
        useNewConnectorRequest(onFormSubmitted, prepareData as (data: T) => NewConnectorRequest)
      : /* eslint-disable react-hooks/rules-of-hooks */
        (updateRequest => useUpdateConnectorRequest(onFormSubmitted, updateRequest))((data: T) =>
          tuple(
            { connectorId: connector.connector_id, connectorName: connector.name },
            prepareData(data) as UpdateConnectorRequest
          )
        )

  return onSubmit
}
