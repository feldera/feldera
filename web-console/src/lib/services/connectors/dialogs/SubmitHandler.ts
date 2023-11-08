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
    newConnector(sourceDesc, {
      onSuccess: resp => {
        invalidateQuery(queryClient, PipelineManagerQuery.connector())
        pushMessage({ message: 'Connector created successfully!', key: new Date().getTime(), color: 'success' })
        onFormSubmitted({
          connector_id: resp.connector_id,
          ...sourceDesc
        })
      },
      onError: error => {
        pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
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
  getRequestData: (data: TData) => [ConnectorId, UpdateConnectorRequest]
): SubmitHandler<TData> => {
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  const { mutate: updateConnector, isPending } = useMutation<
    UpdateConnectorResponse,
    ApiError,
    { connector_id: ConnectorId; request: UpdateConnectorRequest }
  >({
    mutationFn: args => ConnectorsService.updateConnector(args.connector_id, args.request)
  })

  return (data: TData) => {
    if (isPending) {
      return
    }
    const [connector_id, request] = getRequestData(data)
    updateConnector(
      { connector_id, request },
      {
        onSettled: () => {
          invalidateQuery(queryClient, PipelineManagerQuery.connector())
          invalidateQuery(queryClient, PipelineManagerQuery.connectorStatus(connector_id))
        },
        onSuccess: () => {
          pushMessage({ message: 'Connector updated successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: connector_id,
            name: request.name,
            description: request.description,
            config: request.config!
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

  const newRequest = (data: T) => prepareData(data) as NewConnectorRequest

  const updateRequest = (data: T) => tuple(connector!.connector_id, prepareData(data) as UpdateConnectorRequest)

  // error: React Hook "..." is called conditionally.
  //        React Hooks must be called in the exact same order in every component render
  // TODO: connector won't change during lifetime. Figure out how to refactor to avoid the warning
  const onSubmit =
    connector === undefined
      ? /* eslint-disable react-hooks/rules-of-hooks */
        useNewConnectorRequest(onFormSubmitted, newRequest)
      : /* eslint-disable react-hooks/rules-of-hooks */
        useUpdateConnectorRequest(onFormSubmitted, updateRequest)

  return onSubmit
}
