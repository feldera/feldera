import { useMutation, useQueryClient } from '@tanstack/react-query'
import { FieldValues, SubmitHandler } from 'react-hook-form'
import {
  NewConnectorRequest,
  NewConnectorResponse,
  CancelError,
  ConnectorService,
  ConnectorDescr
} from 'src/types/manager'
import useStatusNotification from 'src/components/errors/useStatusNotification'

export const SourceFormCreateHandle = <TData extends FieldValues>(
  onFormSubmitted: (c: ConnectorDescr | undefined) => void,
  toSourceRequest: (data: TData) => NewConnectorRequest
): SubmitHandler<TData> => {
  const { mutate, isLoading } = useMutation<NewConnectorResponse, CancelError, NewConnectorRequest>(
    ConnectorService.newConnector
  )
  const queryClient = useQueryClient()
  const { pushMessage } = useStatusNotification()

  return (data: TData) => {
    const source_desc = toSourceRequest(data)

    if (!isLoading) {
      mutate(source_desc, {
        onSuccess: resp => {
          queryClient.invalidateQueries(['connector'])
          pushMessage({ message: 'Data Source created successfully!', key: new Date().getTime(), color: 'success' })
          onFormSubmitted({
            connector_id: resp.connector_id,
            name: source_desc.name,
            description: source_desc.description,
            typ: source_desc.typ,
            config: source_desc.config
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
