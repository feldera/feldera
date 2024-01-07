import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { ConnectorDescr } from '$lib/services/manager'
import { Dispatch, SetStateAction } from 'react'

import { useQuery } from '@tanstack/react-query'

import { AnyConnectorDialog } from './AnyConnector'

export const UnknownConnectorDialog = ({
  connectorName,
  ...props
}: {
  connectorName: string
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  existingTitle: (name: string) => string
  submitButton: JSX.Element
  disabled?: boolean
  onSuccess?: (connector: ConnectorDescr, oldConnectorName: string) => void
}) => {
  const PipelineManagerQuery = usePipelineManagerQuery()
  const { data } = useQuery(PipelineManagerQuery.connectorStatus(connectorName))
  if (!data) {
    return <></>
  }
  return <AnyConnectorDialog connector={data} {...props}></AnyConnectorDialog>
}
