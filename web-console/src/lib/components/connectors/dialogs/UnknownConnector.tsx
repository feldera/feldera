import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { Dispatch, SetStateAction } from 'react'

import { useQuery } from '@tanstack/react-query'

import { AnyConnectorDialog } from './AnyConnector'

export const UnknownConnectorDialog = ({
  connectorId,
  ...props
}: {
  connectorId: string
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  existingTitle: (name: string) => string
  submitButton: JSX.Element
  disabled?: boolean
}) => {
  const { data } = useQuery(PipelineManagerQuery.connectorStatus(connectorId))
  if (!data) {
    return <></>
  }
  return <AnyConnectorDialog connector={data} {...props}></AnyConnectorDialog>
}
