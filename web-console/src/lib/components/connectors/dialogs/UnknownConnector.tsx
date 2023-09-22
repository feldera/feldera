import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { Dispatch, SetStateAction } from 'react'

import { useQuery } from '@tanstack/react-query'

import { AnyConnectorDialog } from './AnyConnector'

export const UnknownConnectorDialog = (props: {
  connectorId: string
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
}) => {
  const { data } = useQuery(PipelineManagerQuery.connectorStatus(props.connectorId))
  if (!data) {
    return <></>
  }
  return <AnyConnectorDialog show={props.show} setShow={props.setShow} connector={data}></AnyConnectorDialog>
}
