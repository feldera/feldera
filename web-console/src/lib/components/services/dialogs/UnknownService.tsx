import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { ServiceType } from '$lib/types/xgressServices'
import { ServiceDialogProps } from '$lib/types/xgressServices/ServiceDialog'

import { useQuery } from '@tanstack/react-query'

import { AnyServiceDialog } from './AnyService'

export const UnknownServiceDialog = (
  props: Omit<ServiceDialogProps, 'service'> & ({ serviceName: string } | { serviceType: ServiceType })
) => {
  const PipelineManagerQuery = usePipelineManagerQuery()
  const { data } = useQuery(PipelineManagerQuery.listServices()) // Fetching entire list to utilize cache that probably exists
  const service = 'serviceName' in props ? data?.find(service => service.name === props.serviceName) : props.serviceType
  if (!service) {
    return <></>
  }
  return <AnyServiceDialog {...props} service={service}></AnyServiceDialog>
}
