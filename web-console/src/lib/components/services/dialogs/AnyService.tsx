import { KafkaServiceDialog } from '$lib/components/services/dialogs/KafkaService'
import { ServiceDescr } from '$lib/services/manager'
import { ServiceType } from '$lib/types/xgressServices'
import { ServiceDialogProps } from '$lib/types/xgressServices/ServiceDialog'
import { match, P } from 'ts-pattern'

const getServiceType = (service: ServiceDescr | ServiceType): ServiceType =>
  match(service)
    .with('kafka', () => 'kafka' as const)
    .with(P.shape({ name: P.string }), service => getServiceType(service.config_type as any))
    .exhaustive()

export const AnyServiceDialog = (
  props: Omit<ServiceDialogProps, 'service'> & { service: ServiceDescr | ServiceType }
) => {
  const service = typeof props.service === 'string' ? undefined : props.service
  return match(getServiceType(props.service))
    .with('kafka', () => <KafkaServiceDialog {...props} service={service}></KafkaServiceDialog>)
    .exhaustive()
}
