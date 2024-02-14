import { LibrdkafkaOptionType } from '$lib/functions/kafka/librdkafkaOptions'
import { ServiceDescr } from '$lib/services/manager'
import { Dispatch, SetStateAction } from 'react'

export type ServiceType = 'kafka'

export type ServiceProps = {
  config: { bootstrap_servers: string[] } & Record<string, LibrdkafkaOptionType>
  description: string
  name: string
}

export type ServiceDialogProps = {
  service?: ServiceDescr
  show: boolean
  setShow: Dispatch<SetStateAction<boolean>>
  onSubmit: (serviceType: ServiceType, props: ServiceProps) => void
  onSuccess?: (service: ServiceDescr, oldServiceName: string) => void
  existingTitle: ((name: string) => string) | null
  submitButton: JSX.Element
  disabled?: boolean
}
