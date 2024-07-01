import { LibrdkafkaOptionsElement } from '$lib/components/services/dialogs/elements/LibrdkafkaOptionsElement'
import { librdkafkaAuthOptions, LibrdkafkaOptions, librdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'

const fieldOptions = librdkafkaOptions
  .filter(o => o.scope === '*' || o.scope === 'P')
  .filter(o => !librdkafkaAuthOptions.includes(o.name as any))
  .reduce((acc, o) => ((acc[o.name.replaceAll('.', '_')] = o), acc), {} as Record<string, LibrdkafkaOptions>)

export const TabKafkaOutputDetails = (props: { disabled?: boolean; parentName: string }) => {
  const requiredFields = ['bootstrap_servers', 'topic']
  return (
    <LibrdkafkaOptionsElement
      parentName={props.parentName}
      fieldOptions={fieldOptions}
      requiredFields={requiredFields}
      disabled={props.disabled}
    ></LibrdkafkaOptionsElement>
  )
}
