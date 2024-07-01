import { LibrdkafkaOptionsElement } from '$lib/components/services/dialogs/elements/LibrdkafkaOptionsElement'
import { librdkafkaAuthOptions, LibrdkafkaOptions, librdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'

const fieldOptions = librdkafkaOptions
  .filter(o => o.scope === '*' || o.scope === 'C')
  .filter(o => !librdkafkaAuthOptions.includes(o.name as any))
  .reduce((acc, o) => ((acc[o.name.replaceAll('.', '_')] = o), acc), {} as Record<string, LibrdkafkaOptions>)

export const TabKafkaInputDetails = (props: { disabled?: boolean; parentName: string }) => {
  const requiredFields = ['bootstrap_servers', 'auto_offset_reset', 'group_id', 'topics']
  return (
    <LibrdkafkaOptionsElement
      parentName={props.parentName}
      fieldOptions={fieldOptions}
      requiredFields={requiredFields}
      disabled={props.disabled}
    ></LibrdkafkaOptionsElement>
  )
}
