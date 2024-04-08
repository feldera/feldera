import { librdkafkaDefaultValue, LibrdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'
import { useFormContext } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'

import { Autocomplete, TextField } from '@mui/material'

export const PickLibrdkafkaOptionElement = (props: {
  parentName: string
  fieldOptions: Record<string, Omit<LibrdkafkaOptions, 'name'>>
  usedFields: string[]
}) => {
  const ctx = useFormContext()
  return (
    <Autocomplete
      value={null}
      inputValue={undefined}
      blurOnSelect={true}
      onChange={(e, value) => {
        if (!value) {
          return
        }
        const field = value.replaceAll('.', '_')
        setTimeout(() => ctx.setFocus(props.parentName + '.' + field), 0)
        if (ctx.getValues(props.parentName + '.' + field)) {
          return
        }
        const option = props.fieldOptions[field]
        invariant(option)
        ctx.setValue(props.parentName + '.' + field, librdkafkaDefaultValue(option))
      }}
      options={Object.keys(props.fieldOptions)
        .filter(option => !props.usedFields.includes(option))
        .map(option => option.replaceAll('_', '.'))}
      size='small'
      renderInput={params => <TextField placeholder='Add option' {...params} />}
    />
  )
}
