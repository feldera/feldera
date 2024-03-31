import { LibrdkafkaOptions } from '$lib/functions/kafka/librdkafkaOptions'
import { AutocompleteElement, SwitchElement, TextFieldElement } from 'react-hook-form-mui'
import { match } from 'ts-pattern'

import { Box } from '@mui/system'

export const LibrdkafkaOptionElement = (props: {
  field: string
  fieldOptions: LibrdkafkaOptions | undefined
  disabled?: boolean
  parentName: string
}) => {
  if (!props.fieldOptions) {
    return <></>
  }

  const fieldOptions = props.fieldOptions
  return match(fieldOptions.type)
    .with('string', () => (
      <TextFieldElement
        key={props.field}
        name={props.parentName + '.' + props.field}
        size='small'
        fullWidth
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
      ></TextFieldElement>
    ))
    .with('number', () => (
      <TextFieldElement
        key={props.field}
        name={props.parentName + '.' + props.field}
        size='small'
        fullWidth
        type='number'
        inputProps={{
          ...(([, min, max]) => {
            return { min: parseInt(min), max: parseInt(max) }
          })(fieldOptions.range.match(/(\d+) .. (\d+)/) ?? []),
          inputProps: {
            'data-testid': 'input-' + props.field
          }
        }}
      ></TextFieldElement>
    ))
    .with('enum', () => (
      <AutocompleteElement
        key={props.field}
        name={props.parentName + '.' + props.field}
        options={fieldOptions.range.split(', ').map(option => ({
          id: option,
          label: option
        }))}
        textFieldProps={{
          inputProps: {
            'data-testid': 'input-' + props.field
          } as any
        }}
        autocompleteProps={{
          disableClearable: true,
          size: 'small',
          fullWidth: true,
          disabled: props.disabled,
          slotProps: {
            paper: {
              'data-testid': 'input-' + props.field
            } as any
          }
        }}
      ></AutocompleteElement>
    ))
    .with('boolean', () => (
      <Box sx={{ width: '100%' }}>
        <SwitchElement
          key={props.field}
          name={props.parentName + '.' + props.field}
          label={''}
          switchProps={{
            inputProps: {
              'data-testid': 'input-' + props.field
            } as any
          }}
        ></SwitchElement>
      </Box>
    ))
    .with('list', 'array', () => (
      <TextFieldElement
        key={props.field}
        multiline
        transform={{
          input: (v: string[]) => {
            return v?.join(', ') ?? ''
          },
          output: (v: string) => {
            return v.split(', ')
          }
        }}
        name={props.parentName + '.' + props.field}
        size='small'
        fullWidth
        disabled={props.disabled}
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
      />
    ))
    .exhaustive()
}
