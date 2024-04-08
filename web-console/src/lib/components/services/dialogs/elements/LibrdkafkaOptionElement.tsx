import { NumberInput } from '$lib/components/input/NumberInput'
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

  return match(props.fieldOptions)
    .with({ type: 'string' }, () => (
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
    .with({ type: 'number' }, options => (
      <TextFieldElement
        key={props.field}
        name={props.parentName + '.' + props.field}
        size='small'
        fullWidth
        component={NumberInput as any}
        {...options.range}
        {...{ allowInvalidRange: true }}
        inputProps={{
          inputProps: {
            'data-testid': 'input-' + props.field
          }
        }}
      ></TextFieldElement>
    ))
    .with({ type: 'enum' }, options => (
      <AutocompleteElement
        key={props.field}
        name={props.parentName + '.' + props.field}
        options={options.range.map(option => ({
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
    .with({ type: 'boolean' }, () => (
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
    .with({ type: 'list' }, { type: 'array' }, () => (
      <TextFieldElement
        key={props.field}
        multiline
        transform={{
          input: (v: string[]) => {
            return v?.join(', ')
          },
          output: event => {
            return event.target.value.split(', ')
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
