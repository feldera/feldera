import { BigNumberElement } from '$lib/components/input/BigNumberInput'
import { FormFieldOptions } from '$lib/functions/forms'
import { AutocompleteElement, SwitchElement, TextFieldElement } from 'react-hook-form-mui'
import { match } from 'ts-pattern'

import { Box } from '@mui/system'

export const FormFieldElement = (props: {
  field: string
  fieldOptions: FormFieldOptions | undefined
  disabled?: boolean
  parentName: string | undefined
}) => {
  if (!props.fieldOptions) {
    return <></>
  }

  const fieldPrefix = props.parentName ? props.parentName + '.' : ''

  return match(props.fieldOptions)
    .with({ type: 'string' }, () => (
      <TextFieldElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
      ></TextFieldElement>
    ))
    .with({ type: 'number' }, ({ range }) => (
      <TextFieldElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        type='number'
        inputProps={{
          ...range,
          inputProps: {
            'data-testid': 'input-' + props.field
          }
        }}
      ></TextFieldElement>
    ))
    .with({ type: 'bignumber' }, ({ range }) => (
      <BigNumberElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        inputProps={{
          ...range,
          inputProps: {
            'data-testid': 'input-' + props.field
          }
        }}
      />
    ))
    .with({ type: 'enum' }, ({ range }) => (
      <AutocompleteElement
        key={props.field}
        name={fieldPrefix + props.field}
        options={range.map(option => ({
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
          name={fieldPrefix + props.field}
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
            return v?.join(', ') ?? ''
          },
          output: event => {
            return event.target.value.split(', ')
          }
        }}
        name={fieldPrefix + props.field}
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
