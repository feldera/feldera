import { BigNumberElement } from '$lib/components/input/BigNumberInput'
import { NumberElement } from '$lib/components/input/NumberInput'
import { FormFieldOptions } from '$lib/functions/forms'
import { useState } from 'react'
import {
  AutocompleteElement,
  FieldPath,
  FieldValues,
  SwitchElement,
  TextFieldElement,
  TextFieldElementProps
} from 'react-hook-form-mui'
import { match } from 'ts-pattern'

import { FormHelperText, IconButton, InputAdornment } from '@mui/material'
import { Box } from '@mui/system'

const PasswordElement = <
  TFieldValues extends FieldValues = FieldValues,
  TName extends FieldPath<TFieldValues> = FieldPath<TFieldValues>,
  TValue = unknown
>(
  props: TextFieldElementProps<TFieldValues, TName, TValue>
) => {
  const [showPassword, setShowPassword] = useState(false)
  return (
    <TextFieldElement
      {...props}
      InputProps={{
        ...props.InputProps,
        type: showPassword ? 'text' : 'password',
        endAdornment: (
          <InputAdornment position='end'>
            <IconButton
              aria-label='toggle password visibility'
              onClick={() => setShowPassword(!showPassword)}
              onMouseDown={event => event.preventDefault()}
            >
              {showPassword ? <i className='bx bx-hide' /> : <i className='bx bx-show' />}
            </IconButton>
          </InputAdornment>
        )
      }}
    />
  )
}

export const FormFieldElement = (props: {
  field: string
  fieldOptions: FormFieldOptions | undefined
  disabled?: boolean
  parentName: string | undefined
  helperText?: string
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
        helperText={props.helperText}
      ></TextFieldElement>
    ))
    .with({ type: 'secret_string' }, () => (
      <PasswordElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
        helperText={props.helperText}
      ></PasswordElement>
    ))
    .with({ type: 'number' }, ({ range }) => (
      <NumberElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        {...range}
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
        optional
        helperText={props.helperText}
      ></NumberElement>
    ))
    .with({ type: 'bignumber' }, ({ range }) => (
      <BigNumberElement
        key={props.field}
        name={fieldPrefix + props.field}
        size='small'
        fullWidth
        {...range}
        inputProps={{
          'data-testid': 'input-' + props.field
        }}
        optional
        helperText={props.helperText}
      />
    ))
    .with({ type: 'enum' }, ({ range }) => (
      <AutocompleteElement
        key={props.field}
        name={fieldPrefix + props.field}
        options={range.map(o => o)}
        textFieldProps={{
          inputProps: {
            'data-testid': 'input-' + props.field
          } as any,
          helperText: props.helperText
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
        {!!props.helperText && <FormHelperText>{props.helperText}</FormHelperText>}
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
        helperText={props.helperText}
      />
    ))
    .exhaustive()
}
