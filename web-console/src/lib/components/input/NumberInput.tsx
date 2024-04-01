import { nonNull } from '$lib/functions/common/function'
import { ChangeEventHandler, ClipboardEventHandler, KeyboardEventHandler } from 'react'
import invariant from 'tiny-invariant'

import { TextField, TextFieldProps } from '@mui/material'

export const handleNumericKeyDown: KeyboardEventHandler<Element> = event => {
  if (/Key[ACVXYZ]/.test(event.code) && event.ctrlKey) {
    return
  }
  if (/Key\w/.test(event.code)) {
    event.preventDefault()
    return
  }
  if (
    !/Digit\d|Numpad\d|Minus|NumpadSubtract|Period|Comma|NumpadDecimal|Backspace|Delete/.test(event.code) ||
    event.shiftKey
  ) {
    event.preventDefault()
    return
  }
}

const handleValueInput = (min: number | undefined, max: number | undefined, value: string | undefined | null) => {
  if (!nonNull(value)) {
    return value
  }
  if (value === '') {
    return undefined
  }
  const number = parseFloat(value)
  if (Number.isNaN(number)) {
    return 'ignore' as const
  }
  if ((nonNull(max) && number > max) || (nonNull(min) && number < min)) {
    return 'ignore' as const
  }
  return number
}

export const numberInputProps = (
  props: Omit<TextFieldProps, 'value'> & {
    value?: number | null
    min?: number
    max?: number
    step?: number
  }
): TextFieldProps => {
  invariant(props.value === null || props.value === undefined || typeof props.value === 'number')

  const handleChange: ChangeEventHandler<HTMLInputElement> = event => {
    const value = handleValueInput(props.min, props.max, event.target.value)
    if (value === 'ignore') {
      return
    }
    return props.onChange?.({ ...event, target: { ...event.target, value: value as any } })
  }
  const handlePaste: ClipboardEventHandler<HTMLDivElement> = event => {
    const value = handleValueInput(props.min, props.max, event.clipboardData.getData('text/plain'))
    if (value === 'ignore') {
      event.preventDefault()
      return
    }
  }

  return {
    ...props,
    inputProps: {
      ...{
        ...props.inputProps,
        min: props.min,
        max: props.max,
        step: props.step
      },
      ...('value' in props ? { value: props.value === null ? '' : props.value } : {})
    },
    type: 'number',
    onChange: handleChange,
    onPaste: handlePaste,
    onKeyDown: handleNumericKeyDown
  }
}

export const NumberInput = (props: TextFieldProps & { min?: number; max?: number; value?: number | null }) => (
  <TextField {...numberInputProps(props)}></TextField>
)
