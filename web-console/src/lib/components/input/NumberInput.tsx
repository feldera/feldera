import { useIntermediateInput } from '$lib/components/input/IntermediateInput'
import { nonNull } from '$lib/functions/common/function'
import { ChangeEventHandler, ClipboardEventHandler, KeyboardEventHandler } from 'react'
import invariant from 'tiny-invariant'

import { TextField } from '@mui/material'

import type { TextFieldProps } from '@mui/material'

export const handleNumericKeyDown: KeyboardEventHandler<Element> = event => {
  // Allow keyboard shotcuts for Copy, Paste, etc.
  // .ctrlKey is true when Ctrl is pressed in most OS-s
  // .metaKey is true when Cmd is pressed in MacOS
  if (/Key[ACVXYZ]/.test(event.code) && (event.ctrlKey || event.metaKey)) {
    return
  }
  // Forbid text character keys
  if (/Key\w/.test(event.code)) {
    event.preventDefault()
    return
  }
  // Allow keys:
  // Digit\d|Numpad\d - keyboard and numpad digits
  // Minus|NumpadSubtract - minus sign
  // Period|Comma|NumpadDecimal - decimal point symbol for different locales
  // Backspace|Delete - text delete keys
  // Arrow[Up|Down|Left|Right] - keyboard arrows for cursor navigation
  if (
    !/Digit\d|Numpad\d|Minus|NumpadSubtract|Period|Comma|NumpadDecimal|Backspace|Delete|Arrow[Up|Down|Left|Right]/.test(
      event.code
    ) ||
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

/**
 * Generate props to pass to TextField component as-is to get a strict number input
 * Doesn't allow typing up numbers if the min value is larger than zero
 */
export const numberRangeInputProps = (
  props: Omit<TextFieldProps, 'value'> & {
    value?: number | null
    min?: number
    max?: number
    step?: number
  }
) => {
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
    ...('value' in props ? { value: props.value === null ? '' : props.value } : {}),
    inputProps: {
      ...{
        ...props.inputProps,
        min: props.min,
        max: props.max,
        step: props.step
      }
    },
    type: 'number',
    onChange: handleChange,
    onPaste: handlePaste,
    onKeyDown: handleNumericKeyDown
  } as TextFieldProps & { value?: number }
}

/**
 * Doesn't allow typing up numbers if the min value is larger than zero
 */
export const NumberRangeInput = (props: TextFieldProps & { min?: number; max?: number; value?: number | null }) => (
  <TextField {...numberRangeInputProps(props)}></TextField>
)

/**
 * Input highlights in red when the value doesn't fit the provided range
 * Error value is not applied
 */
export const NumberInput = (
  props: TextFieldProps & { min?: number; max?: number; value?: number | null; allowInvalidRange?: boolean }
) => {
  const intermediateInputProps = useIntermediateInput<number>({
    ...numberRangeInputProps(props),
    textToValue: text => {
      const value = parseFloat(text)
      if (Number.isNaN(value)) {
        throw new Error()
      }
      if ((nonNull(props.min) && props.min > value) || (nonNull(props.max) && props.max < value)) {
        props.allowInvalidRange && props.value !== value && props.onChange?.({ target: { value } } as any)
        throw new Error()
      }
      return value
    },
    valueToText: valid => valid?.toFixed() ?? ''
  })
  return <TextField {...intermediateInputProps}></TextField>
}
