import { useIntermediateInput } from '$lib/components/input/IntermediateInput'
import { nonNull } from '$lib/functions/common/function'
import { TextFieldElementProps, useFormContext, useFormState, useWatch } from 'react-hook-form-mui'
import invariant from 'tiny-invariant'

import { FormHelperText, TextField } from '@mui/material'

import type { TextFieldProps } from '@mui/material'
export type NumberInputProps = {
  min?: number
  max?: number
  value?: number | null
  allowInvalidRange?: boolean
  optional?: boolean
}

/**
 * Input highlights in red when the value doesn't fit the provided range
 * Invalid characters cannot be entered
 * Error value is not applied unless allowInvalidRange is true
 */
export const NumberInput = (props: TextFieldProps & NumberInputProps) => {
  const cfg = {
    textToValue: (text: string | null) => {
      if (text === null) {
        return { valid: null }
      }
      if (text === '') {
        return props.optional ? { valid: undefined } : 'invalid'
      }
      if (/^-?\d+\.$/.test(text)) {
        return 'invalid'
      }
      const value = Number(text)
      if (Number.isNaN(value)) {
        throw new Error()
      }
      if ((nonNull(props.min) && props.min > value) || (nonNull(props.max) && props.max < value)) {
        if (props.allowInvalidRange) {
          props.onChange?.({ target: { value } } as any)
        }
        return 'invalid'
      }
      return { valid: value }
    },
    valueToText: (valid?: number | null) => {
      return valid === null ? null : valid?.toString() ?? ''
    }
  }
  const valueObj = 'value' in props ? ({ value: props.value } as { value?: number | null }) : {}
  const intermediateInputProps = useIntermediateInput({
    ...(props as Omit<TextFieldProps, 'value'>),
    ...valueObj,
    ...cfg
  })
  return <TextField {...intermediateInputProps} />
}

export const NumberElement = (props: Omit<TextFieldElementProps & NumberInputProps, 'value'>) => {
  const ctx = useFormContext()
  const value = useWatch({ name: props.name })
  const state = useFormState({ name: props.name })
  invariant(value === undefined || value === null || Number.isFinite(value))
  return (
    <>
      <NumberInput
        {...props}
        value={value}
        onChange={event => {
          ctx.setValue(props.name, event.target.value)
        }}
      />
      {(e => e && <FormHelperText sx={{ color: 'error.main' }}>{e.message?.toString()}</FormHelperText>)(
        state.errors[props.name]
      )}
    </>
  )
}

/* Implementation for reference */
// export const handleNumericKeyDown: KeyboardEventHandler<Element> = event => {
//   // Allow keyboard shotcuts for Copy, Paste, etc.
//   // .ctrlKey is true when Ctrl is pressed in most OS-s
//   // .metaKey is true when Cmd is pressed in MacOS
//   if (/Key[ACVXYZ]/.test(event.code) && (event.ctrlKey || event.metaKey)) {
//     return
//   }
//   // Forbid text character keys
//   if (/Key\w/.test(event.code)) {
//     event.preventDefault()
//     return
//   }
//   // Allow keys:
//   // Digit\d|Numpad\d - keyboard and numpad digits
//   // Minus|NumpadSubtract - minus sign
//   // Period|Comma|NumpadDecimal - decimal point symbol for different locales
//   // Backspace|Delete - text delete keys
//   // Arrow[Up|Down|Left|Right] - keyboard arrows for cursor navigation
//   if (
//     !/Digit\d|Numpad\d|Minus|NumpadSubtract|Period|Comma|NumpadDecimal|Backspace|Delete|Arrow[Up|Down|Left|Right]/.test(
//       event.code
//     ) ||
//     event.shiftKey
//   ) {
//     event.preventDefault()
//     return
//   }
// }
