import { ChangeEvent, Dispatch, useReducer } from 'react'
import invariant from 'tiny-invariant'

import { TextFieldProps } from '@mui/material'

type IntermediateInputState<T> = { valid: T | null } | { intermediate: string }

/**
 * Generate properties to pass into TextField component as-is
 */
export function useIntermediateInput<T>(
  props: {
    value?: T
    onChange?: (event: ChangeEvent<HTMLInputElement>) => void
    textToValue: (text: string) => T
    valueToText: (v: T | null) => string
  } & TextFieldProps
) {
  const [value, setValueText] = useReducer(intermediateInputReducer(props.textToValue, props.onChange), {
    valid: props.value ?? null
  })
  return intermediateValueInputProps({
    ...props,
    valueToText: props.valueToText,
    value,
    setValueText
  })
}

/**
 * Input component that can be in an invalid state.
 * While in invalid state, changes in input are not reflected on edited value.
 */
export function intermediateValueInputProps<T>(
  props: {
    value: IntermediateInputState<T>
    setValueText: Dispatch<string | null>
    valueToText: (v: T | null) => string
  } & TextFieldProps
) {
  const error = 'intermediate' in props.value
  return {
    ...props,
    sx: {
      ...props.sx,
      backgroundColor: error ? '#FF000015' : undefined
    },
    error,
    onChange: (e: ChangeEvent<HTMLInputElement>) => props.setValueText(e.target.value),
    value:
      'valid' in props.value && props.value.valid === null
        ? ''
        : 'valid' in props.value
          ? props.valueToText(props.value.valid)
          : props.value.intermediate
  }
}

export function intermediateInputReducer<T>(
  textToValue: (text: string) => T,
  onChange?: (event: ChangeEvent<HTMLInputElement>) => void
) {
  return (_state: IntermediateInputState<T>, action: string | null): IntermediateInputState<T> => {
    try {
      const value = action === null ? null : textToValue(action)
      onChange?.({ target: { value } } as any)
      return {
        valid: value
      }
    } catch {
      invariant(action !== null)
      return {
        intermediate: action
      }
    }
  }
}
