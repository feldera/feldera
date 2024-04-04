import { ChangeEvent, Dispatch, useReducer } from 'react'
import invariant from 'tiny-invariant'

import { TextFieldProps } from '@mui/material'

type IntermediateInputState<T> = { valid: T | null } | { intermediate: string }

const getIntermediateState = <T>(
  textToValue: (text: string) => T,
  action: string | null
): IntermediateInputState<T> => {
  try {
    const value = action === null ? null : textToValue(action)
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
  const [value, setValueText] = useReducer(
    intermediateInputReducer(props.textToValue, props.onChange),
    getIntermediateState(props.textToValue, props.valueToText(props.value ?? null))
  )
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
export function intermediateValueInputProps<T>({
  value,
  setValueText,
  valueToText,
  ...props
}: {
  value: IntermediateInputState<T>
  setValueText: Dispatch<string | null>
  valueToText: (v: T | null) => string
} & TextFieldProps) {
  const error = 'intermediate' in value
  return {
    ...props,
    sx: {
      ...props.sx,
      '.MuiInputBase-input': {
        backgroundColor: error ? '#FF000015' : undefined
      }
    },
    error,
    onChange: (e: ChangeEvent<HTMLInputElement>) => setValueText(e.target.value),
    value:
      'valid' in value && value.valid === null ? '' : 'valid' in value ? valueToText(value.valid) : value.intermediate
  }
}

export function intermediateInputReducer<T>(
  textToValue: (text: string) => T,
  onChange?: (event: ChangeEvent<HTMLInputElement>) => void
) {
  return (_state: IntermediateInputState<T>, action: string | null) => {
    const result = getIntermediateState(textToValue, action)
    if ('valid' in result) {
      onChange?.({ target: { value: result.valid } } as any)
    }
    return result
  }
}
