import { ChangeEvent, Dispatch, useEffect, useReducer, useState } from 'react'

import { TextFieldProps } from '@mui/material'

type IntermediateInputState<T> = { valid: T } | { intermediate: string | null }

const getIntermediateState = <T>(
  state: IntermediateInputState<T>,
  textToValue: (text: string | null) => { valid: T } | 'invalid',
  action: string | null
): IntermediateInputState<T> => {
  try {
    const value = textToValue(action)
    if (value === 'invalid') {
      return {
        intermediate: action
      }
    }
    return {
      valid: value.valid
    }
  } catch {
    return state
  }
}

/**
 * Generate properties to pass into TextField component as-is
 * Forwards style and other props
 */
export function useIntermediateInput<T>({
  nullDisplayText = '',
  ...props
}: {
  onChange?: (event: ChangeEvent<HTMLInputElement>) => void
  nullDisplayText?: string
} & (
  | {
      value?: T
      textToValue: (text: string | null) => { valid: T } | 'invalid'
      valueToText: (valid: T) => string | null
      optional?: false
    }
  | {
      value?: T | undefined
      textToValue: (text: string | null) => { valid: T | undefined } | 'invalid'
      valueToText: (valid: T | undefined) => string | null
      optional?: true
    }
) &
  TextFieldProps) {
  const [value, setValueText] = useReducer(
    intermediateInputReducer(props.textToValue, props.onChange),
    props.value === undefined && !props.optional ? { intermediate: '' } : { valid: props.value }
  )
  {
    // The value of the input can either be changed
    // through processing the new editing state text (via setValueText)
    // or through the change of the props.value upstream - outside of reducer state.
    // The following useEffect resets the reducer state if the value upstream was changed,
    // regardless of current editing state, produced by useReducer.
    const propsValueText = 'value' in props && props.value !== undefined ? props.valueToText(props.value) : ''
    const [oldText, setOldText] = useState(propsValueText)
    useEffect(() => {
      if (propsValueText === oldText) {
        return
      }
      setValueText(propsValueText)
      setOldText(propsValueText)
    }, [propsValueText, oldText])
  }
  return intermediateValueInputProps({
    ...props,
    valueToText: props.valueToText,
    value,
    setValueText,
    nullDisplayText
  })
}

/**
 * Input component that can be in an invalid state.
 * While in invalid state, changes in input are not reflected on edited value.
 */
function intermediateValueInputProps<T>({
  value,
  setValueText,
  valueToText,
  ...props
}: {
  value: IntermediateInputState<T | undefined>
  setValueText: Dispatch<string | null>
  valueToText: (valid: T) => string | null
  nullDisplayText: string
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
    onChange: (e: ChangeEvent<HTMLInputElement>) => setValueText(e.target.value),
    value: 'valid' in value ? valueToText(value.valid!) ?? props.nullDisplayText : value.intermediate
  }
}

export function intermediateInputReducer<T>(
  textToValue: (text: string | null) => { valid: T } | 'invalid',
  onChange?: (event: ChangeEvent<HTMLInputElement>) => void
) {
  return (state: IntermediateInputState<T>, action: string | null) => {
    const result = getIntermediateState(state, textToValue, action)
    if ('valid' in result) {
      onChange?.({ target: { value: result.valid } } as any)
    }
    return result
  }
}
