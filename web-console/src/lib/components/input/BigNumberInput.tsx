import { useIntermediateInput } from '$lib/components/input/IntermediateInput'
import { nonNull } from '$lib/functions/common/function'
import { BigNumber } from 'bignumber.js/bignumber.js'
import { ChangeEventHandler } from 'react'
import invariant from 'tiny-invariant'

import { TextField, TextFieldProps } from '@mui/material'

const validValue = (
  value: BigNumber | null | undefined,
  props: {
    precision?: number | null
    scale?: number | null
    min?: BigNumber
    max?: BigNumber
  }
) => {
  if (!nonNull(value)) {
    return true
  }
  // Ensure value and defaultValue fit within precision and scale
  return !(
    (nonNull(props.min) && props.min.gt(value)) ||
    (nonNull(props.max) && props.max.lt(value)) ||
    (nonNull(props.scale) && !value.decimalPlaces(props.scale).eq(value)) ||
    (nonNull(props.precision) && value.precision(true) > props.precision)
  )
}

export const BigNumberInput = (
  props: Omit<TextFieldProps, 'type' | 'value' | 'defaultValue' | 'onChange' | 'min' | 'max'> & {
    value?: BigNumber | null
    defaultValue?: BigNumber
    precision?: number | null
    scale?: number | null
    onChange?: ChangeEventHandler<HTMLInputElement>
    min?: BigNumber.Value
    max?: BigNumber.Value
    allowInvalidRange?: boolean
  }
) => {
  // In a sane world we could expect value to be BigNumber
  const propsValue = props.value === null ? null : props.value ? BigNumber(props.value) : undefined

  const min = nonNull(props.min) ? BigNumber(props.min) : undefined
  const max = nonNull(props.max) ? BigNumber(props.max) : undefined

  invariant(
    validValue(propsValue, { ...props, min, max }),
    `BigNumber input value ${propsValue} doesn't fit precision ${props.precision} scale ${props.scale}`
  )
  invariant(
    validValue(props.defaultValue, { ...props, min, max }),
    `BigNumber input default value ${props.defaultValue} doesn't fit ${{
      precision: props.precision,
      scale: props.scale
    }}`
  )

  const cfg = {
    textToValue: (text: string | null) => {
      if (text === null) {
        return { valid: null }
      }
      if (text === '') {
        return 'invalid'
      }
      if (/^-?\d+\.$/.test(text)) {
        return 'invalid'
      }
      const value = BigNumber(text)
      if (value.isNaN()) {
        throw new Error()
      }
      if (!validValue(value, { ...props, min, max })) {
        if (props.allowInvalidRange) {
          props.onChange?.({ target: { value } } as any)
        }
        return 'invalid'
      }
      return { valid: value }
    },
    valueToText: (valid: BigNumber | null) => {
      return valid === null ? null : valid?.toFixed() ?? ''
    }
  }
  const valueObj = 'value' in props ? ({ value: props.value } as { value?: BigNumber | null }) : {}
  const intermediateInputProps = useIntermediateInput({
    ...(props as Omit<TextFieldProps, 'value'>),
    ...valueObj,
    ...cfg
  })
  return <TextField {...intermediateInputProps} />
}
