import { handleNumericKeyDown } from '$lib/components/input/NumberInput'
import { nonNull } from '$lib/functions/common/function'
import { Arguments } from '$lib/types/common/function'
import { BigNumber } from 'bignumber.js'
import { ChangeEventHandler, ClipboardEventHandler, WheelEventHandler } from 'react'
import invariant from 'tiny-invariant'

import { SxProps, TextField, TextFieldProps, Theme } from '@mui/material'

const validValue = (value: BigNumber | undefined, props: { precision?: number | null; scale?: number | null }) => {
  if (!nonNull(value)) {
    return true
  }
  // Ensure value and defaultValue fit within precision and scale
  return (
    (!nonNull(props.scale) || value.decimalPlaces(props.scale).eq(value)) &&
    (!nonNull(props.precision) || value.precision(true) <= props.precision)
  )
}

const handleValueInput = (
  props: { min?: BigNumber.Value; max?: BigNumber.Value; precision?: number | null; scale?: number | null },
  value: string
) => {
  if (value === '') {
    return undefined
  }

  const newValue = new BigNumber(value)

  const isInvalidValue =
    (props.min && newValue.lt(props.min)) || (props.max && newValue.gt(props.max)) || !validValue(newValue, props)
  if (isInvalidValue) {
    return 'ignore' as const
  }

  return newValue
}

export const bigNumberInputProps = (
  props: Partial<{
    value: BigNumber | null
    defaultValue: BigNumber
    precision: number | null
    scale: number | null
    onChange: ChangeEventHandler<HTMLInputElement>
    min: BigNumber.Value
    max: BigNumber.Value
    sx: SxProps<Theme>
  }>
) => {
  // In a sane world we could expect value to be BigNumber
  const propsValue = props.value ? BigNumber(props.value) : undefined

  invariant(
    validValue(propsValue, props),
    `BigNumber input value ${propsValue} doesn't fit precision ${props.precision} scale ${props.scale}`
  )
  invariant(
    validValue(props.defaultValue, props),
    `BigNumber input default value ${props.defaultValue} doesn't fit ${{
      precision: props.precision,
      scale: props.scale
    }}`
  )

  const handleChange: ChangeEventHandler<HTMLInputElement> = event => {
    const value = handleValueInput(props, event.target.value)
    if (value === 'ignore') {
      return
    }
    return props.onChange?.({ ...event, target: { ...event.target, value: value as any } })
  }
  const handlePaste: ClipboardEventHandler<HTMLInputElement> = event => {
    const value = handleValueInput(props, event.clipboardData.getData('text/plain'))
    if (value === 'ignore') {
      event.preventDefault()
      return
    }
  }
  const handleWheel: WheelEventHandler = event => {
    const value = handleValueInput(props, (propsValue ?? new BigNumber(0)).plus(event.deltaY < 0 ? 1 : -1).toFixed())
    return props.onChange?.({ ...event, target: { ...event.target, value: value } } as any)
  }

  return {
    type: 'number',
    inputProps: {
      onWheel: handleWheel,
      value: propsValue?.toFixed() ?? '',
      defaultValue: props.defaultValue?.toFixed(),
      onChange: handleChange,
      onPaste: handlePaste
    },
    onKeyDown: handleNumericKeyDown,
    sx: {
      ...props.sx,
      '& input::-webkit-outer-spin-button, & input::-webkit-inner-spin-button': {
        display: 'none'
      },
      '& input[type=number]': {
        MozAppearance: 'textfield'
      }
    }
  }
}

export const BigNumberInput = ({
  value,
  defaultValue,
  precision,
  scale,
  onChange,
  min,
  max,
  sx,
  ...props
}: Arguments<typeof bigNumberInputProps>[0] &
  Omit<TextFieldProps, 'type' | 'value' | 'defaultValue' | 'onChange' | 'min' | 'max'>) => {
  return (
    <TextField
      {...props}
      {...bigNumberInputProps({
        value,
        defaultValue,
        precision,
        scale,
        onChange,
        min,
        max,
        sx
      })}
    ></TextField>
  )
}
