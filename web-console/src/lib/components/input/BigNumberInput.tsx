import { nonNull } from '$lib/functions/common/function'
import { Arguments } from '$lib/types/common/function'
import { BigNumber } from 'bignumber.js'
import { FormEventHandler, WheelEventHandler } from 'react'
import invariant from 'tiny-invariant'

import { StandardTextFieldProps, SxProps, TextField, TextFieldProps, Theme } from '@mui/material'

type EventElementType = Omit<HTMLInputElement, 'value'> & { value?: BigNumber | null }

const callOnChange = <E1 extends { target: any }, E2 extends { target: { value: any } }>(
  cb: ((event: E1) => void) | undefined,
  event: E2,
  value: BigNumber | undefined
) => {
  cb?.({ ...(event as any), target: { ...event.target, value } })
}

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

const handleChange = (
  props: { min?: BigNumber.Value; max?: BigNumber.Value; precision?: number | null; scale?: number | null },
  event: any,
  value: string,
  cb: any
) => {
  if (value === '') {
    callOnChange(cb, event, undefined)
    return
  }

  const newValue = new BigNumber(value)

  const isInvalidValue =
    (props.min && newValue.lt(props.min)) || (props.max && newValue.gt(props.max)) || !validValue(newValue, props)
  if (isInvalidValue) {
    return
  }

  callOnChange(cb, event, newValue)
}

export const bigNumberInputProps = (
  props: Partial<{
    value: BigNumber | null
    defaultValue: BigNumber
    precision: number | null
    scale: number | null
    onChange: React.ChangeEventHandler<EventElementType>
    onInput: FormEventHandler
    min: BigNumber.Value
    max: BigNumber.Value
    sx: SxProps<Theme>
  }>
) => {
  // In a sane world we could expect value to be BigNumber
  const value = props.value ? BigNumber(props.value) : undefined

  invariant(
    validValue(value, props),
    `BigNumber input value ${value} doesn't fit precision ${props.precision} scale ${props.scale}`
  )
  invariant(
    validValue(props.defaultValue, props),
    `BigNumber input default value ${props.defaultValue} doesn't fit ${{
      precision: props.precision,
      scale: props.scale
    }}`
  )

  const onChange: TextFieldProps['onChange'] = !props.onChange
    ? undefined
    : event => handleChange(props, event, event.target.value, props.onChange)
  const onInput: TextFieldProps['onInput'] = !props.onInput
    ? undefined
    : event => handleChange(props, event, (event.target as any).value, props.onInput)
  const onWheel: WheelEventHandler = event => {
    handleChange(props, event, (value ?? new BigNumber(0)).plus(event.deltaY < 0 ? 1 : -1).toFixed(), props.onChange)
  }

  return {
    type: 'number',
    inputProps: {
      onWheel: onWheel,
      value: value?.toFixed() ?? '',
      defaultValue: props.defaultValue?.toFixed(),
      onChange: (e => {
        onChange?.(e)
      }) satisfies StandardTextFieldProps['onChange'],
      onInput: (e => {
        onInput?.(e)
      }) satisfies FormEventHandler<HTMLInputElement>
    },
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
