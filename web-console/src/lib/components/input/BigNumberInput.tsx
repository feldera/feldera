import { nonNull } from '$lib/functions/common/function'
import { BigNumber } from 'bignumber.js'
import invariant from 'tiny-invariant'

import { TextField, TextFieldProps } from '@mui/material'

type EventElementType = Omit<HTMLTextAreaElement | HTMLInputElement, 'value'> & { value?: BigNumber }

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
  console.log('validValue', value, typeof value)
  // Ensure value and defaultValue fit within precision and scale
  return (
    (!nonNull(props.scale) || value.decimalPlaces(props.scale).eq(value)) &&
    (!nonNull(props.precision) || value.precision(true) <= props.precision)
  )
}

export const BigNumberInput = ({
  sx,
  ...props
}: Omit<TextFieldProps, 'type' | 'value' | 'defaultValue' | 'onChange' | 'min' | 'max'> &
  Partial<{
    value: BigNumber
    defaultValue: BigNumber
    precision: number | null
    scale: number | null
    onChange: React.ChangeEventHandler<EventElementType>
    min: BigNumber.Value
    max: BigNumber.Value
  }>) => {
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

  const handleChange = (event: any, value: string, cb: any) => {
    if (value === '') {
      callOnChange(cb, event, undefined)
      return
    }

    const newValue = new BigNumber(value)

    console.log('handleChange')
    const isInvalidValue =
      (props.min && newValue.lt(props.min)) || (props.max && newValue.gt(props.max)) || !validValue(newValue, props)
    if (isInvalidValue) {
      return
    }

    callOnChange(cb, event, newValue)
  }

  const onChange: TextFieldProps['onChange'] = !props.onChange
    ? undefined
    : event => handleChange(event, event.target.value, props.onChange)
  const onInput: TextFieldProps['onInput'] = !props.onInput
    ? undefined
    : event => handleChange(event, (event.target as any).value, props.onInput)
  const onWheel: TextFieldProps['onWheel'] = event => {
    handleChange(event, (value ?? new BigNumber(0)).plus(event.deltaY < 0 ? 1 : -1).toFixed(), props.onChange)
  }

  return (
    <TextField
      {...props}
      onWheel={onWheel}
      type='number'
      value={value?.toFixed() ?? ''}
      defaultValue={props.defaultValue?.toFixed()}
      onChange={e => {
        onChange?.(e)
      }}
      onInput={e => {
        onInput?.(e)
      }}
      sx={{
        ...sx,
        '& input::-webkit-outer-spin-button, & input::-webkit-inner-spin-button': {
          display: 'none'
        },
        '& input[type=number]': {
          MozAppearance: 'textfield'
        }
      }}
    ></TextField>
  )
}
