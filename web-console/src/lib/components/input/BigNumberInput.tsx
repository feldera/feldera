import { BigNumber } from 'bignumber.js'

import { TextField, TextFieldProps } from '@mui/material'

type EventElementType = Omit<HTMLTextAreaElement | HTMLInputElement, 'value'> & { value?: BigNumber }

const callOnChange = <E1 extends { target: any }, E2 extends { target: { value: any } }>(
  cb: ((event: E1) => void) | undefined,
  event: E2,
  value: BigNumber | undefined
) => {
  cb?.({ ...(event as any), target: { ...event.target, value } })
}

export const BigNumberInput = ({
  ...props
}: Omit<TextFieldProps, 'type' | 'value' | 'defaultValue' | 'onChange' | 'min' | 'max'> &
  Partial<{
    value: BigNumber
    defaultValue: BigNumber
    onChange: React.ChangeEventHandler<EventElementType>
    min: BigNumber.Value
    max: BigNumber.Value
  }>) => {
  const handleChange = (event: any, value: string, cb: any) => {
    console.log('handleChange', value, event, event.currentTarget.value)
    if (value === '') {
      callOnChange(cb, event, undefined)
      return
    }

    const newValue = new BigNumber(value)
    console.log('handleChange 2', newValue)

    const isInvalidValue = (props.min && newValue.lt(props.min)) || (props.max && newValue.gt(props.max))
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

  return (
    <TextField
      {...props}
      type='number'
      value={props.value?.toFixed()}
      defaultValue={props.defaultValue?.toFixed()}
      onChange={onChange}
      onInput={onInput}
    ></TextField>
  )
}
