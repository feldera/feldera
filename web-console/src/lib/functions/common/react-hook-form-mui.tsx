import { useFormContext, useWatch } from 'react-hook-form-mui'

import { Slider, SliderProps } from '@mui/material'

import { nonNull } from './function'

export const SliderElement = ({
  name,
  fromValue = a => a,
  toValue = a => a,
  ...props
}: { name: string | [string, string]; fromValue?: (a: number) => number; toValue?: (a: number) => number } & Omit<
  SliderProps,
  'name' | 'value' | 'onChange'
>) => {
  const ctx = useFormContext()
  const watched = useWatch({ name: typeof name === 'string' ? [name] : (name as string[]) }).map(x =>
    nonNull(x) ? fromValue(x) : 0
  )
  return (
    <Slider
      {...props}
      value={watched.length === 1 ? watched[0] : watched}
      onChange={(_, value) => {
        if (typeof value === 'number' || value.length === 1) {
          ctx.setValue(name as string, toValue(typeof value === 'number' ? value : value[0]))
          return
        }
        ctx.setValue(name[0], toValue(value[0]))
        ctx.setValue(name[1], toValue(value[1]))
      }}
    />
  )
}
