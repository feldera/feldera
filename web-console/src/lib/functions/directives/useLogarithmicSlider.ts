import { SliderProps } from '@mui/material'

const fromLog2 = (maxLinear: number, maxLog: number) => (value: number) =>
  (Math.log2(value) / Math.log2(maxLog)) * maxLinear
const toLog2 = (maxLinear: number, maxLog: number) => (value: number) =>
  Math.round(Math.pow(2, (value / maxLinear) * Math.log2(maxLog)))

/**
 * Example usage:
 * ```
 *  <SliderElement
      ...
      step={null}
      valueLabelDisplay='off'
      name={['field_min', 'field_max']}
      {...useLogarithmicSlider({
        fromValue: v => v / 100,
        toValue: v => v * 100,
        marks: new Array(2).fill(undefined).flatMap((_, i) =>
          new Array(10)
            .fill(undefined)
            .map((_, i) => 100 * (i + 1))
            .map(value => ({
              value: value * Math.pow(10, i),
              label: Number.isInteger(Math.log10(value * Math.pow(10, i)))
                ? (value => format(value * 1000000, '0.00bd'))(value * Math.pow(10, i))
                : ''
            }))
            .concat({ value: 1500 * Math.pow(10, i), label: '' })
        ),
        max: 100,
        maxLog: 100
      })}
    />
  ```
 * @returns
 */
export const useLogarithmicSlider = ({
  min: _min = 0,
  max = 100,
  fromValue = v => v,
  toValue = v => v,
  valueLabelFormat = v => v,
  ...props
}: {
  maxLog: number
  min?: number
  max?: number
  fromValue?: (a: number) => number
  toValue?: (a: number) => number
  valueLabelFormat?: SliderProps['valueLabelFormat']
  marks?: SliderProps['marks']
}) => ({
  valueLabelFormat: (value: number, index: number) =>
    valueLabelFormat instanceof Function
      ? valueLabelFormat(toValue(toLog2(max, props.maxLog)(value)), index)
      : valueLabelFormat,
  fromValue: (v: number) => fromLog2(max, props.maxLog)(fromValue(v)),
  toValue: (v: number) => toValue(toLog2(max, props.maxLog)(v)),
  marks:
    typeof props.marks === 'boolean'
      ? props.marks
      : props.marks?.map(mark => ({ value: fromLog2(max, props.maxLog)(fromValue(mark.value)), label: mark.label }))
})