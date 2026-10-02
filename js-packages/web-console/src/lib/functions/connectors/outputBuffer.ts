import { BigNumber } from 'bignumber.js'
import * as va from 'valibot'
import { bignumber, maxBigNumber, minBigNumber } from '$lib/functions/common/valibot'
import type { FormFieldOptions } from '$lib/functions/forms'

export type OutputBufferConfig = {
  enable_output_buffer?: boolean
  max_output_buffer_size_records?: BigNumber
  max_output_buffer_time?: string | null
}

export const defaultOutputBufferOptions: OutputBufferConfig = {}

const minU64 = BigNumber(0)
const maxU64 = BigNumber('18446744073709551615')

export const outputBufferConfigSchema = va.object({
  enable_output_buffer: va.optional(va.boolean()),
  max_output_buffer_time: va.optional(va.nullable(va.string())),
  max_output_buffer_size_records: va.optional(
    va.pipe(bignumber, minBigNumber(minU64), maxBigNumber(maxU64))
  )
})

export const outputBufferConfigValidation = () =>
  va.forward(
    va.partialCheck(
      [['max_output_buffer_time'], ['max_output_buffer_size_records']],
      (input: any) =>
        !!input.max_output_buffer_time || !!input.max_output_buffer_size_records,
      'Specify either max_output_buffer_time or max_output_buffer_size_records'
    ),
    ['max_output_buffer_time']
  )

export const outputBufferOptions: Record<string, FormFieldOptions> = {
  enable_output_buffer: { type: 'boolean' },
  max_output_buffer_time: {
    type: 'string',
    getHelperText: () =>
      'Time as a number and a unit, for example "500ms", "10s", "1h30m" or "30d".'
  },
  max_output_buffer_size_records: {
    type: 'bignumber',
    range: { min: BigNumber(minU64), max: maxU64 }
  }
}
