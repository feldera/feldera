import { bignumber, maxBigNumber, minBigNumber } from '$lib/functions/common/valibot'
import type { FormFieldOptions } from '$lib/functions/forms'
import { BigNumber } from 'bignumber.js'
import * as va from 'valibot'

export type OutputBufferConfig = {
  enable_output_buffer?: boolean
  max_output_buffer_size_records?: BigNumber
  max_output_buffer_time_millis?: BigNumber
}

export const defaultOutputBufferOptions: OutputBufferConfig = {}

const minU64 = BigNumber(0)
const maxU64 = BigNumber('18446744073709551615')

export const outputBufferConfigSchema = va.object({
  enable_output_buffer: va.optional(va.boolean()),
  max_output_buffer_time_millis: va.optional(
    va.pipe(bignumber, minBigNumber(minU64), maxBigNumber(maxU64))
  ),
  max_output_buffer_size_records: va.optional(
    va.pipe(bignumber, minBigNumber(minU64), maxBigNumber(maxU64))
  )
})

export const outputBufferConfigValidation = () =>
  va.forward(
    va.partialCheck(
      [['max_output_buffer_time_millis'], ['max_output_buffer_size_records']],
      (input: any) =>
        !!input.max_output_buffer_time_millis || !!input.max_output_buffer_size_records,
      'Specify either max_output_buffer_time_millis or max_output_buffer_size_records'
    ),
    ['max_output_buffer_time_millis']
  )

export const outputBufferOptions: Record<string, FormFieldOptions> = {
  enable_output_buffer: { type: 'boolean' },
  max_output_buffer_time_millis: {
    type: 'bignumber',
    range: { min: BigNumber(minU64), max: maxU64 }
  },
  max_output_buffer_size_records: {
    type: 'bignumber',
    range: { min: BigNumber(minU64), max: maxU64 }
  }
}
