import type { BigNumber } from 'bignumber.js'
import type { Dayjs } from 'dayjs'

export type SQLValueJS =
  | string
  | number
  | boolean
  | BigNumber
  | Dayjs
  | Uint8Array
  | SQLValueJS[]
  | Map<string, SQLValueJS>
  | null
