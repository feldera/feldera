import type { BigNumber } from 'bignumber.js'
import type { Dayjs } from 'dayjs'

export type SQLValueJS =
  | string
  | number
  | boolean
  | BigNumber
  | Dayjs
  | SQLValueJS[]
  | Map<string, SQLValueJS>
  | null
