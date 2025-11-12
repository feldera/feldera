import type { Dayjs } from 'dayjs'
import type { BigNumber } from 'bignumber.js'

export type SQLValueJS =
  | string
  | number
  | boolean
  | BigNumber
  | Dayjs
  | SQLValueJS[]
  | Map<string, SQLValueJS>
  | null
