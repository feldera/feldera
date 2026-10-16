import { BigNumber } from 'bignumber.js'

export const inRangeInclusive =
  (range: { min: BigNumber; max: BigNumber }) => (value: BigNumber.Value) => {
    return range.min.comparedTo(value)! <= 0 && range.max.comparedTo(value)! >= 0
  }

export const clampBigNumber = (min: BigNumber, max: BigNumber, value: BigNumber.Value) =>
  BigNumber.min(max, BigNumber.max(min, value))
