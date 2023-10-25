import { BigNumber } from 'bignumber.js'

export const inRangeInclusive = (range: [min: BigNumber, max: BigNumber] | null) => (value: BigNumber.Value) => {
  if (range === null) {
    return true
  }
  return range[0].comparedTo(value) <= 0 && range[1].comparedTo(value) >= 0
}

export const clampBigNumber = (min: BigNumber, max: BigNumber, value: BigNumber.Value) =>
  BigNumber.min(max, BigNumber.max(min, value))
