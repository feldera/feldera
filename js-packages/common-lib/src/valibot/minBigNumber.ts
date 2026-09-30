import type { BigNumber } from 'bignumber.js'
import { rawCheck } from 'valibot'

export const minBigNumber = (minValue: BigNumber) =>
  rawCheck<BigNumber>(({ dataset, config, addIssue }) => {
    if (!dataset.typed) {
      return
    }
    if (dataset.value.gte(minValue)) {
      return
    }
    addIssue({
      message: `Expected BigNumber >= ${minValue}`
    })
  })
