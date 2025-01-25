import { rawCheck } from 'valibot'
import { BigNumber } from 'bignumber.js/bignumber.js'

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
