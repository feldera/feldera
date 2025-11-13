import { rawCheck } from 'valibot'
import { BigNumber } from 'bignumber.js'

export const maxBigNumber = (maxValue: BigNumber) =>
  rawCheck<BigNumber>(({ dataset, config, addIssue }) => {
    if (!dataset.typed) {
      return
    }
    if (dataset.value.lte(maxValue)) {
      return
    }
    addIssue({
      message: `Expected BigNumber <= ${maxValue}`
    })
  })
