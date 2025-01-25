import { custom } from 'valibot'
import { BigNumber } from 'bignumber.js/bignumber.js'

export const bignumber = custom<BigNumber>((input) => BigNumber.isBigNumber(input))
