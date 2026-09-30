import { BigNumber } from 'bignumber.js'
import { custom } from 'valibot'

export const bignumber = custom<BigNumber>((input) => BigNumber.isBigNumber(input))
