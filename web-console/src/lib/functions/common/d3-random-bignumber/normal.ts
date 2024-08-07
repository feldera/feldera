import { BigNumber } from 'bignumber.js/bignumber.js'

import { RandomBigNumberGenerationSource } from './generationSource'

export interface RandomNormalBigNumber extends RandomBigNumberGenerationSource {
  /**
   * Returns a function for generating random numbers with a normal (Gaussian) distribution.
   * The expected value of the generated numbers is mu, with the given standard deviation sigma.
   * If mu is not specified, it defaults to 0; if sigma is not specified, it defaults to 1.
   *
   * @param mu Expected value, defaults to 0.
   * @param sigma Standard deviation, defaults to 1.
   */
  (mu?: BigNumber, sigma?: BigNumber): () => BigNumber
}

export const randomNormalBigNumber: RandomNormalBigNumber = (function sourceRandomNormalBigNumber(
  source
) {
  function randomNormalBigNumber(mu = new BigNumber(0), sigma = new BigNumber(1)): () => BigNumber {
    let x: number | null, r: number
    return function () {
      let y: number

      // If available, use the second previously-generated uniform random.
      if (x != null) {
        ;(y = x), (x = null)
      }
      // Otherwise, generate a new x and y.
      else
        do {
          x = source() * 2 - 1
          y = source() * 2 - 1
          r = new BigNumber(x).pow(2).plus(new BigNumber(y).pow(2)).toNumber()
        } while (!r || r > 1)

      return mu.plus(
        sigma.times(y).times(
          new BigNumber(-2)
            .times(new BigNumber(Math.log(r)))
            .div(r)
            .sqrt()
        )
      )
    }
  }

  randomNormalBigNumber.source = sourceRandomNormalBigNumber

  return randomNormalBigNumber
})(Math.random)
