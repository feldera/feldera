import { BigNumber } from 'bignumber.js'

import type { RandomBigNumberGenerationSource } from './generationSource'

/**
 * A configurable random number generator for the exponential distribution.
 */
export interface RandomExponentialBigNumber extends RandomBigNumberGenerationSource {
  /**
   * Returns a function for generating random numbers with an exponential distribution with the rate lambda;
   * equivalent to time between events in a Poisson process with a mean of 1 / lambda.
   *
   * @param lambda Expected time between events.
   */
  (lambda: BigNumber): () => BigNumber
}

export const randomExponentialBigNumber: RandomExponentialBigNumber =
  (function sourceRandomExponentialBigNumber(source) {
    function randomExponentialBigNumber(lambda: BigNumber) {
      return function () {
        return new BigNumber(-Math.log(1 - source())).div(lambda)
      }
    }

    randomExponentialBigNumber.source = sourceRandomExponentialBigNumber

    return randomExponentialBigNumber
  })(Math.random)
