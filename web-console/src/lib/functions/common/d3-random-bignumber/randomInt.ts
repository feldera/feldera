import { BigNumber } from 'bignumber.js/bignumber.js'

import type { RandomBigNumberGenerationSource } from './generationSource'

/**
 * A configurable random integer generator for the uniform distribution.
 */
export interface RandomIntBigNumber extends RandomBigNumberGenerationSource {
  /**
   * Returns a function for generating random integers with a uniform distribution.
   * The minimum allowed value of a returned number is ⌊min⌋ (inclusive), and the maximum is ⌊max - 1⌋ (inclusive)
   * Min defaults to 0.
   *
   * @param max The maximum allowed value of a returned number.
   */
  (max: BigNumber): () => BigNumber
  /**
   * Returns a function for generating random integers with a uniform distribution.
   * The minimum allowed value of a returned number is ⌊min⌋ (inclusive), and the maximum is ⌊max - 1⌋ (inclusive)
   *
   * @param min The minimum allowed value of a returned number.
   * @param max The maximum allowed value of a returned number.
   */
  // tslint:disable-next-line:unified-signatures
  (min: BigNumber, max: BigNumber): () => BigNumber
}

export const randomIntBigNumber: RandomIntBigNumber = (function sourceRandomIntBigNumber(source) {
  function randomIntBigNumber(min: BigNumber, max?: BigNumber) {
    if (arguments.length < 2) (max = min), (min = new BigNumber(0))
    min = min!.decimalPlaces(0, BigNumber.ROUND_FLOOR)
    max = max!.decimalPlaces(0, BigNumber.ROUND_FLOOR).minus(min)
    return function () {
      return max!.times(source()).plus(min!).decimalPlaces(0, BigNumber.ROUND_FLOOR)
    }
  }

  randomIntBigNumber.source = sourceRandomIntBigNumber

  return randomIntBigNumber
})(Math.random)
