export interface RandomBigNumberGenerationSource {
  /**
   * Returns the same type of function for generating random numbers but where the given random number
   * generator source is used as the source of randomness instead of Math.random.
   * This is useful when a seeded random number generator is preferable to Math.random.
   *
   * @param source Source (pseudo-)random number generator implementing the Math.random interface.
   * The given random number generator must implement the same interface as Math.random and
   * only return values in the range [0, 1).
   */
  source(source: () => number): this
}
