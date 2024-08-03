import type { BaseValidation, ErrorMessage } from 'valibot'
import { BigNumber } from 'bignumber.js/bignumber.js'
import { actionIssue, actionOutput } from 'valibot'

/**
 * Max BigNumber validation type.
 */
interface MaxBigNumberValidation<TInput extends BigNumber, TRequirement extends TInput>
  extends BaseValidation<TInput> {
  /**
   * The validation type.
   */
  type: 'max_big_number'
  /**
   * The maximum value.
   */
  requirement: TRequirement
}

/**
 * Creates a validation function that validates the value of a BigNumber.
 *
 * @param requirement The maximum value.
 * @param message The error message.
 *
 * @returns A validation function.
 */
export function maxBigNumber<TInput extends BigNumber, TRequirement extends TInput>(
  requirement: TRequirement,
  message?: ErrorMessage
): MaxBigNumberValidation<TInput, TRequirement> {
  return {
    type: 'max_big_number',
    expects: `<=${requirement.toFixed()}`,
    async: false,
    message,
    requirement,
    _parse(input) {
      if (input.lte(this.requirement)) {
        return actionOutput(input)
      }
      return actionIssue(this, maxBigNumber, input, 'value', input.toFixed())
    }
  }
}
