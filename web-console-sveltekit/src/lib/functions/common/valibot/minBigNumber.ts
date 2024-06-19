import type { BaseValidation, ErrorMessage } from 'valibot'
import { BigNumber } from 'bignumber.js/bignumber.js'
import { actionIssue, actionOutput } from 'valibot'

/**
 * Min value validation type.
 */
interface MinBigNumberValidation<TInput extends BigNumber, TRequirement extends TInput>
  extends BaseValidation<TInput> {
  /**
   * The validation type.
   */
  type: 'min_big_number'
  /**
   * The minimum value.
   */
  requirement: TRequirement
}

/**
 * Creates a validation function that validates the value of a string, number or date.
 *
 * @param requirement The minimum value.
 * @param message The error message.
 *
 * @returns A validation function.
 */
export function minBigNumber<TInput extends BigNumber, TRequirement extends TInput>(
  requirement: TRequirement,
  message?: ErrorMessage
): MinBigNumberValidation<TInput, TRequirement> {
  return {
    type: 'min_big_number',
    expects: `>=${requirement.toFixed()}`,
    async: false,
    message,
    requirement,
    _parse(input) {
      if (input.gte(this.requirement)) {
        return actionOutput(input)
      }
      return actionIssue(this, minBigNumber, input, 'value', input.toFixed())
    }
  }
}
