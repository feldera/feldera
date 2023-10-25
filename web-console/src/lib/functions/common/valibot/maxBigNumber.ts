import type { ErrorMessage, PipeResult } from 'valibot'
import { BigNumber } from 'bignumber.js'
import { getOutput, getPipeIssues } from 'valibot'

/**
 * Creates a validation function that validates the value of a string, number or date.
 *
 * @param requirement The maximum value.
 * @param error The error message.
 *
 * @returns A validation function.
 */
export function maxBigNumber<TInput extends BigNumber, TRequirement extends TInput>(
  requirement: TRequirement,
  error?: ErrorMessage
) {
  return (input: TInput): PipeResult<TInput> =>
    input.gt(requirement) ? getPipeIssues('max_value', error || 'Invalid value', input) : getOutput(input)
}
