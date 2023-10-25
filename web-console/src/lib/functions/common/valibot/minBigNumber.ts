import type { ErrorMessage, PipeResult } from 'valibot'
import { BigNumber } from 'bignumber.js'
import { getOutput, getPipeIssues } from 'valibot'

/**
 * Creates a validation function that validates the value of a string, number or date.
 *
 * @param requirement The minimum value.
 * @param error The error message.
 *
 * @returns A validation function.
 */
export function minBigNumber<TInput extends BigNumber, TRequirement extends TInput>(
  requirement: TRequirement,
  error?: ErrorMessage
) {
  return (input: TInput): PipeResult<TInput> =>
    input.lt(requirement) ? getPipeIssues('min_value', error || 'Invalid value', input) : getOutput(input)
}
