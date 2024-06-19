import type { BaseSchema, ErrorMessage, Pipe } from 'valibot'
import { BigNumber } from 'bignumber.js/bignumber.js'
import { defaultArgs, IssueReason, pipeResult, schemaIssue } from 'valibot'

/**
 * BigNumber schema type.
 */
interface BigNumberSchema<TOutput = BigNumber> extends BaseSchema<BigNumber, TOutput> {
  /**
   * The schema type.
   */
  type: IssueReason & 'instance'
  /**
   * The error message.
   */
  message: ErrorMessage | undefined
  /**
   * The validation and transformation pipeline.
   */
  pipe: Pipe<BigNumber> | undefined
}

/**
 * Creates a BigNumber schema.
 *
 * @param pipe A validation and transformation pipe.
 *
 * @returns A BigNumber schema.
 */
export function bignumber(pipe?: Pipe<BigNumber>): BigNumberSchema

/**
 * Creates a BigNumber schema.
 *
 * @param error The error message.
 * @param pipe A validation and transformation pipe.
 *
 * @returns A BigNumber schema.
 */
export function bignumber(message?: ErrorMessage, pipe?: Pipe<BigNumber>): BigNumberSchema

export function bignumber(
  arg1?: ErrorMessage | Pipe<BigNumber>,
  arg2?: Pipe<BigNumber>
): BigNumberSchema {
  // Get error and pipe argument
  const [message, pipe] = defaultArgs(arg1, arg2)

  // Create and return BigNumber schema
  return {
    /**
     * The schema type.
     */
    type: 'instance',

    expects: 'bignumber',

    /**
     * Whether it's async.
     */
    async: false,

    message,

    pipe,

    /**
     * Parses unknown input based on its schema.
     *
     * @param input The input to be parsed.
     * @param info The parse info.
     *
     * @returns The parsed output.
     */
    _parse(input, config) {
      // Check type of input
      if (!BigNumber.isBigNumber(input)) {
        return schemaIssue(this, bignumber, input, config)
      }

      // Execute pipe and return result
      return pipeResult(this, input, config)
    }
  }
}
