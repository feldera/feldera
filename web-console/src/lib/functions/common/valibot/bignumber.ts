import type { BaseSchema, ErrorMessage, Pipe } from 'valibot'
import { BigNumber } from 'bignumber.js'
import { executePipe, getDefaultArgs, getSchemaIssues } from 'valibot'

/**
 * BigNumber schema type.
 */
export type BigNumberSchema<TOutput = BigNumber> = BaseSchema<BigNumber, TOutput> & {
  schema: 'bignumber'
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
export function bignumber(error?: ErrorMessage, pipe?: Pipe<BigNumber>): BigNumberSchema

export function bignumber(arg1?: ErrorMessage | Pipe<BigNumber>, arg2?: Pipe<BigNumber>): BigNumberSchema {
  // Get error and pipe argument
  const [error, pipe] = getDefaultArgs(arg1, arg2)

  // Create and return BigNumber schema
  return {
    /**
     * The schema type.
     */
    schema: 'bignumber',

    /**
     * Whether it's async.
     */
    async: false,

    /**
     * Parses unknown input based on its schema.
     *
     * @param input The input to be parsed.
     * @param info The parse info.
     *
     * @returns The parsed output.
     */
    _parse(input, info) {
      // Check type of input
      if (!BigNumber.isBigNumber(input)) {
        return getSchemaIssues(info, 'type', 'bignumber', error || 'Invalid type', input)
      }

      // Execute pipe and return result
      return executePipe(input, pipe, info, 'type')
    }
  }
}
