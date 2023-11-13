import type { BaseSchema, Issues, Output } from 'valibot'
import { getSchemaIssues } from 'valibot'

// ====================================

// https://github.com/fabian-hiller/valibot/pull/117/files

/**
 * Intersection options type.
 */
export type IntersectionOptions = [BaseSchema<any>, BaseSchema<any>, ...BaseSchema<any>[]]

type IntersectionInput<TIntersectionOptions extends IntersectionOptions> = TIntersectionOptions extends [
  BaseSchema<infer TInput, any>,
  ...infer TRest
]
  ? TRest extends IntersectionOptions
    ? TInput & IntersectionInput<TRest>
    : TRest extends [BaseSchema<infer TInput2, any>]
      ? TInput & TInput2
      : never
  : never

type IntersectionOutput<TIntersectionOptions extends IntersectionOptions> = TIntersectionOptions extends [
  BaseSchema<any, infer TOutput>,
  ...infer TRest
]
  ? TRest extends IntersectionOptions
    ? TOutput & IntersectionOutput<TRest>
    : TRest extends [BaseSchema<any, infer TOutput2>]
      ? TOutput & TOutput2
      : never
  : never

/**
 * Intersection schema type.
 */
export type IntersectionSchema<
  TIntersectionOptions extends IntersectionOptions,
  TOutput = IntersectionOutput<TIntersectionOptions>
> = BaseSchema<IntersectionInput<TIntersectionOptions>, TOutput> & {
  schema: 'intersection'
  intersection: TIntersectionOptions
}

/**
 * Creates an intersection schema.
 *
 * @param intersection The intersection schema.
 * @param error The error message.
 *
 * @returns An intersection schema.
 */
export function intersection<TIntersectionOptions extends IntersectionOptions>(
  intersection: TIntersectionOptions,
  error?: string
): IntersectionSchema<TIntersectionOptions> {
  return {
    /**
     * The schema type.
     */
    schema: 'intersection',

    /**
     * The intersection schema.
     */
    intersection,

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
      // Create issues and output
      let issues: Issues | undefined
      let output: [Output<IntersectionOptions[number]>] | undefined

      // Parse schema of each option
      for (const schema of intersection) {
        const result = schema._parse(input, info)

        // If there are issues, set output and break loop
        if (result.issues) {
          issues = result.issues
          // collect deeply nested issues
          while (issues?.length) {
            issues =
              (issues => (issues.length ? (issues as Issues) : undefined))(issues.flatMap(i => i.issues ?? [])) ??
              issues
            break
          }
          break
        } else {
          if (output) {
            output.push(result.output)
          } else {
            output = [result.output]
          }
        }
      }

      // Return input as output or issues
      return !issues && output
        ? {
            output: output.reduce((acc, value) => {
              if (typeof value === 'object') {
                return { ...acc, ...value }
              }
              return value
            }) as IntersectionOutput<TIntersectionOptions>
          }
        : issues?.length
          ? { issues }
          : getSchemaIssues(info, 'type', 'union', error || 'Invalid type', input, issues)
    }
  }
}
