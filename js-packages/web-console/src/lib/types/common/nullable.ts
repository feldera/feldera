/**
 * `Value`, carrying the nullability of `Source`: `null` stays `null` and
 * `undefined` stays `undefined`, so a mapping function can accept a nullable
 * argument without the caller unwrapping it first.
 *
 * @example
 * ```ts
 * type A = SameNullability<Microseconds, number>              // Microseconds
 * type B = SameNullability<Microseconds, number | undefined>  // Microseconds | undefined
 * ```
 */
export type SameNullability<Value, Source> = Source extends null | undefined ? Source : Value
