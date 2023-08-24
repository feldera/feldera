/**
 *
 * @param value
 * @returns
 */
export function nonNull<T>(value: T | null | undefined): value is NonNullable<T> {
  return value !== null && value !== undefined
}
