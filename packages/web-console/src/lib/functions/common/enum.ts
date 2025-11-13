/**
 * Uses reverse enum mapping TypeScript feature
 * Only available for source Enums with numeric literal values
 * @see https://stackoverflow.com/a/55214563
 * @param From
 * @param To
 * @returns
 */
export function convertEnum<
  typeofFrom extends Record<string | number | symbol, number>,
  typeofTo extends Record<string | number | symbol, string | number | symbol>
>(From: typeofFrom, To: typeofTo) {
  return (from: typeofFrom[keyof typeofFrom]) => {
    return To[From[from] as keyof typeof To]
  }
}
