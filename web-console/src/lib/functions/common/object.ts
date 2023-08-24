/**
 * Check if a value is an empty object
 * @param obj
 * @returns
 */
export const isEmptyObject = (obj: unknown): obj is Record<never, never> =>
  typeof obj === 'object' && obj !== null && Object.keys(obj).length === 0
