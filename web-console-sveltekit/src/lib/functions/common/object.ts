/**
 * Check if a value is an empty object
 * @param obj
 * @returns
 */
export const isEmptyObject = (obj: unknown): obj is Record<never, never> =>
  typeof obj === 'object' && obj !== null && Object.keys(obj).length === 0

/**
 * Removes undefined fields of an object
 * Mutates the object
 * @see https://stackoverflow.com/questions/25421233/javascript-removing-undefined-fields-from-an-object
 * @param obj
 * @returns
 */
export function pruneObj<T extends Record<string | number | symbol, unknown>>(obj: T) {
  for (const prop in obj) {
    if (obj.hasOwnProperty(prop) && obj[prop] === undefined) {
      delete obj[prop]
    }
  }
  return obj
}

export const getField = (path: string, object: any) =>
  path.split('.').reduce((acc, field) => (acc instanceof Object ? acc[field] : acc), object)
