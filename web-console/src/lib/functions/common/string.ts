/**
 * Escape regular expression string.
 * @see https://stackoverflow.com/questions/3561493/is-there-a-regexp-escape-function-in-javascript
 * @param value
 * @returns
 */
export const escapeRegExp = (value: string) => {
  return value.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&')
}

/**
 * Check if a string is a valid URL using the Javascript URL class.
 * @see https://stackoverflow.com/questions/61634973/yup-validation-of-website-using-url-very-strict
 * @param url
 * @returns
 */
export const isUrl = (url: string | undefined) => {
  if (!url) return false
  try {
    new URL(url)
  } catch (e) {
    return false
  }

  return true
}

/**
 * Generate a random name, used for giving random names in certain cases.
 * @returns
 */
export const randomString = (): string =>
  new Date().getTime().toString(36) + Math.random().toString(36).slice(2)

/**
 * Remove a prefix in a string if it exists
 * @param value
 * @param prefix
 * @returns
 */
export const removePrefix = (value: string, prefix: string) =>
  value.startsWith(prefix) ? value.slice(prefix.length) : value

/**
 * Convert bytes (number of) to a human readable string
 * @example humanSize(1024) // "1 KiB"
 * @param bytes
 * @returns
 */
export function humanSize(bytes: number): string {
  const thresh = 1024
  if (Math.abs(bytes) < thresh) {
    return bytes + ' B'
  }
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB']
  let u = -1
  do {
    bytes /= thresh
    ++u
  } while (Math.abs(bytes) >= thresh && u < units.length - 1)

  return bytes.toFixed(1) + ' ' + units[u]
}

export function nthIndexOf(
  str: string,
  substring: string,
  n: number,
  position: number = 0
): number {
  if (n <= 0) return -1

  const index = str.indexOf(substring, position)

  if (index === -1) return -1
  if (n === 1) return index

  return nthIndexOf(str, substring, n - 1, index + 1)
}
