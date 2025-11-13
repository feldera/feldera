/**
 * Composition hook to determine whether the app is served over HTTP or HTTPS
 */

export type Protocol = 'http' | 'https'

/**
 * Returns the protocol (http or https) used to serve the application
 * @returns The current protocol as 'http' or 'https'
 */
export const useProtocol = (): Protocol => {
  if (typeof window === 'undefined') {
    // SSR context - default to https
    return 'https'
  }

  return window.location.protocol === 'https:' ? 'https' : 'http'
}

/**
 * Returns whether the app is served over HTTPS
 * @returns true if served over HTTPS, false otherwise
 */
export const isSecure = (): boolean => {
  return useProtocol() === 'https'
}

/**
 * Returns whether the app is served over HTTP (not HTTPS)
 * @returns true if served over HTTP, false otherwise
 */
export const isInsecure = (): boolean => {
  return useProtocol() === 'http'
}
