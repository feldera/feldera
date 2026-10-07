import posthog from 'posthog-js'

import { trackConceptualHq, trackConceptualHqSignup } from '$lib/services/conceptualHq'
import type { UserProfile } from '$lib/types/auth'

// Holds the `auth_time` of the last reported login, one key per user.
const SIGNIN_MARKER_PREFIX = 'analytics_signin:'

let loginReported = false

/**
 * Report a product-analytics event to every configured backend (PostHog and
 * ConceptualHQ). Each backend no-ops when its integration is disabled, so
 * callers fire unconditionally. Use snake_case event names and property keys for consistency
 * across both tools.
 */
export const captureEvent = (event: string, properties?: Record<string, unknown>) => {
  posthog.capture(event, properties)
  trackConceptualHq(event, properties)
}

/**
 * Report a login once per page load, in this order:
 * 1. `signup` to ConceptualHQ, if the backend created the user recently.
 * 2. `signin` to all backends, once per authentication at the identity provider.
 *    A reload or a token refresh keeps the `auth_time` claim, so the function
 *    does not send `signin` again.
 *
 * Give only a `userCreatedAt` from a new fetch, see `trackConceptualHqSignup`.
 * If the provider does not send `auth_time`, the function sends `signin` on
 * each page load.
 */
export const reportLogin = (
  profile: UserProfile,
  authTime: number | undefined,
  userCreatedAt: string | null | undefined
) => {
  if (loginReported) {
    return
  }
  loginReported = true
  trackConceptualHqSignup(profile, userCreatedAt)
  if (isNewLogin(profile, authTime)) {
    captureEvent('signin')
  }
}

const isNewLogin = (profile: UserProfile, authTime: number | undefined) => {
  const userId = profile.email || profile.id
  if (authTime === undefined || !userId) {
    return true
  }
  const marker = SIGNIN_MARKER_PREFIX + userId
  try {
    if (localStorage.getItem(marker) === String(authTime)) {
      return false
    }
    localStorage.setItem(marker, String(authTime))
  } catch {
    // Storage is blocked. Send `signin` on each page load.
  }
  return true
}
