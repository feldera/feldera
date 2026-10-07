import { ServerDate } from '$lib/compositions/serverTime'
import { refreshConceptualHqDeviceId } from '$lib/compositions/useConceptualHq.svelte'
import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

/**
 * ConceptualHQ command queue. Before the loader script arrives, `window.ca`
 * enqueues each call as `[command, ...args]`; the loader replays the queue once
 * ready. Calls use the queue form, `ca('identify', ...)` / `ca('track', ...)`,
 * which is what the async stub supports (the `ca.identify(...)` method form only
 * exists after the loader has replaced the stub).
 */
type ConceptualAnalytics = ((command: string, ...args: unknown[]) => void) & {
  q: unknown[][]
  l?: number
  getDeviceId?: () => string | null | undefined
}

declare global {
  interface Window {
    ConceptualAnalytics?: string
    ca?: ConceptualAnalytics
  }
}

// Feldera's ConceptualHQ instance. The key is the per-deployment variable and
// travels through `/config`; the host and loader version are fixed here, the
// same way the PostHog `api_host` is hardcoded.
const LOADER_BASE = 'https://oqiset.feldera.com/analytics/loader-v1.js'
const LOADER_VERSION = '1.1.0'

// A user record younger than this is a new signup. The window is long, so it
// also covers a user who closes the tab before the loader sends the event and
// returns later.
const SIGNUP_WINDOW_MS = 6 * 60 * 60 * 1000
// One marker per user, so that a second account in the same browser also
// sends its own `signup`.
const SIGNUP_MARKER_PREFIX = 'conceptualhq_signup:'

let initialized = false

// Keyed on email to match PostHog identity.
const conceptualHqUserId = (profile: UserProfile) => profile.email || profile.id

/**
 * Install the `window.ca` command queue and inject the ConceptualHQ loader.
 * Mirrors the vendor snippet: define the queue stub, stamp the load time, then
 * append the loader script keyed by the deployment's analytics key. Returns the
 * `ca` handle so callers enqueue without re-reading the global.
 */
const loadConceptualAnalytics = (key: string): ConceptualAnalytics => {
  window.ConceptualAnalytics = 'ca'
  let ca = window.ca
  if (!ca) {
    const queue: unknown[][] = []
    ca = Object.assign((...args: unknown[]) => queue.push(args), { q: queue })
    window.ca = ca
  }
  ca.l = Date.now()

  const script = document.createElement('script')
  script.async = true
  script.src = `${LOADER_BASE}?key=${encodeURIComponent(key)}&v=${LOADER_VERSION}`
  // The loader installs the full `ca` API, and with it the visitor ID. It does
  // so before `onload` fires: replacing the stub and draining
  // `ca.q` are synchronous steps of the script, so `getDeviceId` is in place by
  // the time we read it.
  // Potential for regression: were the loaded script to defer that work, a first-time
  // visitor would keep the empty ID until the next page load.
  script.onload = refreshConceptualHqDeviceId
  const firstScript = document.getElementsByTagName('script')[0]
  firstScript.parentNode?.insertBefore(script, firstScript)

  return ca
}

/**
 * Initialize ConceptualHQ analytics for the signed-in user.
 *
 * Identifies the user (keyed on email to match PostHog identity).
 * `reportLogin` in `analytics.ts` sends the login events.
 *
 * Idempotent: repeated calls (warm-cache reconcile, re-navigation) are ignored
 * after the first success. No-op when the key is empty or outside the browser.
 */
export const initConceptualHq = (config: Configuration, profile: UserProfile) => {
  if (initialized || !config.conceptualhq || typeof window === 'undefined') {
    return
  }
  initialized = true

  const ca = loadConceptualAnalytics(config.conceptualhq)

  const userId = conceptualHqUserId(profile)
  if (userId) {
    ca('identify', userId, {
      email: profile.email ?? undefined,
      name: profile.name ?? undefined
    })
  }
}

/**
 * Track `signup` once per user and browser, for a user that the
 * backend created in the last `SIGNUP_WINDOW_MS`. Give only a `user_created_at`
 * from a new fetch, because the cached session can be from the previous user of
 * this browser.
 *
 * No-op when the loader is not installed (analytics is off) or outside the
 * browser.
 */
export const trackConceptualHqSignup = (
  profile: UserProfile,
  userCreatedAt: string | null | undefined,
  now = ServerDate.now()
) => {
  if (typeof window === 'undefined' || !window.ca || !userCreatedAt) {
    return
  }
  const userId = conceptualHqUserId(profile)
  // A small negative age is clock skew between the server and the browser.
  const age = now - Date.parse(userCreatedAt)
  if (!userId || !(age < SIGNUP_WINDOW_MS)) {
    return
  }
  const marker = SIGNUP_MARKER_PREFIX + userId
  try {
    if (localStorage.getItem(marker)) {
      return
    }
    localStorage.setItem(marker, new Date(now).toISOString())
  } catch {
    // Storage is blocked. Send the event. ConceptualHQ can drop a repeat by its
    // `dedupe_id`.
  }
  // The host tells the deployments apart, e.g. `try.feldera.com` for the sandbox.
  const host = window.location.hostname
  window.ca('track', 'signup', { host, dedupe_id: `signup:${host}:${userId}` })
}

/**
 * Track an event in ConceptualHQ. No-op when the loader was never installed
 * (analytics disabled) or outside the browser, so callers fire unconditionally.
 * Prefer the shared `captureEvent` in `analytics.ts` over calling this directly,
 * so PostHog and ConceptualHQ stay in sync.
 */
export const trackConceptualHq = (event: string, properties?: Record<string, unknown>) => {
  if (typeof window === 'undefined' || !window.ca) {
    return
  }
  if (properties) {
    window.ca('track', event, properties)
  } else {
    window.ca('track', event)
  }
}
