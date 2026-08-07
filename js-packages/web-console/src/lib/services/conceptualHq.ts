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

let initialized = false

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
 * Identifies the user (keyed on email to match PostHog identity) and tracks a
 * `signin` event.
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

  const userId = profile.email || profile.id
  if (userId) {
    ca('identify', userId, {
      email: profile.email ?? undefined,
      name: profile.name ?? undefined
    })
  }
  ca('track', 'signin')
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
