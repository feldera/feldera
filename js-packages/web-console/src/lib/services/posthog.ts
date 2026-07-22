import posthog from 'posthog-js'

import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

let initialized = false

/**
 * Initialize PostHog for the signed-in user: start the SDK, identify the user,
 * and report a `signin` event. The telemetry key is served by the
 * pipeline-manager in `/config` (`config.posthog`), so deployments configure it
 * without rebuilding the console.
 *
 * Mirrors `initConceptualHq`: idempotent via its own `initialized` guard, so the
 * warm-cache reconcile and `invalidateAll()` re-runs report `signin` once per
 * session. No-op when the key is empty or outside the browser.
 */
export const initPosthog = (config: Configuration, profile: UserProfile) => {
  if (initialized || !config.posthog || typeof window === 'undefined') {
    return
  }
  initialized = true

  posthog.init(config.posthog, {
    api_host: 'https://us.i.posthog.com',
    person_profiles: 'identified_only',
    capture_pageview: false,
    capture_pageleave: false
  })

  if (profile.email) {
    posthog.identify(profile.email, {
      email: profile.email,
      name: profile.name,
      auth_id: profile.id
    })
  }

  posthog.capture('signin')
}
