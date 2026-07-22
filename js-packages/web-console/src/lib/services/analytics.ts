import posthog from 'posthog-js'

import { trackConceptualHq } from '$lib/services/conceptualHq'

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
