import { type ProductFruitsUserObject, productFruits } from 'product-fruits'

import type { Configuration } from '$lib/services/manager'
import type { UserProfile } from '$lib/types/auth'

/**
 * User interface language passed to Product Fruits. The console ships English
 * only, so this is fixed until localization exists.
 */
const language = 'en'

let initialized = false

/**
 * Map an authenticated user's profile to the identity Product Fruits expects.
 * `username` is the sole required field; we key it on the email that PostHog
 * also identifies on so onboarding progress stays tied to one person, and fall
 * back to the user id when no email is present.
 */
export const toProductFruitsUser = (profile: UserProfile): ProductFruitsUserObject => ({
  username: profile.email || profile.id || 'anonymous',
  email: profile.email ?? undefined,
  firstname: profile.name ?? undefined
})

/**
 * Initialize Product Fruits for the signed-in user.
 *
 * Idempotent: repeated calls (warm-cache reconcile, re-navigation) are ignored
 * after the first success, so it is safe to call from
 * `initializeConfigDependencies`. No-op when the workspace code is empty or when
 * running outside the browser.
 */
export const initProductFruits = (config: Configuration, profile: UserProfile) => {
  if (initialized || !config.product_fruits || typeof window === 'undefined') {
    return
  }
  initialized = true
  productFruits.init(config.product_fruits, language, toProductFruitsUser(profile))
}
