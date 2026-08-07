import { clearConfigCaches } from '$lib/compositions/configCache'
import { resolve } from '$lib/functions/svelte'
import { setSelectedTenant } from '$lib/services/auth'

/**
 * Persist the tenant selection (by tenant id) and restart the app.
 *
 * A full page load rather than `invalidateAll()`: layout components persist
 * across invalidation, so module state and pollers initialized for the old
 * tenant would carry over. A fresh page start re-fetches /config/session under
 * the new `Feldera-Tenant` header from a clean slate.
 *
 * `to` names where to restart, for the tenant page: a user who deep-linked into
 * a page should land there once a tenant is chosen. The header switcher omits it
 * and restarts on the home page, because the current page is tenant-scoped and
 * may not exist in the tenant switched to.
 */
export const switchTenant = (tenantId: string, options?: { to?: string }) => {
  setSelectedTenant(tenantId)
  clearConfigCaches()
  window.location.assign(options?.to ?? resolve('/'))
}
