import { invalidateAll } from '$app/navigation'

/** The `error_code` an SDK rejection carries, either directly or as its cause. */
export const errorCodeOf = (reason: unknown): string | undefined => {
  const cause = (reason as { cause?: { error_code?: string } } | undefined)?.cause
  return cause?.error_code ?? (reason as { error_code?: string } | undefined)?.error_code
}

let recheckPending = false

/**
 * Re-run the loaders because the server says this session holds no tenant
 * membership at all, which happens when an administrator removes the last one
 * while the user is working. The root `load()` then resolves no acting tenant,
 * and the `(authorized)` group's gate redirects to the tenant page.
 *
 * Called from the global error interceptor, so a single revocation can be
 * reported by many failing requests: the latch keeps that to one re-run. It
 * also stops the re-run from re-entering itself, since `load()` fetches
 * `/config`, which fails the same way while no tenant resolves.
 */
export const requestTenantRecheck = () => {
  if (recheckPending) {
    return
  }
  recheckPending = true
  invalidateAll()
}

/**
 * Whether a re-check is in flight. The root `load()` consults this to bypass its
 * warm config cache: that cache still describes the tenant the session just lost,
 * and rendering from it would leave the app up with every request failing.
 */
export const isTenantRecheckPending = () => recheckPending

/** Called when a config fetch succeeds, so regained access re-arms the latch. */
export const resetTenantRecheck = () => {
  recheckPending = false
}
