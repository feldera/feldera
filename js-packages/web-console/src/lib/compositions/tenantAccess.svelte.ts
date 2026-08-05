/** The `error_code` an SDK rejection carries, either directly or as its cause. */
export const errorCodeOf = (reason: unknown): string | undefined => {
  const cause = (reason as { cause?: { error_code?: string } } | undefined)?.cause
  return cause?.error_code ?? (reason as { error_code?: string } | undefined)?.error_code
}

let lost = $state(false)

/**
 * Whether the server has told us this session holds no tenant membership at
 * all, which happens when an administrator removes the last one while the user
 * is working.
 *
 * Nothing the client holds can recover from that: there is no stale selection
 * to drop and a reload lands in the same place, so the console has to stop
 * asking and show the no-access screen. The authenticated layout folds this
 * into its gate, which also parks the pollers; without it they keep polling on
 * layout data that still says a tenant is resolved, and the user watches every
 * request fail until they happen to refresh.
 */
export const tenantAccessLost = {
  get current() {
    return lost
  },
  /** Called from the global error interceptor. */
  mark() {
    lost = true
  },
  /** Called when a config fetch succeeds, so regained access clears the gate. */
  reset() {
    lost = false
  }
}
