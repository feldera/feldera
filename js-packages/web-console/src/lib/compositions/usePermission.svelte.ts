import { hasPermissions, type Permission } from '#lib/services/rbac.ts'
import { page } from '$app/state'

// Reactive single-permission check for gates that feed a boolean into existing
// plumbing rather than wrapping markup, e.g. Monaco's `editDisabled`. Reads the
// permission list materialized into `page.data.feldera` at init (see
// +layout.ts). For markup gates prefer the `<RBAC>` wrapper.
export const usePermission = (permission: Permission) => {
  const allowed = $derived(hasPermissions(page.data.feldera, permission))
  return {
    get allowed() {
      return allowed
    }
  }
}
