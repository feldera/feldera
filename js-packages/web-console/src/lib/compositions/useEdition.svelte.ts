import { page } from '$app/state'

/**
 * Whether this Feldera instance runs the enterprise edition, where the API server, compiler
 * and runner are separate processes. In the open-source edition they are tasks within a
 * single process, which changes what a symptom such as missing monitoring data implies.
 */
export const useIsEnterprise = () => {
  const isEnterprise: boolean = $derived(
    !!page.data.feldera &&
      (page.data.feldera.edition.startsWith('Enterprise') ||
        page.data.feldera.edition.startsWith('Premium'))
  )
  return {
    get value() {
      return isEnterprise
    }
  }
}
