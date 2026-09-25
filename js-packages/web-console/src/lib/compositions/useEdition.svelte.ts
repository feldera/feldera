import { page } from '$app/state'

/** Whether this Feldera instance runs an enterprise edition (Enterprise or EnterpriseDev). */
export const useIsEnterprise = () => {
  const isEnterprise: boolean = $derived(
    !!page.data.feldera && page.data.feldera.edition.startsWith('Enterprise')
  )
  return {
    get value() {
      return isEnterprise
    }
  }
}
