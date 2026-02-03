import { page } from '$app/state'

export const usePremiumFeatures = () => {
  const isAdvanced: boolean = $derived(
    !!page.data.feldera &&
      (page.data.feldera.edition.startsWith('Enterprise') ||
        page.data.feldera.edition.startsWith('Premium'))
  )
  return {
    get value() {
      return isAdvanced
    }
  }
}
