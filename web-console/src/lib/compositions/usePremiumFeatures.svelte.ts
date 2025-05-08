import { page } from '$app/state'

export const usePremiumFeatures = () => {
  let isAdvanced: boolean = $derived(
    page.data.feldera.edition.startsWith('Enterprise') ||
      page.data.feldera.edition.startsWith('Premium')
  )
  return {
    get value() {
      return isAdvanced
    }
  }
}
