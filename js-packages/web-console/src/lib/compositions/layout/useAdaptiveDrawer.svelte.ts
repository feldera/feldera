import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { useIsTablet } from './useIsMobile.svelte'

export const useAdaptiveDrawer = (type: 'left' | 'right') => {
  const isTablet = useIsTablet()

  const showDrawer = useLocalStorage(`layout/drawer/${type}`, !isTablet)

  $effect(() => {
    if (!isTablet.current) {
      showDrawer.value = false
    }
  })

  return {
    get value() {
      return showDrawer.value
    },
    set value(show: boolean) {
      showDrawer.value = show
    },
    get isMobileDrawer() {
      return isTablet.current
    }
  }
}
