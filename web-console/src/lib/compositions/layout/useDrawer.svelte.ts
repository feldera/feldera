import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { useIsTablet } from './useIsMobile.svelte'

export const useDrawer = () => {
  const isTablet = useIsTablet()

  const showDrawer = useLocalStorage('layout/drawer', !isTablet)
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
