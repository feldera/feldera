import { useLocalStorage } from '$lib/compositions/localStore.svelte'
import { useIsMobile } from './useIsMobile.svelte'

export const useDrawer = () => {
  const isMobile = useIsMobile()

  const showDrawer = useLocalStorage('layout/drawer', !isMobile)
  return {
    get value() {
      return showDrawer.value
    },
    set value(show: boolean) {
      showDrawer.value = show
    },
    get isMobileDrawer() {
      return isMobile.current
    }
  }
}
