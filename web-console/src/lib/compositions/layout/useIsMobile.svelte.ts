import { listen } from 'svelte-mq-store'
import { MediaQuery, Store } from 'runed'

export const useIsTablet = () => {
  return new Store(listen('(max-width: 1280px)'))
  // const isTablet = new MediaQuery('(max-width: 1280px)')
}

export const useIsMobile = () => {
  return new Store(listen('(max-width: 640px)'))
  // const isMobile = new MediaQuery('(max-width: 640px)')
}
