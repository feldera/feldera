import { listen } from 'svelte-mq-store'
import { MediaQuery, Store } from 'runed'

export const useIsMobile = () => {
  return new Store(listen('(max-width: 1200px)'))
  // const isMobile = new MediaQuery('(max-width: 1200px)')
}
