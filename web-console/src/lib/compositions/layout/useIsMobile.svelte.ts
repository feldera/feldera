import { MediaQuery } from 'svelte/reactivity'

export const useIsTablet = () => {
  return new MediaQuery('not (min-width: 1280px)')
}

export const useIsMobile = () => {
  return new MediaQuery('not (min-width: 640px)')
}

export const useIsScreenMd = () => {
  return new MediaQuery('min-width: 768px')
}

export const useIsScreenLg = () => {
  return new MediaQuery('min-width: 1024px')
}

export const useIsScreenXl = () => {
  return new MediaQuery('min-width: 1280px')
}
