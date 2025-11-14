import type { Snippet } from '$lib/types/svelte'

let drawerContent: Snippet | null = $state(null)

export const useContextDrawer = () => {
  return {
    get content() {
      return drawerContent
    },
    set content(value: typeof drawerContent) {
      drawerContent = value
    }
  }
}
