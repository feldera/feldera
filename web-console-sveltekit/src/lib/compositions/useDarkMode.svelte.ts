import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export let useDarkMode = () => {
  let mode = useLocalStorage<'dark' | 'light'>('darkMode', 'light')
  return {
    get darkMode() {
      return mode
    },
    toggleDarkMode() {
      // window.document.body.classList.toggle('dark')
      mode.value = mode.value === 'dark' ? 'light' : 'dark'
    }
  }
}
