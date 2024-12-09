import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export let useDarkMode = () => {
  let mode = useLocalStorage<'dark' | 'light'>('darkMode', 'light')
  return {
    get current() {
      return mode.value
    },
    set current(value: 'dark' | 'light') {
      mode.value = value
    },
    toggle() {
      // window.document.body.classList.toggle('dark')
      mode.value = mode.value === 'dark' ? 'light' : 'dark'
    }
  }
}
