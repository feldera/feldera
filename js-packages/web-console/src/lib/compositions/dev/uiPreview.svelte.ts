// Dev-only switches for previewing UI states that are awkward to reach against a real
// backend. They are read only when `import.meta.env.DEV` is set, so a build ignores them.
import { useLocalStorage } from '$lib/compositions/localStore.svelte'

export const useUiPreview = () => {
  const noPipelines = useLocalStorage('dev/uiPreview/noPipelines', false)
  return {
    /** Show every pipeline list as empty, as for a new user. */
    get noPipelines() {
      return import.meta.env.DEV && noPipelines.value
    },
    set noPipelines(value: boolean) {
      noPipelines.value = value
    }
  }
}
