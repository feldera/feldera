import type { Snippet } from 'svelte'

type DialogAction = { name: string; callback: () => void | Promise<any>; 'data-testid'?: string }

export type GlobalDialogContent = {
  title: string
  description: string
  scrollableContent?: string
  onSuccess: DialogAction
}

let globalDialog = $state(null as null | Snippet)
let onclose: (() => void) | null = $state(null)

export const useGlobalDialog = () => {
  return {
    get dialog() {
      return globalDialog
    },
    set dialog(value: typeof globalDialog) {
      onclose = null
      globalDialog = value
    },
    get onclose() {
      return onclose
    },
    set onclose(value: typeof onclose) {
      onclose = value
    }
  }
}
