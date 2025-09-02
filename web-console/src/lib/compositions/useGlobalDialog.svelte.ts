import type { Snippet } from 'svelte'
import DangerDialog from '$lib/components/dialogs/DangerDialog.svelte'

type DialogAction = { name: string; callback: () => void | Promise<any>; 'data-testid'?: string }

export type GlobalDialogContent = {
  title: string
  description: string
  scrollableContent?: string
  onSuccess: DialogAction
}

let globalDialogState = $state(null as null | Snippet)

export const useGlobalDialog = () => {
  return {
    get dialog() {
      return globalDialogState
    },
    set dialog(value: typeof globalDialogState) {
      globalDialogState = value
    }
  }
}
