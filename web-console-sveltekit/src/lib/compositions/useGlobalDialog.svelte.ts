import type { Snippet } from "svelte";
import DangerDialog from '$lib/components/dialogs/DangerDialog.svelte'

type DialogAction = { name: string; callback: () => void; 'data-testid'?: string }

export type GlobalDialogContent = { title: string; description: string; onSuccess: DialogAction }

let globalDialogState = $state(null as null | Snippet)

export const useGlobalDialog = () =>
  //   props: {
  //   content: null | GlobalDialogContent
  //   showDialog: (content: GlobalDialogContent) => void
  //   closeDialog: () => void
  // }
  {
    return {
      get dialog() {
        return globalDialogState
      },
      set dialog(value: typeof globalDialogState) {
        globalDialogState = value
      }
    }
  }
