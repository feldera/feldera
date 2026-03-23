import type { Snippet } from '$lib/types/svelte'

export type DialogAction = {
  name: string
  callback: () => void | Promise<any>
  'data-testid'?: string
  disabledMessage?: string
}

export type GlobalDialogContent = {
  title: string
  description?: string
  scrollableContent?: string
  /**
   * This callback is run when the primary action button is pressed.
   * The dialog does not close automatically.
   * If omitted, the action buttons are not shown.
   */
  onSuccess?: DialogAction
  /**
   * Called when the user clicks the cancel button, the "X" button, or clicks away from the dialog.
   * If `name` is provided, it overrides the default "Cancel" button label.
   */
  onCancel?: Partial<DialogAction>
}

let globalDialog = $state(null as null | Snippet)
let globalOnClickAway = $state(null as null | (() => void))

export const useGlobalDialog = () => {
  return {
    get dialog() {
      return globalDialog
    },
    set dialog(value: typeof globalDialog) {
      globalDialog = value
      if (!value) globalOnClickAway = null
    },
    get onClickAway() {
      return globalOnClickAway
    },
    set onClickAway(value: typeof globalOnClickAway) {
      globalOnClickAway = value
    }
  }
}
