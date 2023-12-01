import { create } from 'zustand'

type DialogAction = { name: string; callback: () => void; 'data-testid'?: string }

type GlobalDialogContent = { title: string; description: string; onSuccess: DialogAction }

const useGlobalDialog = create<{
  content: null | GlobalDialogContent
  showDialog: (content: GlobalDialogContent) => void
  closeDialog: () => void
}>()(set => ({
  content: null,
  showDialog: content => set({ content }),
  closeDialog: () => set({ content: null })
}))

export const useDeleteDialog = () => {
  const content = useGlobalDialog(d => d.content)
  const showDialog = useGlobalDialog(d => d.showDialog)
  const closeDialog = useGlobalDialog(d => d.closeDialog)
  return {
    content,
    showDeleteDialog:
      <Args extends unknown[]>(
        actionName: string,
        itemName: string | ((...args: Args) => string),
        onAction: (...args: Args) => void,
        description = 'Are you sure? This action is irreversible.'
      ) =>
      (...args: Args) =>
        showDialog({
          title: actionName + ' ' + (typeof itemName === 'string' ? itemName : itemName(...args)) + '?',
          description,
          onSuccess: {
            name: actionName,
            callback: () => onAction(...args),
            'data-testid': 'button-confirm-delete'
          }
        }),
    closeDialog
  }
}
