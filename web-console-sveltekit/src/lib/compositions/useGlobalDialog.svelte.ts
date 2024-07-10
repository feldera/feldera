type DialogAction = { name: string; callback: () => void; 'data-testid'?: string }

type GlobalDialogContent = { title: string; description: string; onSuccess: DialogAction }

let globalDialogState = $state(null as null | GlobalDialogContent)

export const useGlobalDialog = () =>
  //   props: {
  //   content: null | GlobalDialogContent
  //   showDialog: (content: GlobalDialogContent) => void
  //   closeDialog: () => void
  // }
  {
    return {
      get content() {
        return globalDialogState
      },
      set content(value: typeof globalDialogState) {
        globalDialogState = value
      }
    }
  }

export const useDeleteDialog = () => {
  let p = useGlobalDialog()
  return {
    get content() {
      return p.content
    },
    showDeleteDialog:
      <Args extends unknown[]>(
        actionName: string,
        itemName: string | ((...args: Args) => string),
        onAction: (...args: Args) => void,
        description = 'Are you sure? This action is irreversible.'
      ) =>
      (...args: Args) => {
        // showDialog({
        p.content = {
          title:
            actionName + ' ' + (typeof itemName === 'string' ? itemName : itemName(...args)) + '?',
          description,
          onSuccess: {
            name: actionName,
            callback: () => onAction(...args),
            'data-testid': 'button-confirm-delete'
          }
        }
      },
    closeDialog() {
      p.content = null
    }
  }
}
