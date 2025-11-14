<script lang="ts" module>
  export function deleteDialogProps<Args extends unknown[]>(
    actionName: string,
    itemName: string | ((...args: Args) => string),
    onAction: (...args: Args) => void | Promise<any>,
    description = 'Are you sure? This action is irreversible.',
    scrollableContent?: string
  ) {
    return (...args: Args) => ({
      actionName,
      itemName,
      onAction,
      description,
      scrollableContent,
      args
    })
  }
</script>

<script lang="ts" generics="Args extends unknown[]">
  import DangerDialog from './DangerDialog.svelte'

  let {
    actionName,
    itemName,
    onAction,
    description = 'Are you sure? This action is irreversible.',
    scrollableContent,
    args,
    onClose
  }: {
    actionName: string
    itemName: string | ((...args: Args) => string)
    onAction: (...args: Args) => void | Promise<any>
    description?: string
    scrollableContent?: string
    args: Args
    onClose: () => void
  } = $props()

  let content = $derived({
    title: typeof itemName === 'string' ? itemName : itemName(...args),
    description,
    scrollableContent,
    onSuccess: {
      name: actionName,
      callback: () => onAction(...args),
      'data-testid': 'button-confirm-delete'
    }
  })
</script>

<DangerDialog {content} {onClose}></DangerDialog>
