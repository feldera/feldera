<script lang="ts" module>
  export function deleteDialogProps<Args extends unknown[]>(
    actionName: string,
    itemName: string | ((...args: Args) => string),
    onAction: (...args: Args) => void | Promise<any>,
    description?: string,
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
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'

  import GenericDialog from './GenericDialog.svelte'

  let {
    actionName,
    itemName,
    onAction,
    description = 'Are you sure? This action is irreversible.',
    scrollableContent,
    args
  }: {
    actionName: string
    itemName: string | ((...args: Args) => string)
    onAction: (...args: Args) => void | Promise<any>
    description?: string
    scrollableContent?: string
    args: Args
  } = $props()

  let globalDialog = useGlobalDialog()

  let content = $derived({
    title: typeof itemName === 'string' ? itemName : itemName(...args),
    description,
    scrollableContent,
    onSuccess: {
      name: actionName,
      callback: async () => {
        await onAction(...args)
        globalDialog.dialog = null
      },
      'data-testid': 'button-confirm-delete'
    }
  })
</script>

<GenericDialog {content} danger></GenericDialog>
