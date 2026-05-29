<script lang="ts">
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'

  const { onConfirm }: { onConfirm: () => void } = $props()

  const globalDialog = useGlobalDialog()
</script>

<GenericDialog
  content={{
    title: 'Create a checkpoint?',
    description: 'This may delete the oldest checkpoint and briefly increase pipeline load.',
    onSuccess: {
      name: 'Checkpoint',
      'data-testid': 'btn-confirm-checkpoint',
      callback: () => {
        globalDialog.dialog = null
        onConfirm()
      }
    }
  }}
  swapActions
></GenericDialog>
