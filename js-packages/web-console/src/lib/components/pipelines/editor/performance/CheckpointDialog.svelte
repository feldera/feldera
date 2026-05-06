<script lang="ts">
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'

  const { onConfirm }: { onConfirm: () => void } = $props()

  const globalDialog = useGlobalDialog()
</script>

<GenericDialog
  content={{
    title: 'Checkpoint the current pipeline state',
    description:
      'Create the checkpoint? This may delete the oldest checkpoint, and will temporarily increase load on the pipeline.',
    onSuccess: {
      name: 'Checkpoint',
      'data-testid': 'btn-confirm-checkpoint',
      callback: () => {
        globalDialog.dialog = null
        onConfirm()
      }
    }
  }}
></GenericDialog>
