<script lang="ts">
  import { onDestroy } from 'svelte'
  import GenericDialog from '#lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '#lib/compositions/layout/useGlobalDialog.svelte.ts'

  let {
    fileNames,
    onResolve
  }: {
    /** The files that the link sets to non-empty code. */
    fileNames: string[]
    /** Called once, with `true` if the user confirms the save and `false` otherwise. */
    onResolve: (isConfirmed: boolean) => void
  } = $props()

  const globalDialog = useGlobalDialog()
  let isResolved = false
  const settle = (isConfirmed: boolean) => {
    if (isResolved) {
      return
    }
    isResolved = true
    onResolve(isConfirmed)
  }
  // Cancel the save if the dialog closes without an answer, e.g. when another
  // dialog replaces it.
  onDestroy(() => settle(false))
</script>

<GenericDialog
  danger
  content={{
    title: 'Compile code from a link?',
    description:
      `This code came from the link you clicked. The link filled in ${fileNames.join(', ')}.\n\n` +
      'Saving compiles the pipeline on the server, and any Rust code in it runs there.\n' +
      'Compile only if you believe the code is safe.',
    onSuccess: {
      name: 'Compile',
      callback: () => {
        settle(true)
        globalDialog.dialog = null
      },
      'data-testid': 'btn-dialog-save-pasted-link'
    },
    onCancel: {
      callback: () => settle(false)
    }
  }}
/>
