<script lang="ts">
  import type { Snippet } from 'svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import JsonForm from '$lib/components/dialogs/JSONForm.svelte'

  let {
    json,
    filePath,
    onApply,
    onClose,
    title,
    disabled
  }: {
    json: string
    filePath: string
    onApply: (json: string) => Promise<void>
    onClose: () => void
    title: Snippet
    disabled?: boolean
  } = $props()
  let value = $state(json)
  $effect(() => {
    value = json
  })
  let onsubmit: () => void = $state(() => {})
</script>

<GenericDialog onApply={onsubmit} {onClose} {title}>
  <div class="h-96">
    <JsonForm {json} {filePath} onSubmit={onApply} {disabled}></JsonForm>
  </div>
</GenericDialog>
