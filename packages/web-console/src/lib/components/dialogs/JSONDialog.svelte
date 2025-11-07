<script lang="ts">
  import type { Snippet } from 'svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import JsonForm from '$lib/components/dialogs/JSONForm.svelte'

  let {
    value,
    filePath,
    onApply,
    onClose,
    title,
    readOnlyMessage,
    disabled,
    refreshOnChange = true
  }: {
    value: string
    filePath: string
    onApply: (json: string) => Promise<void>
    onClose: () => void
    title: Snippet
    readOnlyMessage?: { value: string }
    disabled?: boolean
    refreshOnChange?: boolean
  } = $props()

  let current = $state(value)
  {
    let original = $state(refreshOnChange ? value : undefined)
    $effect(() => {
      if (original === undefined) {
        return
      }
      value
      $effect.root(() => {
        if (original !== value) {
          current = original = value
        }
      })
    })
  }

  $effect(() => {
    current = value
  })

  const submitHandler = async () => {
    onApply(current).then(onClose, () => {})
  }
</script>

<GenericDialog onApply={submitHandler} {onClose} {title} {disabled} confirmLabel="Apply">
  <div class="h-96">
    <JsonForm {filePath} onSubmit={onApply} bind:value={current} {disabled} {readOnlyMessage}
    ></JsonForm>
  </div>
</GenericDialog>
