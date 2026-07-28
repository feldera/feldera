<script lang="ts">
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import JsonForm from '$lib/components/dialogs/JSONForm.svelte'

  const {
    value,
    filePath,
    onApply,
    title,
    readOnlyMessage,
    disabled,
    disabledMessage,
    refreshOnChange = true
  }: {
    value: string
    filePath: string
    onApply?: (json: string) => Promise<void>
    title: string
    readOnlyMessage?: { value: string }
    disabled?: boolean
    disabledMessage?: string
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
    onApply?.(current)
  }
</script>

<GenericDialog
  content={{
    title,
    onSuccess: onApply ? { name: 'Apply', callback: submitHandler, disabledMessage } : undefined,
    onCancel: onApply ? undefined : { name: 'Close' }
  }}
  {disabled}
>
  <div class="h-96">
    <JsonForm
      {filePath}
      onSubmit={onApply}
      bind:value={current}
      disabled={disabled || !onApply}
      {readOnlyMessage}
    ></JsonForm>
  </div>
</GenericDialog>
