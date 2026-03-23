<script lang="ts">
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import JsonForm from '$lib/components/dialogs/JSONForm.svelte'

  const {
    values,
    metadata,
    onApply,
    title,
    disabled,
    disabledMessage,
    refreshOnChange = true
  }: {
    values: Record<string, string>
    metadata?: Record<
      string,
      { title?: string; editorClass?: string; filePath?: string; readOnlyMessage?: string }
    >
    onApply: (values: Record<string, string>) => Promise<void>
    title: string
    disabled?: boolean
    disabledMessage?: string
    refreshOnChange?: boolean
  } = $props()

  let current = $state(values)
  {
    let original = $state(refreshOnChange ? values : undefined)
    $effect(() => {
      if (original === undefined) {
        return
      }
      values
      $effect.root(() => {
        Object.keys(values).forEach((key) => {
          if (original[key] !== values[key]) {
            current[key] = original[key] = values[key]
          }
        })
      })
    })
  }

  const submitResults = async () => {
    onApply(current)
  }
</script>

<GenericDialog
  content={{ title, onSuccess: { name: 'Apply', callback: submitResults, disabledMessage } }}
  {disabled}
>
  {#each Object.keys(current) as key}
    <span class="font-normal">{metadata?.[key].title ?? ''}</span>
    <div class={metadata?.[key].editorClass}>
      <JsonForm
        filePath={metadata?.[key].filePath ?? key}
        onSubmit={submitResults}
        bind:value={current[key]}
        readOnlyMessage={metadata?.[key]?.readOnlyMessage
          ? { value: metadata[key].readOnlyMessage }
          : undefined}
        {disabled}
      ></JsonForm>
    </div>
  {/each}
</GenericDialog>
