<script lang="ts">
  import type { Snippet } from 'svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import JsonForm from '$lib/components/dialogs/JSONForm.svelte'

  let {
    values,
    metadata,
    onApply,
    onClose,
    title,
    disabled
  }: {
    values: Record<string, string>
    metadata?: Record<string, { title?: string; editorClass?: string; filePath?: string }>
    onApply: (values: Record<string, string>) => Promise<void>
    onClose: () => void
    title: Snippet
    disabled?: boolean
  } = $props()
  let states = $state(values)
  let refs: Record<string, () => string> = $state({})
  const submitResults = async () => {
    let res: Record<string, string> = {}
    Object.entries(refs).forEach(([key, getData]) => (res[key] = getData()))
    console.log('aaaa', res)
    onApply(res).then(onClose)
  }
</script>

<GenericDialog onApply={submitResults} {onClose} {title}>
  {#each Object.keys(states) as key}
    <span class="font-normal">{metadata?.[key].title ?? ''}</span>
    <div class={metadata?.[key].editorClass}>
      <JsonForm filePath={metadata?.[key].filePath ?? key} json={states[key]} onSubmit={submitResults} bind:getData={refs[key]} {disabled}
      ></JsonForm>
    </div>
  {/each}
</GenericDialog>
