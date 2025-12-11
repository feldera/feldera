<script lang="ts">
  import { Control, Field, FieldErrors, Label } from 'formsnap'
  import { setError, superForm } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'

  import * as va from 'valibot'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import ClipboardCopyButton from '$lib/components/other/ClipboardCopyButton.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'

  const { onSubmit, onSuccess }: { onSubmit?: () => void; onSuccess?: () => void } = $props()

  const schema = va.object({
    name: va.pipe(va.string(), va.minLength(1, 'Specify API key name'))
  })
  const api = usePipelineManager()
  const form = superForm(
    { name: '' },
    {
      SPA: true,
      validators: valibot(schema),
      onUpdate({ form, ...rest }) {
        if (!form.valid) {
          return
        }
        onSubmit?.()
        api.postApiKey(form.data.name).then(
          (response) => {
            lastGenerated.push({ name: response.name, key: response.api_key })
            onSuccess?.()
          },
          (e) => {
            if ('message' in e) {
              setError(form, 'name', e.message)
            }
          }
        )
      }
    }
  )
  const { form: formData, enhance, submit } = form
  let lastGenerated = $state<{ name: string; key: string }[]>([])
</script>

<div class="flex flex-col gap-4 p-4">
  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
  <form
    class="flex flex-col gap-2"
    use:enhance
    onkeydown={(event) => {
      if (event.key === 'Enter') {
        event.preventDefault()
        submit()
      }
    }}
  >
    <Field {form} name="name">
      <Control>
        {#snippet children(attrs)}
          <Label>Generate new</Label>
          <div class="flex items-center gap-4">
            <!-- svelte-ignore a11y_autofocus -->
            <input
              placeholder="Name"
              class="input w-full"
              {...attrs}
              bind:value={$formData.name}
              autofocus
            />
            <div class="">
              <button class="btn preset-filled-surface-50-950">Generate</button>
            </div>
          </div>
        {/snippet}
      </Control>
      <FieldErrors>
        {#snippet children({ errors, errorProps })}
          {#each errors as error}
            <span class="text-error-500" {...errorProps}>{error}</span>
          {/each}
        {/snippet}
      </FieldErrors>
    </Field>
  </form>
  <div>
    {#if lastGenerated.length}
      <div class="flex flex-col rounded bg-surface-50-950 p-4">
        <div class="flex w-full flex-nowrap gap-2">
          <div class="fd fd-circle-alert w-6 text-[20px]"></div>
          <span>
            {#if lastGenerated.length === 1}
              This key will only be shown once and cannot be viewed again.<br />
              <span class="font-semibold">Please ensure you store it securely.</span>
            {:else}
              These keys will only be shown once and cannot be viewed again.<br />
              <span class="font-semibold">Please ensure you store them securely.</span>
            {/if}
          </span>
          <button
            class="fd fd-x ml-auto btn-icon text-[24px]"
            onclick={() => {
              lastGenerated = []
            }}
            aria-label="Hide new keys"
          ></button>
        </div>

        <div class="pt-4 pl-8">
          {#each lastGenerated as { name, key }}
            <div class="flex w-full flex-nowrap items-center gap-2">
              <span>{name}:</span>
              <span class="w-full overflow-hidden overflow-ellipsis">{key}</span>
              <ClipboardCopyButton value={key}></ClipboardCopyButton>
              <Tooltip placement="top">Copy to clipboard</Tooltip>
            </div>
          {/each}
        </div>
      </div>
    {/if}
  </div>
</div>
