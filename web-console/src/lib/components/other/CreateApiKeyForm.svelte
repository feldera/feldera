<script lang="ts">
  import { postApiKey } from '$lib/services/pipelineManager'
  import { superForm, setError } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'
  import { Field, FieldErrors, Control } from 'formsnap'
  import { clipboard } from '@svelte-bin/clipboard'

  import * as va from 'valibot'
  import { clickedClass } from '$lib/compositions/actions/clickedClass'
  import Tooltip from '$lib/components/common/Tooltip.svelte'

  let { onClose }: { onClose: () => void } = $props()

  const schema = va.object({
    name: va.pipe(va.string(), va.minLength(1, 'Specify API key name'))
  })
  const form = superForm(
    { name: '' },
    {
      SPA: true,
      validators: valibot(schema),
      onUpdate({ form, ...rest }) {
        if (!form.valid) {
          return
        }
        postApiKey(form.data.name).then(
          (response) => lastGenerated.push({ name: response.name, key: response.api_key }),
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
    class="flex flex-col gap-4"
    use:enhance
    onkeydown={(event) => {
      if (event.key === 'Enter') {
        event.preventDefault()
        submit()
      }
    }}
  >
    <div class="h5 font-normal">Generate a new API key</div>
    <div>
      You will be shown the generated API key only once.<br />
      You will not be able to view it afterwards.<br />
      Please store it securely.
    </div>

    <Field {form} name="name">
      <Control>
        {#snippet children(attrs)}
          <!-- svelte-ignore a11y_autofocus -->
          <input
            placeholder="Name"
            class="input w-full"
            {...attrs}
            bind:value={$formData.name}
            autofocus
          />
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
    <div class="flex w-full justify-between">
      <button
        class="btn preset-outlined-surface-500"
        onclick={onClose}
        onkeydown={(event) => {
          if (event.key === 'Enter') {
            event.preventDefault()
            onClose()
          }
        }}>BACK</button
      >
      <button class="btn preset-outlined-primary-500">GENERATE</button>
    </div>
  </form>
  <div class="">
    {#if lastGenerated.length}
      Generated keys:
    {/if}
    {#each lastGenerated as { name, key }}
      <div class="flex w-full flex-nowrap items-center gap-2">
        <span>{name}:</span>
        <span class="w-full overflow-hidden overflow-ellipsis">{key}</span>
        <button
          class="btn-icon flex-none"
          use:clipboard={key}
          use:clickedClass={{
            base: 'fd fd-content_copy',
            clicked: 'fd fd-check text-success-500 text-[20px] pointer-events-none'
          }}
          aria-label="Copy to clipboard"
        ></button>
        <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="top">
          Copy to clipboard
        </Tooltip>
      </div>
    {/each}
  </div>
</div>
