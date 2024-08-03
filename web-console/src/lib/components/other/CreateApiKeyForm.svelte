<script lang="ts">
  import { postApiKey } from '$lib/services/pipelineManager'
  import { superForm } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'
  import { Field, Label, FieldErrors, Control, Description, Fieldset, Legend } from 'formsnap'
  import { clipboard } from '@svelte-bin/clipboard'

  import * as va from 'valibot'

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
        postApiKey(form.data.name).then((response) =>
          lastGenerated.push({ name: response.name, key: response.api_key })
        )
      }
    }
  )
  const { form: formData, enhance } = form
  let lastGenerated = $state<{ name: string; key: string }[]>([])
</script>

<div class="flex flex-col gap-4 p-4">
  <form class="flex flex-col gap-4" use:enhance>
    <div class="h5 font-normal">Generate a new API key</div>
    <div>
      You will be shown the generated API key only once.<br />
      You will not be able to view it afterwards.<br />
      Please store it securely.
    </div>

    <Field {form} name="name">
      <Control let:attrs>
        <input placeholder="Name" class="input w-full" {...attrs} bind:value={$formData.name} />
      </Control>
      <FieldErrors />
    </Field>
    <div class="flex w-full justify-between">
      <button class="btn preset-outlined-surface-500" onclick={onClose}>BACK</button>
      <button class="btn preset-outlined-primary-500" type="submit">GENERATE</button>
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
        <button class=" bx bx-copy btn-icon flex-none" use:clipboard={key}></button>
      </div>
    {/each}
  </div>
</div>
