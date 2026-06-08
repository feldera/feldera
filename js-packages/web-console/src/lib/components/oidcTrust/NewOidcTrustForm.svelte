<script lang="ts">
  import { Select } from 'common-ui'
  import { Control, Field, FieldErrors, Label } from 'formsnap'
  import { setError, superForm } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'

  import * as va from 'valibot'
  import { page } from '$app/state'
  import { postOidcTrust, type Role } from '$lib/services/pipelineManager'

  const { onSubmit, onSuccess }: { onSubmit?: () => void; onSuccess?: () => void } = $props()

  // The backend caps the granted role at the caller's role; only an owner may
  // grant `owner`, so offer it only then.
  const isOwner = page.data.feldera?.isOwner ?? false

  const schema = va.object({
    name: va.pipe(va.string(), va.minLength(1, 'Specify a name')),
    issuer: va.pipe(va.string(), va.minLength(1, 'Specify the issuer URL')),
    subject: va.pipe(va.string(), va.minLength(1, 'Specify a subject pattern')),
    audience: va.string(),
    description: va.string(),
    role: va.picklist(['read', 'write', 'admin', 'owner'] as const)
  })

  const form = superForm(
    {
      name: '',
      issuer: '',
      subject: '',
      audience: '',
      description: '',
      role: 'read' as Role
    },
    {
      SPA: true,
      validators: valibot(schema),
      onUpdate({ form: f }) {
        if (!f.valid) return
        onSubmit?.()
        postOidcTrust({
          name: f.data.name,
          issuer: f.data.issuer,
          subject: f.data.subject,
          audience: f.data.audience || undefined,
          description: f.data.description || undefined,
          role: f.data.role
        }).then(
          () => onSuccess?.(),
          (e) => setError(f, 'name', 'message' in e ? e.message : String(e))
        )
      }
    }
  )
  const { form: formData, enhance, submit } = form
</script>

<form
  class="flex flex-col gap-3"
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
        <Label>Name</Label>
        <input
          placeholder="github-actions-prod"
          class="input w-full"
          {...attrs}
          bind:value={$formData.name}
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

  <Field {form} name="issuer">
    <Control>
      {#snippet children(attrs)}
        <Label>Issuer URL</Label>
        <input
          placeholder="https://token.actions.githubusercontent.com"
          class="input w-full"
          {...attrs}
          bind:value={$formData.issuer}
        />
      {/snippet}
    </Control>
  </Field>

  <Field {form} name="subject">
    <Control>
      {#snippet children(attrs)}
        <Label>Subject pattern</Label>
        <input
          placeholder="repo:my-org/my-repo:ref:refs/heads/main"
          class="input w-full"
          {...attrs}
          bind:value={$formData.subject}
        />
      {/snippet}
    </Control>
  </Field>

  <Field {form} name="audience">
    <Control>
      {#snippet children(attrs)}
        <Label>Audience pattern (optional)</Label>
        <input
          placeholder="feldera"
          class="input w-full"
          {...attrs}
          bind:value={$formData.audience}
        />
      {/snippet}
    </Control>
  </Field>

  <Field {form} name="description">
    <Control>
      {#snippet children(attrs)}
        <Label>Description (optional)</Label>
        <input
          placeholder="What does this trust grant?"
          class="input w-full"
          {...attrs}
          bind:value={$formData.description}
        />
      {/snippet}
    </Control>
  </Field>

  <Field {form} name="role">
    <Control>
      {#snippet children(attrs)}
        <Label>Role</Label>
        <Select class="w-full" {...attrs} bind:value={$formData.role}>
          <option value="read">read</option>
          <option value="write">write</option>
          <option value="admin">admin</option>
          {#if isOwner}
            <option value="owner">owner</option>
          {/if}
        </Select>
      {/snippet}
    </Control>
  </Field>

  <p class="text-xs opacity-70">
    JWTs from <code>Issuer</code> whose <code>sub</code> matches
    <code>Subject pattern</code> (and, if specified, whose <code>aud</code> matches
    <code>Audience pattern</code>) authorize requests as this tenant. <code>*</code> is a
    wildcard.
  </p>

  <div class="flex justify-end">
    <button class="btn preset-filled-surface-50-950">Create</button>
  </div>
</form>
