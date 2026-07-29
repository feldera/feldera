<script lang="ts" module>
  // Fields to prefill the form with, e.g. to duplicate an existing trust.
  export type TrustFormData = {
    name: string
    issuer: string
    subject: string
    audience: string
    description: string
    role: 'read' | 'write' | 'admin'
  }
</script>

<script lang="ts">
  import { Select } from 'common-ui'
  import { Control, Field, FieldErrors, Label } from 'formsnap'
  import { superForm } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'

  import * as va from 'valibot'
  import { type MemberRole, postOidcTrust } from '$lib/services/pipelineManager'

  const {
    onSubmit,
    onSuccess,
    tenant
  }: {
    onSubmit?: () => void
    onSuccess?: () => void
    tenant?: string
  } = $props()

  // Backend rejections (role cap, duplicate name, ...) can concern any field, so
  // show them as a form-level error rather than pinning every one to Name.
  let submitError = $state('')

  const schema = va.object({
    name: va.pipe(va.string(), va.minLength(1, 'Specify a name')),
    issuer: va.pipe(va.string(), va.minLength(1, 'Specify the issuer URL')),
    subject: va.pipe(va.string(), va.minLength(1, 'Specify a subject pattern')),
    audience: va.string(),
    description: va.string(),
    role: va.picklist(['read', 'write', 'admin'] as const)
  })

  const form = superForm(
    {
      name: '',
      issuer: '',
      subject: '',
      audience: '',
      description: '',
      role: 'read' as MemberRole
    },
    {
      SPA: true,
      validators: valibot(schema),
      onUpdate({ form: f }) {
        if (!f.valid) {
          return
        }
        submitError = ''
        onSubmit?.()
        postOidcTrust(
          {
            name: f.data.name,
            issuer: f.data.issuer,
            subject: f.data.subject,
            audience: f.data.audience || undefined,
            description: f.data.description || undefined,
            role: f.data.role
          },
          tenant
        ).then(
          () => onSuccess?.(),
          (e) => {
            submitError = e instanceof Error ? e.message : String(e)
          }
        )
      }
    }
  )
  const { form: formData, enhance, submit } = form

  // Prefill the form to duplicate an existing trust; the user reviews and
  // presses Create. The caller reveals and scrolls the form into view.
  export function fill(values: TrustFormData) {
    $formData = { ...$formData, ...values }
  }
</script>

{#snippet fieldErrors()}
  <FieldErrors>
    {#snippet children({ errors, errorProps })}
      {#each errors as error}
        <span class="text-error-500" {...errorProps}>{error}</span>
      {/each}
    {/snippet}
  </FieldErrors>
{/snippet}

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
  <span class="text-xl font-semibold">Create new trust</span>
  <Field {form} name="name">
    <Control>
      {#snippet children(attrs)}
        <Label>Name</Label>
        <input
          placeholder="github-actions-prod"
          class="input h-9"
          {...attrs}
          bind:value={$formData.name}
        />
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <Field {form} name="issuer">
    <Control>
      {#snippet children(attrs)}
        <Label>Issuer URL</Label>
        <input
          placeholder="https://token.actions.githubusercontent.com"
          class="input h-9"
          {...attrs}
          bind:value={$formData.issuer}
        />
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <Field {form} name="subject">
    <Control>
      {#snippet children(attrs)}
        <Label>Subject pattern</Label>
        <input
          placeholder="repo:my-org/my-repo:ref:refs/heads/main"
          class="input h-9"
          {...attrs}
          bind:value={$formData.subject}
        />
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <Field {form} name="audience">
    <Control>
      {#snippet children(attrs)}
        <Label>Audience pattern (optional)</Label>
        <input placeholder="feldera" class="input h-9" {...attrs} bind:value={$formData.audience} />
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <Field {form} name="description">
    <Control>
      {#snippet children(attrs)}
        <Label>Description (optional)</Label>
        <input
          placeholder="What does this trust grant?"
          class="input h-9"
          {...attrs}
          bind:value={$formData.description}
        />
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <Field {form} name="role">
    <Control>
      {#snippet children(attrs)}
        <Label>Role</Label>
        <Select class="h-9 text-base!" {...attrs} bind:value={$formData.role}>
          <option value="read">read</option>
          <option value="write">write</option>
          <option value="admin">admin</option>
        </Select>
      {/snippet}
    </Control>
    {@render fieldErrors()}
  </Field>

  <p class="text-sm text-surface-800-200">
    JWTs from <code>Issuer</code> whose <code>sub</code> matches
    <code>Subject pattern</code> authorize requests. <code>Audience pattern</code>, if set, is an
    extra filter on the <code>aud</code> claim (not the tenant selector). <code>*</code> is a
    wildcard. When one identity is trusted by several tenants, the
    <code>Feldera-Tenant</code> header picks which one.
  </p>

  {#if submitError}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{submitError}</div>
  {/if}

  <div class="flex justify-end">
    <button class="btn preset-filled-primary-500">Create</button>
  </div>
</form>
