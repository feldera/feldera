<script lang="ts">
  import { Select } from 'common-ui'
  import { Control, Field, FieldErrors, Label } from 'formsnap'
  import { superForm } from 'sveltekit-superforms'
  import { valibot } from 'sveltekit-superforms/adapters'

  import * as va from 'valibot'
  import { page } from '$app/state'
  import { postOidcTrust, type Role } from '$lib/services/pipelineManager'

  const {
    onSubmit,
    onSuccess,
    allowOwner = false,
    fixedRole,
    tenant
  }: {
    onSubmit?: () => void
    onSuccess?: () => void
    allowOwner?: boolean
    // When set, the form creates trusts at exactly this role and hides the role
    // picker (the admin page's owner-access section passes `fixedRole="owner"`).
    fixedRole?: Role
    tenant?: string
  } = $props()

  // Backend rejections (role cap, duplicate name, ...) can concern any field, so
  // show them as a form-level error rather than pinning every one to Name.
  let submitError = $state('')

  // `owner` is a platform-wide grant, so it is offered only to an owner AND only
  // where owner trusts belong (the Admin page's owner-access section, which
  // passes `allowOwner`). The tenant-scoped "Manage OIDC trust" menu never
  // offers it. read/write/admin trusts are creatable by any tenant admin.
  const canGrantOwner = allowOwner && (page.data.feldera?.isOwner ?? false)

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
      role: (fixedRole ?? 'read') as Role
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

  {#if fixedRole}
    <input type="hidden" bind:value={$formData.role} />
  {:else}
    <Field {form} name="role">
      <Control>
        {#snippet children(attrs)}
          <Label>Role</Label>
          <Select class="w-full" {...attrs} bind:value={$formData.role}>
            <option value="read">read</option>
            <option value="write">write</option>
            <option value="admin">admin</option>
            {#if canGrantOwner}
              <option value="owner">owner</option>
            {/if}
          </Select>
        {/snippet}
      </Control>
    </Field>
  {/if}

  <p class="text-xs opacity-70">
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
    <button class="btn preset-filled-surface-50-950">Create</button>
  </div>
</form>
