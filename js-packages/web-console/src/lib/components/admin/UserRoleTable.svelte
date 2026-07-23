<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { Select } from 'common-ui'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import {
    addTenantUser,
    getAuthConfig,
    getTenantUsers,
    removeTenantUser,
    setTenantUserRole,
    type TenantUser
  } from '$lib/services/pipelineManager'

  // Optional tenant (UUID/name): an owner viewing another tenant's members via
  // the admin page's per-tenant picker, without changing the global selection.
  const { tenant }: { tenant?: string } = $props()

  // Members authenticate through the platform's single configured OIDC issuer,
  // so a pre-provisioned grant is always keyed to that issuer (shown read-only).
  const authConfig = asyncReadable<Record<string, { issuer?: string }> | undefined>(
    undefined,
    getAuthConfig
  )
  const configuredIssuer = $derived(
    $authConfig?.GenericOidc?.issuer ??
      $authConfig?.AwsCognito?.issuer ??
      '(configured at deploy time)'
  )

  const users = asyncReadable<TenantUser[]>([], () => getTenantUsers(tenant), { reloadable: true })
  // Reload when the selected tenant changes.
  $effect(() => {
    tenant
    users.reload?.()
  })

  const globalDialog = useGlobalDialog()

  // Pending per-row role edits, keyed by user_id. Absent means unchanged.
  let pendingRole = $state<Record<string, 'read' | 'write' | 'admin'>>({})
  let savingUserId = $state<string | null>(null)
  let errorMessage = $state('')

  // Pre-provision (add-member) form state.
  let newSubject = $state('')
  let newEmail = $state('')
  let newRole = $state<'read' | 'write' | 'admin'>('read')
  let adding = $state(false)

  // A member the IdP resolved as owner can appear in the list; owner is not
  // assignable here, so show it read-only rather than a blank select.
  const roleOptions = ['read', 'write', 'admin'] as const
  const isAssignable = (role: string): role is 'read' | 'write' | 'admin' =>
    (roleOptions as readonly string[]).includes(role)

  const roleOf = (user: TenantUser) => pendingRole[user.user_id] ?? user.role

  const addMember = async () => {
    errorMessage = ''
    adding = true
    try {
      await addTenantUser(
        {
          subject: newSubject.trim(),
          email: newEmail.trim() || undefined,
          role: newRole
        },
        tenant
      )
      newSubject = ''
      newEmail = ''
      newRole = 'read'
      users.reload?.()
    } catch (e) {
      errorMessage = e instanceof Error ? e.message : String(e)
    } finally {
      adding = false
    }
  }

  const save = async (user: TenantUser) => {
    const role = roleOf(user)
    if (!isAssignable(role)) {
      return // owner is not assignable here
    }
    errorMessage = ''
    savingUserId = user.user_id
    try {
      await setTenantUserRole(user.user_id, role, tenant)
      delete pendingRole[user.user_id]
      users.reload?.()
    } catch (e) {
      errorMessage = e instanceof Error ? e.message : String(e)
    } finally {
      savingUserId = null
    }
  }
</script>

<div class="flex flex-col gap-3">
  <p class="text-sm opacity-70">
    Members appear here after their first login. Assign read, write, or admin. Removing a member
    drops their role now, but if your identity provider still grants them access they are re-added
    at the default role on their next login — revoke at the provider for a durable block.
    Pre-provision a member below to grant a role before their first login.
  </p>
  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}

  <!-- Pre-provision by identity: create the membership before the user's first
       login. The grant is dormant until that identity signs in to the tenant. -->
  <div class="rounded border border-surface-100-900 p-3">
    <div class="mb-2">
      <div class="font-medium">Pre-provision a member</div>
      <p class="text-sm opacity-70">
        Grant a role before the user's first login. <b>Subject</b> must exactly match the
        <code>sub</code> claim of the JWT the user will present — otherwise the grant will not attach
        at login. The issuer is the platform's configured one (shown below). Email is for display only.
      </p>
    </div>
    <form
      class="flex flex-wrap items-end gap-2"
      onsubmit={(e) => {
        e.preventDefault()
        addMember()
      }}
    >
      <label class="flex flex-col text-sm">
        <span class="opacity-70">Provider — OIDC issuer (<code>iss</code>)</span>
        <input
          class="input w-64 opacity-60"
          value={configuredIssuer}
          readonly
          disabled
          title="Statically configured at deploy time (Helm: FELDERA_AUTH_ISSUER). Members authenticate through this issuer; it cannot be changed here."
        />
      </label>
      <label class="flex flex-col text-sm">
        <span class="opacity-70">Subject — OIDC <code>sub</code></span>
        <input
          class="input w-56"
          bind:value={newSubject}
          placeholder="10769150350006150715113082367"
        />
      </label>
      <label class="flex flex-col text-sm">
        <span class="opacity-70">Email (display only)</span>
        <input class="input w-56" bind:value={newEmail} placeholder="user@acme.com" />
      </label>
      <label class="flex flex-col text-sm">
        <span class="opacity-70">Role</span>
        <Select bind:value={newRole} class="w-28">
          <option value="read">read</option>
          <option value="write">write</option>
          <option value="admin">admin</option>
        </Select>
      </label>
      <button
        type="submit"
        class="btn preset-filled-primary-500"
        disabled={adding || !newSubject.trim()}
      >
        Add member
      </button>
    </form>
  </div>
  <div class="scrollbar flex flex-col gap-2 overflow-auto">
    {#each $users as user (user.user_id)}
      {#snippet removeDialog()}
        <GenericDialog
          content={{
            title: `Remove ${user.email ?? user.subject}?`,
            description:
              'Drops their role in this tenant now. If your identity provider still grants them access, they are re-added at the default role on their next login; revoke at the provider for a durable block. Continue?',
            onSuccess: {
              name: 'Remove',
              callback: async () => {
                // Surface failures instead of swallowing them and leaving the
                // dialog stuck; only close on success.
                try {
                  await removeTenantUser(user.user_id, tenant)
                  users.reload?.()
                  globalDialog.dialog = null
                } catch (e) {
                  errorMessage = e instanceof Error ? e.message : String(e)
                  globalDialog.dialog = null
                }
              },
              'data-testid': 'button-confirm-remove'
            },
            onCancel: {
              callback: () => {
                globalDialog.dialog = null
              }
            }
          }}
          noclose
          danger
        ></GenericDialog>
      {/snippet}
      <div class="flex flex-nowrap items-center gap-2 border-b border-surface-100-900 py-2">
        <div class="w-full">
          <div class="font-medium">{user.email ?? user.subject}</div>
          <div class="text-sm opacity-70">
            <code>{user.provider}</code> · sub=<code>{user.subject}</code>
          </div>
        </div>
        {#if isAssignable(user.role)}
          <Select
            value={roleOf(user)}
            onchange={(e) => {
              pendingRole[user.user_id] = (e.currentTarget as HTMLSelectElement).value as
                | 'read'
                | 'write'
                | 'admin'
            }}
            class="w-28"
            aria-label="Role for {user.email ?? user.subject}"
          >
            <option value="read">read</option>
            <option value="write">write</option>
            <option value="admin">admin</option>
          </Select>
          <button
            class="btn preset-filled-surface-50-950"
            disabled={pendingRole[user.user_id] === undefined || savingUserId === user.user_id}
            onclick={() => save(user)}
          >
            Save
          </button>
        {:else}
          <!-- owner is platform-wide, not a tenant membership, so it is shown
               read-only here. -->
          <span class="w-28 text-center opacity-70">{user.role}</span>
        {/if}
        <button
          class="fd fd-trash-2 btn-icon text-[20px]"
          aria-label="Remove {user.email ?? user.subject}"
          onclick={() => (globalDialog.dialog = removeDialog)}
        ></button>
      </div>
    {:else}
      <div class="opacity-70">No members yet. Users appear after their first login.</div>
    {/each}
  </div>
</div>
