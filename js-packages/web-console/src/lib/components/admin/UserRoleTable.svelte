<script lang="ts">
  import { asyncReadable } from '@square/svelte-store'
  import { Select } from 'common-ui'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import {
    getTenantUsers,
    removeTenantUser,
    setTenantUserRole,
    type TenantUser
  } from '$lib/services/pipelineManager'

  const users = asyncReadable<TenantUser[]>([], getTenantUsers, { reloadable: true })

  const globalDialog = useGlobalDialog()

  // Pending per-row role edits, keyed by user_id. Absent means unchanged.
  let pendingRole = $state<Record<string, 'read' | 'write' | 'admin'>>({})
  let savingUserId = $state<string | null>(null)
  let errorMessage = $state('')

  const roleOf = (user: TenantUser) => pendingRole[user.user_id] ?? user.role

  const save = async (user: TenantUser) => {
    const role = roleOf(user)
    errorMessage = ''
    savingUserId = user.user_id
    try {
      await setTenantUserRole(user.user_id, role)
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
    Members appear here after their first login. Assign read, write, or admin; remove a member to
    revoke access.
  </p>
  {#if errorMessage}
    <div class="rounded preset-outlined-error-600-400 p-2 text-sm">{errorMessage}</div>
  {/if}
  <div class="scrollbar flex flex-col gap-2 overflow-auto">
    {#each $users as user (user.user_id)}
      {#snippet removeDialog()}
        <GenericDialog
          content={{
            title: `Remove ${user.email ?? user.subject}?`,
            description: 'They lose access to this tenant until invited again. Continue?',
            onSuccess: {
              name: 'Remove',
              callback: async () => {
                await removeTenantUser(user.user_id)
                users.reload?.()
                globalDialog.dialog = null
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
