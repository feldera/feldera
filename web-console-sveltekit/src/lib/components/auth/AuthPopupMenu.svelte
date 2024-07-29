<script lang="ts">
  import { base } from '$app/paths'
  import type { UserProfile } from '$lib/compositions/auth'
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import invariant from 'tiny-invariant'
  import ApiKeyMenu from '../other/ApiKeyMenu.svelte'

  const globalDialog = useGlobalDialog()
  let {
    user,
    signOut
  }: { user: UserProfile; signOut: (params: { callbackUrl: string }) => Promise<void> } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  <div>
    <div class="flex gap-2">
      {#if user.image}
        <img class="h-10 w-10 rounded-full" src={user.image} alt="User avatar" />
      {:else}
        <div class="h-10 w-10 rounded-full bg-surface-500"></div>
      {/if}
      <div>
        <div class="h4 font-normal" class:italic={!user.name}>{user.name || 'anonymous'}</div>
        <div class="">{user.email}</div>
      </div>
      <div class="ml-auto text-surface-600-400">logged in</div>
    </div>
  </div>
  <div class="hr"></div>
  <div class="flex flex-col items-start gap-4">
    <button
      class="btn text-surface-800-200 preset-outlined-surface-50-950 hover:preset-filled-surface-50-950"
      onclick={() => (globalDialog.dialog = apiKeyDialog)}
    >
      Manage API keys
    </button>
    <button
      class="btn text-surface-800-200 preset-outlined-surface-50-950 hover:preset-filled-surface-50-950"
      onclick={async () => {
        // invariant($page.data.authDetails.enabled === true)
        await signOut({ callbackUrl: undefined! })
        // await signOut({
        //   callbackUrl: $page.data.authDetails.providerSignOutUrl.replaceAll(
        //     '{redirect_uri}',
        //     encodeURIComponent(window.location.origin + base)
        //   )
        // })
        // await signOut({ callbackUrl: 'https://dev-jzraqtxsr8a3hhhv.us.auth0.com/oidc/logout' })
      }}><span class="bx bx-right-arrow-alt text-[24px]"></span> Logout</button
    >
  </div>
</div>

{#snippet apiKeyDialog()}
  <div class="h-96">
    <ApiKeyMenu></ApiKeyMenu>
  </div>
{/snippet}
