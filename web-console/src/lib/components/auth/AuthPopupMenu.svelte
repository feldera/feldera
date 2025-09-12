<script lang="ts">
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import ApiKeyMenu from '$lib/components/other/ApiKeyMenu.svelte'
  import type { UserProfile } from '$lib/types/auth'
  import DarkModeSwitch from '$lib/components/layout/userPopup/DarkModeSwitch.svelte'
  import VersionDisplay from '$lib/components/version/VersionDisplay.svelte'
  import CurrentTenant from '$lib/components/auth/CurrentTenant.svelte'

  const globalDialog = useGlobalDialog()
  let {
    user,
    signOut
  }: { user: UserProfile; signOut: (params: { callbackUrl: string }) => Promise<void> } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  <button
    class="group flex flex-nowrap items-center justify-between font-medium"
    onclick={() => (globalDialog.dialog = apiKeyDialog)}
  >
    Manage API keys
    <span class=" fd fd-chevron-right rounded px-4 py-2 text-[20px] group-hover:bg-surface-50-950"
    ></span>
  </button>

  <div class="hr"></div>
  <DarkModeSwitch></DarkModeSwitch>
  <div class="hr"></div>
  <div class="flex flex-col gap-2">
    <div class="flex gap-2">
      {#if user.picture}
        <img class="h-10 w-10 rounded-full" src={user.picture} alt="User avatar" />
      {:else}
        <div class="fd fd-circle-user h-10 w-10 rounded-full text-[40px]"></div>
      {/if}
      <div class="">
        <div class="h4 break-all font-normal" class:italic={!user.name}>
          {user.name || 'anonymous'}
        </div>
        <div class="">{user.email}</div>
      </div>
    </div>
    <CurrentTenant></CurrentTenant>
    <div class="">
      <button
        class=" btn px-8 text-surface-800-200 preset-filled-surface-50-950 hover:preset-filled-surface-50-950"
        onclick={async () => {
          await signOut({ callbackUrl: undefined! })
        }}>Sign Out</button
      >
    </div>
  </div>
  <VersionDisplay></VersionDisplay>
</div>

{#snippet apiKeyDialog()}
  <ApiKeyMenu></ApiKeyMenu>
{/snippet}
