<script lang="ts">
  import { useGlobalDialog } from '$lib/compositions/useGlobalDialog.svelte'
  import ApiKeyMenu from '$lib/components/other/ApiKeyMenu.svelte'
  import type { UserProfile } from '$lib/types/auth'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  let { darkMode, toggleDarkMode } = useDarkMode()

  const globalDialog = useGlobalDialog()
  let {
    user,
    signOut
  }: { user: UserProfile; signOut: (params: { callbackUrl: string }) => Promise<void> } = $props()
</script>

<div class="flex flex-col gap-4 p-4">
  <div>
    <div class="flex gap-2">
      {#if user.picture}
        <img class="h-10 w-10 rounded-full" src={user.picture} alt="User avatar" />
      {:else}
        <div class="fd fd-circle-user h-10 w-10 rounded-full text-[40px]"></div>
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
        await signOut({ callbackUrl: undefined! })
      }}><span class="fd fd-arrow-right text-[24px]"></span> Logout</button
    >

    <button
      onclick={toggleDarkMode}
      class="preset-grayout-surface btn-icon text-[24px]
      {darkMode.value === 'dark' ? 'fd fd-sun' : 'fd fd-moon'}"
    ></button>
  </div>
</div>

{#snippet apiKeyDialog()}
  <div class="h-96">
    <ApiKeyMenu></ApiKeyMenu>
  </div>
{/snippet}
