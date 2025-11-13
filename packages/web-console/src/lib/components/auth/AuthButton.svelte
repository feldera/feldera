<script lang="ts">
  import { page } from '$app/state'
  import { fade } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import VersionDisplay from '$lib/components/version/VersionDisplay.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
  import type { AuthDetails } from '$lib/types/auth'
  import DarkModeSwitch from '$lib/components/layout/userPopup/DarkModeSwitch.svelte'
  import CurrentTenant from '$lib/components/auth/CurrentTenant.svelte'

  const { compactBreakpoint = '' }: { compactBreakpoint?: string } = $props()
  const auth = page.data.auth as AuthDetails | undefined
</script>

{#if typeof auth === 'object' && 'logout' in auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button onclick={toggle} class="flex items-center gap-2 rounded font-semibold">
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="fd fd-circle-user btn btn-icon text-[20px] preset-tonal-surface">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute right-0 z-30 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end rounded-container shadow-md scrollbar"
      >
        <AuthPopupMenu user={auth.profile} signOut={auth.logout}></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <!-- <button class="fd fd-lock-open btn-icon cursor-default text-[20px] text-surface-600-400"></button>
  <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip> -->

  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class="fd fd-lock-open btn btn-icon text-[20px] preset-tonal-surface"
        aria-label="Open settings popup"
      >
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="bg-white-dark absolute right-0 z-30 flex max-h-[400px] w-[calc(100vw-16px)] max-w-[360px] flex-col justify-end gap-4 rounded-container p-4 shadow-md scrollbar sm:max-w-[400px]"
      >
        <div class="text-surface-700-300">Authentication is disabled</div>

        <CurrentTenant></CurrentTenant>
        <div class="hr"></div>
        <DarkModeSwitch></DarkModeSwitch>
        <VersionDisplay></VersionDisplay>
      </div>
    {/snippet}
  </Popup>
{/if}
