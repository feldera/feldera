<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { fade } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
  import type { AuthDetails } from '$lib/types/auth'
  import { useDarkMode } from '$lib/compositions/useDarkMode.svelte'
  let { darkMode, toggleDarkMode } = useDarkMode()

  const { compactBreakpoint = '' }: { compactBreakpoint?: string } = $props()
  const auth = $page.data.auth as AuthDetails | undefined
</script>

{#if typeof auth === 'object' && 'logout' in auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class=" ml-2 flex items-center gap-2 rounded font-semibold preset-filled-primary-500"
      >
        <div class="hidden {compactBreakpoint}block w-2"></div>
        <span class="hidden {compactBreakpoint}block">Logged in</span>
        <div class="hidden {compactBreakpoint}block w-1"></div>

        <div class="fd fd-user btn-icon text-[32px]">
          <div class="hidden {compactBreakpoint}block w-2"></div>
        </div>
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end overflow-y-auto rounded bg-white shadow-md scrollbar dark:bg-black"
      >
        <AuthPopupMenu user={auth.profile} signOut={auth.logout}></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <!-- <button class="fd fd-lock-open btn-icon cursor-default text-[24px] text-surface-600-400"></button>
  <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip> -->

  <Popup>
    {#snippet trigger(toggle)}
      <button
        onclick={toggle}
        class="fd fd-lock-open btn btn-icon text-[24px] preset-tonal-surface"
        aria-label="Open settings popup"
      >
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-30 flex max-h-[400px] w-[calc(100vw-100px)] max-w-[260px] flex-col justify-end gap-4 overflow-y-auto rounded bg-white p-4 shadow-md scrollbar dark:bg-black"
      >
        <div class="px-3 text-surface-700-300">Authentication is disabled</div>

        <button
          onclick={toggleDarkMode}
          class="preset-grayout-surface btn-icon text-[24px]
          {darkMode.value === 'dark' ? 'fd fd-sun' : 'fd fd-moon'}"
          aria-label="Toggle dark mode"
        ></button>
      </div>
    {/snippet}
  </Popup>
{/if}
