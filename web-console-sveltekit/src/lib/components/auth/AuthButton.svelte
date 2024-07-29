<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { fade } from 'svelte/transition'
  import Popup from '../common/Popup.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
</script>

{#if 'logout' in $page.data.auth}
  <Popup>
    {#snippet trigger(toggle)}
      <button onclick={toggle} class="bx bx-lock-alt btn-icon text-[24px] preset-tonal-surface">
      </button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black"
      >
        <AuthPopupMenu
          user={$page.data.auth.userInfo}
          signOut={$page.data.auth.logout}
        ></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <button class="bx bx-lock-open-alt btn-icon cursor-default text-[24px] text-surface-600-400"
  ></button>
  <Tooltip class="bg-white text-surface-950-50 dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip>
{/if}
