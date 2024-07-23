<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'
  import { fade } from 'svelte/transition'
  import Popup from '../common/Popup.svelte'
  import AuthPopupMenu from './AuthPopupMenu.svelte'
</script>

{#if $page.data.session}
  <Popup>
    {#snippet trigger(toggle)}
      <button onclick={toggle} class="btn-icon preset-tonal-surface bx bx-lock-alt text-[24px]"
      ></button>
    {/snippet}
    {#snippet content(close)}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[400px] w-[calc(100vw-100px)] max-w-[400px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black">
        <AuthPopupMenu user={$page.data.session!.user!}></AuthPopupMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <button class="btn-icon bx bx-lock-open-alt text-surface-600-400 cursor-default text-[24px]"
  ></button>
  <Tooltip class="text-surface-950-50 bg-white dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip>
{/if}
