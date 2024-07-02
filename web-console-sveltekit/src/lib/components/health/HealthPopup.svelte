<script lang="ts">
  import { useSystemErrors } from '$lib/compositions/health/systemErrors'
  import Tooltip from 'sv-tooltip'
  import Popup from '$lib/components/common/Popup.svelte'
  import { fade } from 'svelte/transition'
  import HealthMenu from './HealthMenu.svelte'
  const systemErrors = useSystemErrors()
</script>

{#if $systemErrors.length}
  <Popup>
    {#snippet trigger(open)}
      <button
        onclick={open}
        class="btn-icon bx bx-error preset-tonal-surface !text-warning-500 cursor-pointer text-[24px]">
      </button>
      <span class="badge-icon preset-filled-error-500 absolute -right-2 -top-2 text-sm">
        {$systemErrors.length}
      </span>
    {/snippet}
    {#snippet content()}
      <div
        transition:fade={{ duration: 100 }}
        class="absolute right-0 z-10 max-h-[500px] w-[calc(100vw-100px)] max-w-[600px] justify-end overflow-y-auto rounded bg-white shadow-md dark:bg-black">
        <HealthMenu {systemErrors}></HealthMenu>
      </div>
    {/snippet}
  </Popup>
{:else}
  <Tooltip tip="No errors detected in Feldera deployment" active={true}>
    <button class="btn-icon bx bx-check-circle preset-tonal-surface !text-success-500 text-[24px]">
    </button>
  </Tooltip>
{/if}
