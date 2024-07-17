<script lang="ts">
  import { page } from '$app/stores'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'

  $effect(() => {
    console.log('aaa')
    console.log('$page.data.session?.user', $page.data.session)
    console.log('aab')
  })
</script>

{#if $page.data.session}
  <button class="btn-icon preset-tonal-surface bx bx-lock-alt text-[24px]"></button>
{:else}
  <button class="btn-icon bx bx-lock-open-alt text-surface-600-400 cursor-default text-[24px]"
  ></button>
  <Tooltip class="text-surface-950-50 bg-white dark:bg-black" placement="left">
    Authentication is disabled
  </Tooltip>
{/if}
