<script lang="ts">
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  import type { PipelineChangesDiff } from '$lib/types/pipelineManager'
  import type { Snippet } from 'svelte'
  import { slide } from 'svelte/transition'

  let {
    changes,
    onskip
  }: {
    changes: PipelineChangesDiff
    onskip?: () => void
  } = $props()

  let numDeleted = $derived(changes.deleted.tables.length + changes.deleted.views.length)
  let numToReinitialize = $derived(changes.modified.tables.length + changes.new.tables.length)
  let numToRecompute = $derived(changes.modified.views.length + changes.new.views.length)
  $effect.root(() => {
    if (numDeleted + numToReinitialize + numToRecompute === 0) {
      onskip?.()
    }
  })
  let showDeleted = $state(true)
  let showToReinitialize = $state(true)
  let showToRecompute = $state(true)
</script>

<div>
  Your pipeline has been modified while it was suspended. Please review the following updates before
  resuming.
</div>

{#snippet list(title: string, items: string[], iconClass: string, itemClass?: string)}
  <div class="flex flex-col gap-4">
    <div class="flex flex-nowrap gap-2">
      <span class="text-[20px] {iconClass}"></span>
      {title}
    </div>
    {#each items as item}
      <div class={itemClass}>
        {item}
      </div>
    {/each}
  </div>
{/snippet}

{#snippet dropdownHeader(open: boolean, toggle: () => void, title: Snippet)}
  <div
    class="flex w-fit cursor-pointer items-center gap-2 py-2"
    onclick={toggle}
    role="presentation"
  >
    <div
      class={'fd fd-chevron-down text-[20px] transition-transform ' + (open ? 'rotate-180' : '')}
    ></div>

    {@render title()}
  </div>
{/snippet}

{#if numDeleted}
  <div>
    <InlineDropdown bind:open={showDeleted}>
      {#snippet header(open, toggle)}
        {#snippet title()}
          <span class="text-lg"
            >{numDeleted === 1
              ? `One Deleted Table or View`
              : `${numDeleted} Deleted Tables and Views`}</span
          >
        {/snippet}
        {@render dropdownHeader(open, toggle, title)}
      {/snippet}
      {#snippet content()}
        <div transition:slide={{ duration: 150 }}>
          <div class="pb-4 text-surface-500">
            These resources have been deleted and will no longer be available
          </div>
          <div class="flex flex-row gap-4 sm:gap-12">
            {@render list(
              'Deleted Tables',
              changes.deleted.tables,
              'fd fd-circle-slash text-error-500',
              'line-through'
            )}
            {@render list(
              'Deleted Views',
              changes.deleted.views,
              'fd fd-circle-slash text-error-500',
              'line-through'
            )}
          </div>
        </div>
      {/snippet}
    </InlineDropdown>
  </div>
{/if}

{#if numToReinitialize}
  <div>
    <InlineDropdown bind:open={showToReinitialize}>
      {#snippet header(open, toggle)}
        {#snippet title()}
          <span class="text-lg"
            >{numToReinitialize === 1
              ? `One Table to Be Reinitialized`
              : `${numToReinitialize} Tables to Be Reinitialized`}</span
          >
        {/snippet}
        {@render dropdownHeader(open, toggle, title)}
      {/snippet}
      {#snippet content()}
        <div transition:slide={{ duration: 150 }}>
          <div class="pb-4 text-surface-500">
            These tables will restart empty. Input connectors will backfill data from the beginning
          </div>
          <div class="flex flex-row gap-4 sm:gap-12">
            {@render list(
              'Modified',
              changes.modified.tables,
              'fd fd-circle-alert text-warning-500'
            )}
            {@render list('New', changes.new.tables, 'fd fd-circle-plus text-success-500')}
          </div>
        </div>
      {/snippet}
    </InlineDropdown>
  </div>
{/if}

{#if numToRecompute}
  <div>
    <InlineDropdown bind:open={showToRecompute}>
      {#snippet header(open, toggle)}
        {#snippet title()}
          <span class="text-lg"
            >{numToRecompute === 1
              ? `One View to Be Recomputed`
              : `${numToRecompute} Views to Be Recomputed`}</span
          >
        {/snippet}
        {@render dropdownHeader(open, toggle, title)}
      {/snippet}
      {#snippet content()}
        <div transition:slide={{ duration: 150 }}>
          <div class="pb-4 text-surface-500">
            These views will be fully recomputed and streamed to any attached output connectors
          </div>
          <div class="flex flex-row gap-4 sm:gap-12">
            {@render list(
              'Modified',
              changes.modified.views,
              'fd fd-circle-alert text-warning-500'
            )}
            {@render list('New', changes.new.views, 'fd fd-circle-plus text-success-500')}
          </div>
        </div>
      {/snippet}
    </InlineDropdown>
  </div>
{/if}
