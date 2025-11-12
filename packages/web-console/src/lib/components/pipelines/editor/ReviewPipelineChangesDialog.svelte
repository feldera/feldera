<script lang="ts">
  import InlineDropdown from '$lib/components/common/InlineDropdown.svelte'
  // import type { PipelineDiff } from '$lib/services/manager'
  import type { PipelineDiff } from '$lib/types/pipelineManager'
  import type { Snippet } from '$lib/types/svelte'
  import { slide } from 'svelte/transition'
  import IconLoader from '$assets/icons/generic/loader-alt.svg?component'

  let {
    changes,
    onCancel,
    onApprove,
    titleEnd
  }: {
    changes: PipelineDiff
    onCancel: () => void
    onApprove: () => Promise<void>
    titleEnd?: Snippet
  } = $props()

  let numRelationsToRemove = $derived(changes.tables.removed.length + changes.views.removed.length)
  let numTablesToBackfill = $derived(changes.tables.modified.length + changes.tables.added.length)
  let numViewsToRecompute = $derived(changes.views.modified.length + changes.views.added.length)
  let numConnectorsToRemove = $derived(
    changes.inputConnectors.removed.length + changes.outputConnectors.removed.length
  )
  let numConnectorsToBackfill = $derived(
    changes.inputConnectors.modified.length + changes.inputConnectors.added.length
  )
  let numConnectorsToRecompute = $derived(
    changes.outputConnectors.modified.length + changes.outputConnectors.added.length
  )
  let showDeleted = $state(true)
  let showToReinitialize = $state(true)
  let showToRecompute = $state(true)
  let showConnectorsDeleted = $state(true)
  let showConnectorsToReinitialize = $state(true)
  let showConnectorsToRecompute = $state(true)

  let approvingPromise: Promise<void> = $state(Promise.resolve())

  const keepFooterAtBottom = false
</script>

<div
  class="bg-white-dark sticky -top-4 z-10 -m-4 mb-0 flex flex-nowrap justify-between p-4 md:-top-6 md:-m-6 md:mb-0 md:p-6"
>
  <div class="place-self-center text-2xl font-semibold">Review Pipeline Changes</div>
  {@render titleEnd?.()}
</div>
<div class="flex flex-col gap-2 md:gap-8">
  {#if changes.error}
    <div class="mb-4 rounded-container border border-warning-500 p-4 text-warning-950-50">
      <div class="flex items-center gap-2">
        <span class="fd fd-circle-alert text-[20px] text-warning-500"></span>
        <span class="font-semibold">Error Computing Changes</span>
      </div>
      <div class="mt-2">{changes.error}</div>
    </div>
  {/if}

  <div>
    This pipeline has been modified while it was suspended. Please review the following updates
    before resuming.
  </div>

  {#snippet list(title: string, items: string[], iconClass: string, itemClass?: string)}
    <div class="flex flex-col gap-2">
      <div class="flex flex-nowrap items-center gap-2">
        <span class="h-[14px] w-4 {iconClass}"></span>
        {title}:
      </div>
      {#each items as item}
        <div class="{itemClass} pl-6">
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
      <span class="text-lg font-semibold">
        {@render title()}
      </span>
    </div>
  {/snippet}

  {#if numRelationsToRemove}
    <div>
      <InlineDropdown bind:open={showDeleted}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numRelationsToRemove === 1
              ? `One Deleted Table or View`
              : `${numRelationsToRemove} Deleted Tables and Views`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">
              These resources have been deleted and will no longer be available
            </div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.tables.removed.length > 0}
                {@render list(
                  'Deleted Tables',
                  changes.tables.removed,
                  'fd fd-circle-slash text-error-500',
                  'line-through'
                )}
              {/if}
              {#if changes.views.removed.length > 0}
                {@render list(
                  'Deleted Views',
                  changes.views.removed,
                  'fd fd-circle-slash text-error-500',
                  'line-through'
                )}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}

  {#if numTablesToBackfill}
    <div>
      <InlineDropdown bind:open={showToReinitialize}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numTablesToBackfill === 1
              ? `One Table to Be Reinitialized`
              : `${numTablesToBackfill} Tables to Be Reinitialized`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">
              These tables will restart empty. Input connectors will backfill data from the
              beginning
            </div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.tables.modified.length > 0}
                {@render list(
                  'Modified',
                  changes.tables.modified,
                  'fd fd-circle-alert text-warning-500'
                )}
              {/if}
              {#if changes.tables.added.length > 0}
                {@render list('New', changes.tables.added, 'fd fd-circle-plus text-success-500')}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}

  {#if numViewsToRecompute}
    <div>
      <InlineDropdown bind:open={showToRecompute}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numViewsToRecompute === 1
              ? `One View to Be Recomputed`
              : `${numViewsToRecompute} Views to Be Recomputed`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">
              These views will be fully recomputed and streamed to any attached output connectors
            </div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.views.modified.length > 0}
                {@render list(
                  'Modified',
                  changes.views.modified,
                  'fd fd-circle-alert text-warning-500'
                )}
              {/if}
              {#if changes.views.added.length > 0}
                {@render list('New', changes.views.added, 'fd fd-circle-plus text-success-500')}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}

  {#if numConnectorsToRemove}
    <div>
      <InlineDropdown bind:open={showConnectorsDeleted}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numConnectorsToRemove === 1
              ? `One Deleted Connector`
              : `${numConnectorsToRemove} Deleted Connectors`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">These connectors have been deleted.</div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.inputConnectors.removed.length > 0}
                {@render list(
                  'Deleted Input Connectors',
                  changes.inputConnectors.removed,
                  'fd fd-circle-slash text-error-500',
                  'line-through'
                )}
              {/if}
              {#if changes.outputConnectors.removed.length > 0}
                {@render list(
                  'Deleted Output Connectors',
                  changes.outputConnectors.removed,
                  'fd fd-circle-slash text-error-500',
                  'line-through'
                )}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}

  {#if numConnectorsToBackfill}
    <div>
      <InlineDropdown bind:open={showConnectorsToReinitialize}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numConnectorsToBackfill === 1
              ? `One Input Connector to Be Reinitialized`
              : `${numConnectorsToBackfill} Input Connectors to Be Reinitialized`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">
              These input connectors will be reset to the initial state.
            </div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.inputConnectors.modified.length > 0}
                {@render list(
                  'Modified',
                  changes.inputConnectors.modified,
                  'fd fd-circle-alert text-warning-500'
                )}
              {/if}
              {#if changes.inputConnectors.added.length > 0}
                {@render list(
                  'New',
                  changes.inputConnectors.added,
                  'fd fd-circle-plus text-success-500'
                )}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}

  {#if numConnectorsToRecompute}
    <div>
      <InlineDropdown bind:open={showConnectorsToRecompute}>
        {#snippet header(open, toggle)}
          {#snippet title()}
            {numConnectorsToRecompute === 1
              ? `One Output Connector to Be Recomputed`
              : `${numConnectorsToRecompute} Output Connectors to Be Recomputed`}
          {/snippet}
          {@render dropdownHeader(open, toggle, title)}
        {/snippet}
        {#snippet content()}
          <div transition:slide={{ duration: 150 }}>
            <div class="pb-4 text-surface-500">
              These connectors will receive any future updates to the views they are attached to.
            </div>
            <div class="grid grid-cols-1 gap-4 sm:grid-cols-2 sm:gap-12">
              {#if changes.outputConnectors.modified.length > 0}
                {@render list(
                  'Modified',
                  changes.outputConnectors.modified,
                  'fd fd-circle-alert text-warning-500'
                )}
              {/if}
              {#if changes.outputConnectors.added.length > 0}
                {@render list(
                  'New',
                  changes.outputConnectors.added,
                  'fd fd-circle-plus text-success-500'
                )}
              {/if}
            </div>
          </div>
        {/snippet}
      </InlineDropdown>
    </div>
  {/if}
</div>

{#if keepFooterAtBottom}
  <div class="flex-grow"></div>
{/if}
<div class="bg-white-dark sticky bottom-0">
  <div class="flex justify-start gap-4 py-4 md:gap-6 md:py-4">
    {#await approvingPromise}
      <button class="btn pointer-events-none pr-8 preset-filled-primary-500">
        <IconLoader class="mr-4 h-5 flex-none animate-spin fill-surface-50"></IconLoader>
        Approving...
      </button>
    {:then _}
      <button
        class="btn preset-filled-primary-500"
        onclick={() => {
          approvingPromise = onApprove().then(() => onCancel())
        }}
      >
        Approve and Continue
      </button>
    {/await}
    <button
      class="btn preset-tonal-surface"
      onclick={() => {
        onCancel()
      }}
    >
      Cancel
    </button>
  </div>
</div>
