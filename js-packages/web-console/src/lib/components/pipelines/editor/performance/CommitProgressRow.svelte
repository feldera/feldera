<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { Tooltip } from 'common-ui'
  import { type Snippet, untrack } from 'svelte'
  import { slide } from 'svelte/transition'
  import { useIsScreenSm } from '$lib/compositions/layout/useIsMobile.svelte'
  import { formatQty } from '$lib/functions/format'
  import type { CommitProgressSummary } from '$lib/services/manager'

  let {
    label,
    status,
    progress,
    idle = 'hide',
    resetKey,
    detail,
    class: _class = ''
  }: {
    /** Title above the status chip, e.g. "Transaction" or "Bootstrapping". */
    label: string
    /** Chip contents, or `null` while the activity this row tracks is idle. */
    status: { label: string; class: string } | null
    progress: CommitProgressSummary | null | undefined
    /**
     * How to render an idle row: 'hide' removes it, 'disable' keeps it in place
     * with a "None" chip so a neighboring row's progress bar never shifts.
     */
    idle?: 'disable' | 'hide'
    /**
     * Observes change in a value of any type: every time it changes,
     * the bar moves to its new position instantly rather than animating.
     */
    resetKey?: unknown
    /** Extra detail rendered next to the chip, e.g. a transaction ID. */
    detail?: Snippet
    class?: string
  } = $props()

  const isIdle = $derived(status === null)

  // From sm up the detail belongs beside the title; below that it sits on the
  // counts line. A snippet cannot render in two places at once, so the position
  // is chosen here rather than by toggling visibility with CSS.
  const isScreenSm = useIsScreenSm()
  const showDetail = $derived(detail !== undefined && !isIdle)

  const total = $derived(
    progress ? progress.completed + progress.in_progress + progress.remaining : 0
  )

  const completedPercent = $derived(total > 0 && progress ? (progress.completed / total) * 100 : 0)

  const inProgressFraction = $derived(
    progress && progress.in_progress_total_records > 0
      ? progress.in_progress_processed_records / progress.in_progress_total_records
      : 0
  )

  const combinedPercent = $derived(
    total > 0 && progress
      ? ((progress.completed + progress.in_progress * inProgressFraction) / total) * 100
      : null
  )

  // Disable the smooth progress bar transition when the tracked activity changes
  // so the bar jumps immediately to the new value rather than animating.
  let disableTransition = $state(false)
  let prevResetKey = untrack(() => resetKey)

  $effect(() => {
    const currentKey = resetKey
    if (currentKey !== prevResetKey) {
      prevResetKey = currentKey
      disableTransition = true
      requestAnimationFrame(() => {
        disableTransition = false
      })
    }
  })

  const transitionClass = $derived(disableTransition ? 'duration-0' : 'duration-2000 ease-linear')
</script>

{#if !isIdle || idle === 'disable'}
  <!-- The row states its own width as both a cap and a flex basis, so a wrapping
       parent lays two rows side by side while both fit and wraps them when they do
       not, without either row stretching to fill a line it has to itself. -->
  <div
    class="items-top flex max-w-150 basis-150 flex-wrap gap-x-4 gap-y-2 {_class}"
    transition:slide
  >
    <div class="flex w-full flex-col items-start sm:w-28 sm:items-center">
      <div class="flex items-baseline justify-center gap-2 text-base text-nowrap">
        {label}
        {#if showDetail && !isScreenSm.current}
          <span class="w-0 font-dm-mono">{@render detail!()}</span>
        {/if}
      </div>
      <div class="flex flex-nowrap items-center justify-center">
        <div></div>
        <div
          class="pointer-events-none chip tracking-wider uppercase {status?.class ??
            'bg-surface-100-900 text-surface-600-400'}"
        >
          {status?.label ?? 'None'}
        </div>
      </div>
    </div>

    <div class="flex flex-1 flex-col">
      <!-- A non-breaking space keeps the row height, and the bar below it, in
           place when there are no operator counts to show. It carries the same
           font as the counts below, whose taller metrics would otherwise leave
           this line 1px shorter and shift the bar up. -->
      <div class="text-base text-nowrap">
        {#if isScreenSm.current}
          <!-- Reserved even without a detail to show, so the counts of a row that
               has none still line up with those of a row that does. -->

          <span class="inline-block w-24 pr-2"
            >{#if showDetail}{@render detail!()}{/if}</span
          >
        {/if}
        {#if progress && !isIdle}
          <!-- The counts name no category of their own; the tooltips carry what
               they count, so the labels stay short enough to fit one line. -->
          <span
            data-testid="box-label-completed"
            class="cursor-help underline decoration-dotted underline-offset-4">Completed</span
          >
          <Tooltip placement="top">Operators that have been fully flushed</Tooltip>
          <span class="font-dm-mono font-bold">{formatQty(progress.completed)}</span> out of
          <span class="font-dm-mono font-bold">{formatQty(total)}</span>
          <span class="ml-2 select-none">·</span>
          <span
            data-testid="box-label-in-progress"
            class="ml-2 cursor-help underline decoration-dotted underline-offset-4"
            >In progress</span
          >
          <Tooltip placement="top">Operators currently being flushed</Tooltip>
          <span class="font-dm-mono font-bold">{formatQty(progress.in_progress)}</span>
        {:else}
          <span class="font-dm-mono font-bold">&nbsp;</span>
        {/if}
      </div>
      <div class="pt-2">
        <div class="relative {isIdle ? 'opacity-50' : ''}">
          <Progress class="h-2" value={isIdle ? 0 : combinedPercent} max={100}>
            <Progress.Track class="bg-surface-600-400">
              <Progress.Range class="bg-yellow-500 {transitionClass}" />
            </Progress.Track>
          </Progress>
          <Progress
            class="absolute inset-x-0 bottom-0 h-2"
            value={isIdle ? 0 : completedPercent}
            max={100}
          >
            <Progress.Track class="opacity-0"></Progress.Track>
            <Progress.Range class="absolute inset-y-0 left-0 bg-success-500 {transitionClass}" />
          </Progress>
        </div>
      </div>
    </div>
  </div>
{/if}
