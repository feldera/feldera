<script lang="ts" generics="K">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { formatQty } from 'common-lib/format'
  import { Tooltip, useIsScreenSm } from 'common-ui'
  import { type Snippet, untrack } from 'svelte'
  import { slide } from 'svelte/transition'
  import type { ProgressSample } from '#lib/compositions/progressBarAnimation.ts'
  import { PIPELINE_STATS_POLL_MS } from '#lib/compositions/useAggregatePipelineStats.svelte.ts'
  import { useProgressBarAnimation } from '#lib/compositions/useProgressBarAnimation.svelte.ts'
  import { commitProgressPercents, operatorTotal } from '#lib/functions/commitProgress.ts'
  import { statusChipClass, statusToneColors } from '#lib/functions/pipelineStatusColor.ts'
  import type { CommitProgressSummary } from '#lib/services/manager/index.ts'

  let {
    label,
    status,
    progress,
    idle = 'hide',
    resetKey,
    showBar = true,
    pollPeriodMs = PIPELINE_STATS_POLL_MS,
    header,
    class: _class = ''
  }: {
    /** Title above the status chip, e.g. "Transaction" or "Bootstrapping". */
    label: string
    /** Chip contents, or `null` while the activity this row tracks is idle. */
    status: Status | null
    progress: CommitProgressSummary | null | undefined
    /**
     * How to render an idle row: 'hide' removes it, 'disable' keeps it in place
     * with a "None" chip.
     */
    idle?: 'disable' | 'hide'
    /**
     * Identifies the activity, for example a transaction ID. When the key
     * changes, the bar moves to 100% and then starts again from 0. Thus, the
     * user sees that one activity stopped and a different activity started.
     */
    resetKey?: K
    /**
     * Set to `false` to hide the bar and its counts while the activity runs,
     * for example while a commit is too short to follow. A bar that started
     * stays until it gets to 100%.
     */
    showBar?: boolean
    /** The time between two polls of `progress`. The bar moves during this period. */
    pollPeriodMs?: number
    /** More data that the row always shows above the bar, for example a transaction ID. */
    header?: Snippet
    class?: string
  } = $props()

  type Status = { label: string; class: string }

  /** The data that the row shows near the bar. It changes at the same time as the bar. */
  type Shown = { key: K; status: Status | null; progress: CommitProgressSummary | null | undefined }

  const toSample = (polled: Shown, showBar: boolean): ProgressSample<Shown> => {
    const percents = commitProgressPercents(polled.progress)
    return {
      key: polled.key,
      active: polled.status !== null && showBar,
      combinedPercent: percents.combined,
      completedPercent: percents.completed,
      data: polled
    }
  }

  const polledSample = $derived(toSample({ key: resetKey as K, status, progress }, showBar))

  // The bar, the chip and the counts render the frame only. They do not use
  // the props from the poll directly.
  const bar = useProgressBarAnimation(
    () => polledSample,
    untrack(() => pollPeriodMs)
  )
  const frame = $derived(bar.current)

  const shown = $derived(frame.sample.data)
  const isBarShown = $derived(frame.sample.active)
  const total = $derived(operatorTotal(shown.progress))
</script>

{#if shown.status !== null || idle === 'disable'}
  <!-- The row states its own width as both a cap and a flex basis, so a wrapping
       parent lays two rows side by side while both fit and wraps them when they do
       not, without either row stretching to fill a line it has to itself. -->
  <div
    class="items-top flex max-w-150 basis-150 flex-wrap gap-x-4 gap-y-2 h-16 {_class}"
    transition:slide
  >
    <div class="flex w-full flex-col items-start sm:w-28 sm:items-center justify-center">
      <div class="text-base text-nowrap">{label}</div>
      <div
        class="pointer-events-none {statusChipClass} {shown.status?.class ??
          statusToneColors.neutral.chip}"
      >
        {shown.status?.label ?? 'None'}
      </div>
    </div>

    <div class="flex min-w-0 flex-1 flex-col h-full justify-end gap-2">
      {@render header?.()}
      <!-- The bar and its counts share one grid cell with an invisible copy of
           their lines. The copy keeps the height of the row the same while the
           bar slides in, shows or hides. -->
      <div class="grid">
        <div class="invisible col-start-1 row-start-1 flex flex-col gap-2" aria-hidden="true">
          <div class="text-base"><span class="font-dm-mono font-bold">&nbsp;</span></div>
          <div class="h-2"></div>
        </div>
        {#if isBarShown}
          <div
            class="col-start-1 row-start-1 flex flex-col gap-2"
            transition:slide
            data-testid="commit-progress-bar"
          >
            <div class="text-base text-nowrap">
              {#if shown.progress}
                <!-- The counts name no category of their own; the tooltips carry what
                   they count, so the labels stay short enough to fit one line. -->
                <span
                  data-testid="box-label-completed"
                  class="cursor-help underline decoration-dotted underline-offset-4"
                  >Completed ops</span
                >
                <Tooltip placement="top">Operators that have been fully flushed</Tooltip>
                <span class="font-dm-mono font-bold">{formatQty(shown.progress.completed)}</span>
                out of
                <span class="font-dm-mono font-bold">{formatQty(total)}</span>
                <span class="ml-2 select-none">·</span>
                <span
                  data-testid="box-label-in-progress"
                  class="ml-2 cursor-help underline decoration-dotted underline-offset-4"
                  >In progress</span
                >
                <Tooltip placement="top">Operators currently being flushed</Tooltip>
                <span class="font-dm-mono font-bold">{formatQty(shown.progress.in_progress)}</span>
              {:else}
                <!-- Keeps the height of the line when there are no counts to show. -->
                <span class="font-dm-mono font-bold">&nbsp;</span>
              {/if}
            </div>
            <div class="relative" style:--bar-duration="{frame.durationMs}ms">
              <Progress class="h-2" value={frame.combinedPercent} max={100}>
                <Progress.Track class="bg-surface-600-400">
                  <Progress.Range class="bg-yellow-500 duration-(--bar-duration) ease-linear" />
                </Progress.Track>
              </Progress>
              <Progress
                class="absolute inset-x-0 bottom-0 h-2"
                value={frame.completedPercent}
                max={100}
              >
                <Progress.Track class="opacity-0"></Progress.Track>
                <Progress.Range
                  class="absolute inset-y-0 left-0 bg-success-500 duration-(--bar-duration) ease-linear"
                />
              </Progress>
            </div>
          </div>
        {/if}
      </div>
    </div>
  </div>
{/if}
