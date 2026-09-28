<script lang="ts" generics="K">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { Tooltip } from 'common-ui'
  import { type Snippet, untrack } from 'svelte'
  import { slide } from 'svelte/transition'
  import { useIsScreenSm } from '$lib/compositions/layout/useIsMobile.svelte'
  import type { ProgressSample } from '$lib/compositions/progressBarAnimation'
  import { useProgressBarAnimation } from '$lib/compositions/useProgressBarAnimation.svelte'
  import { formatQty } from '$lib/functions/format'
  import type { CommitProgressSummary } from '$lib/services/manager'

  let {
    label,
    status,
    progress,
    idle = 'hide',
    resetKey,
    pollPeriodMs = 2000,
    detail,
    class: _class = ''
  }: {
    /** Title above the status chip, e.g. "Transaction" or "Bootstrapping". */
    label: string
    /** Chip contents, or `null` while the activity this row tracks is idle. */
    status: Status | null
    progress: CommitProgressSummary | null | undefined
    /**
     * How to render an idle row: 'hide' removes it, 'disable' keeps it in place
     * with a "None" chip so a neighboring row's progress bar never shifts.
     */
    idle?: 'disable' | 'hide'
    /**
     * Identifies the activity, for example a transaction ID. When the key
     * changes, the bar moves to 100% and then starts again from 0. Thus, the
     * user sees that one activity stopped and a different activity started.
     */
    resetKey?: K
    /** The time between two polls of `progress`. The bar moves during this period. */
    pollPeriodMs?: number
    /**
     * More data that the row shows near the chip, for example a transaction ID.
     * It receives the `resetKey` of the activity on the screen. While the bar
     * of the old activity moves to 100%, this key is older than the latest key.
     */
    detail?: Snippet<[K]>
    class?: string
  } = $props()

  type Status = { label: string; class: string }

  /** The data that the row shows near the bar. It changes at the same time as the bar. */
  type Shown = { key: K; status: Status | null; progress: CommitProgressSummary | null | undefined }

  const operatorTotal = (progress: CommitProgressSummary | null | undefined) =>
    progress ? progress.completed + progress.in_progress + progress.remaining : 0

  const toSample = (polled: Shown): ProgressSample<Shown> => {
    const { progress } = polled
    const total = operatorTotal(progress)
    const inProgressFraction =
      progress && progress.in_progress_total_records > 0
        ? progress.in_progress_processed_records / progress.in_progress_total_records
        : 0
    return {
      key: polled.key,
      active: polled.status !== null,
      combinedPercent:
        total > 0 && progress
          ? ((progress.completed + progress.in_progress * inProgressFraction) / total) * 100
          : null,
      completedPercent: total > 0 && progress ? (progress.completed / total) * 100 : 0,
      data: polled
    }
  }

  const polledSample = $derived(toSample({ key: resetKey as K, status, progress }))

  // The bar and all labels below render the frame only. They do not use the
  // props from the poll directly.
  const bar = useProgressBarAnimation(
    () => polledSample,
    untrack(() => pollPeriodMs)
  )
  const frame = $derived(bar.current)

  const shown = $derived(frame.sample.data)
  const isIdle = $derived(!frame.sample.active)
  const total = $derived(operatorTotal(shown.progress))

  // On sm screens and larger, the detail is near the title. On smaller screens,
  // it is on the line with the counts. A snippet cannot render in two places at
  // the same time. Thus, the script selects the position, not CSS.
  const isScreenSm = useIsScreenSm()
  const showDetail = $derived(detail !== undefined && !isIdle)
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
          <span class="w-0 font-dm-mono">{@render detail!(shown.key)}</span>
        {/if}
      </div>
      <div class="flex flex-nowrap items-center justify-center">
        <div></div>
        <div
          class="pointer-events-none chip tracking-wider uppercase {shown.status?.class ??
            'bg-surface-100-900 text-surface-600-400'}"
        >
          {shown.status?.label ?? 'None'}
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
            >{#if showDetail}{@render detail!(shown.key)}{/if}</span
          >
        {/if}
        {#if shown.progress && !isIdle}
          <!-- The counts name no category of their own; the tooltips carry what
               they count, so the labels stay short enough to fit one line. -->
          <span
            data-testid="box-label-completed"
            class="cursor-help underline decoration-dotted underline-offset-4">Completed ops</span
          >
          <Tooltip placement="top">Operators that have been fully flushed</Tooltip>
          <span class="font-dm-mono font-bold">{formatQty(shown.progress.completed)}</span> out of
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
          <span class="font-dm-mono font-bold">&nbsp;</span>
        {/if}
      </div>
      <div class="pt-2">
        <div
          class="relative {isIdle ? 'opacity-50' : ''}"
          style:--bar-duration="{frame.durationMs}ms"
        >
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
    </div>
  </div>
{/if}
