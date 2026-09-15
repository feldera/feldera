<script lang="ts">
  import {
    type CopySlice,
    emptySearchState,
    LogList,
    type SearchState,
    sliceLinesForCopy
  } from 'common-ui'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { humanSize } from '$lib/functions/common/string'

  const {
    logs,
    search = emptySearchState,
    onStickToBottomChange,
    onMatchCountChange
  }: {
    logs: {
      rows: string[]
      totalSkippedBytes: number
      totalDiscardedLines: number
      firstRowIndex: number
    }
    /** Current search state (see {@link SearchState}), advanced by the host. */
    search?: SearchState
    /** Forwarded to {@link LogList}; fires when stick-to-bottom toggles. */
    onStickToBottomChange?: (stickToBottom: boolean) => void
    /** Forwarded to {@link LogList}; reports the current search match count. */
    onMatchCountChange?: (count: number) => void
  } = $props()

  const getCopyContent = (slice: CopySlice) =>
    // Manager rows already carry trailing newlines; join with '' so copy doesn't double them.
    sliceLinesForCopy(logs.rows, slice, '')
</script>

<LogList
  lines={logs.rows}
  firstLineIndex={logs.firstRowIndex}
  {search}
  streaming
  {getCopyContent}
  {onStickToBottomChange}
  {onMatchCountChange}
  class="bg-white-dark rounded pl-2"
>
  {#snippet header()}
    {#if logs.totalDiscardedLines}
      <WarningBanner>
        {logs.totalDiscardedLines.toLocaleString()} earlier log lines are no longer available. The pipeline
        produced more logs than the server keeps.
      </WarningBanner>
    {/if}
    {#if logs.totalSkippedBytes}
      <WarningBanner>
        Receiving logs faster than can be displayed. Skipping some logs to keep up, {humanSize(
          logs.totalSkippedBytes
        )} in total.
      </WarningBanner>
    {/if}
  {/snippet}
</LogList>
