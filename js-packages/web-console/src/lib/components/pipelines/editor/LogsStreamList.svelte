<script lang="ts">
  import { emptySearchState, LogList, type SearchState, stripAnsi } from 'common-ui'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { humanSize } from '$lib/functions/common/string'

  const {
    logs,
    search = emptySearchState,
    onSearchShortcut
  }: {
    logs: { rows: string[]; totalSkippedBytes: number; firstRowIndex: number }
    /** Current search; the host advances `occurrenceIndex` (wraps modulo match count) and
     *  sets `pattern = null` to clear any highlight. */
    search?: SearchState
    /** Forwarded to {@link LogList}; invoked on Ctrl-F / Cmd-F inside the list. */
    onSearchShortcut?: () => void
  } = $props()

  // Streaming-log rows from the manager already carry trailing newlines, so the default
  // `\n`-join in LogList would double them. Override to join with `''` and reconstitute the
  // original byte stream verbatim.
  function getCopyContent(
    slice: 'all' | { start: { row: number; col: number }; end: { row: number; col: number } }
  ): string {
    if (slice === 'all') {
      return logs.rows.map(stripAnsi).join('')
    }
    const result = logs.rows.slice(slice.start.row, slice.end.row + 1).map(stripAnsi)
    result[0] = result[0].slice(slice.start.col)
    result[result.length - 1] = result[result.length - 1].slice(
      0,
      slice.end.col - (slice.start.row === slice.end.row ? slice.start.col : 0)
    )
    return result.join('')
  }
</script>

<LogList
  lines={logs.rows}
  firstLineIndex={logs.firstRowIndex}
  {search}
  streaming
  {getCopyContent}
  {onSearchShortcut}
  class="bg-white-dark rounded pl-2"
>
  {#snippet header()}
    {#if logs.totalSkippedBytes}
      <WarningBanner>
        Receiving logs faster than can be displayed. Skipping some logs to keep up, {humanSize(
          logs.totalSkippedBytes
        )} in total.
      </WarningBanner>
    {/if}
  {/snippet}
</LogList>
