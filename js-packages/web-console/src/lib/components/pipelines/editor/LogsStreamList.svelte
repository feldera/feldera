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
    onSearchShortcut
  }: {
    logs: { rows: string[]; totalSkippedBytes: number; firstRowIndex: number }
    /** Current search state (see {@link SearchState}), advanced by the host. */
    search?: SearchState
    /** Forwarded to {@link LogList}; invoked on Ctrl-F / Cmd-F inside the list. */
    onSearchShortcut?: () => void
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
