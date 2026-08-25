<script lang="ts">
  import { emptySearchState, LogView, type SearchState } from 'common-ui'
  import WarningBanner from '$lib/components/pipelines/editor/WarningBanner.svelte'
  import { humanSize } from '$lib/functions/common/string'

  const {
    logs,
    search = emptySearchState,
    onStickToBottomChange,
    onMatchCountChange
  }: {
    logs: { rows: string[]; totalSkippedBytes: number; firstRowIndex: number }
    /** Current search state (see {@link SearchState}), advanced by the host. */
    search?: SearchState
    /** Forwarded to {@link LogView}; fires when stick-to-bottom toggles. */
    onStickToBottomChange?: (stickToBottom: boolean) => void
    /** Forwarded to {@link LogView}; reports the current search match count. */
    onMatchCountChange?: (count: number) => void
  } = $props()
</script>

<LogView
  lines={logs.rows}
  firstLineIndex={logs.firstRowIndex}
  {search}
  streaming
  {onStickToBottomChange}
  {onMatchCountChange}
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
</LogView>
