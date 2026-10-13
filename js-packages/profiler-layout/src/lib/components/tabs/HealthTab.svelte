<script lang="ts">
  import { PipelineEventHistory } from 'common-ui'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { pipelineEvents, collectedAt }: AnalysisTabProps = $props()
</script>

{#if pipelineEvents}
  <!-- A new bundle or timestamp closes the details drawer of the previous events. -->
  {#key pipelineEvents}
    <div class="flex min-h-0 flex-1 flex-nowrap overflow-hidden">
      <PipelineEventHistory
        events={pipelineEvents}
        loading={false}
        now={collectedAt ?? new Date()}
        localStorageKey="profiler/drawer/pipelineHealth"
      />
    </div>
  {/key}
{:else}
  <div class="flex h-full items-center justify-center text-sm text-surface-600-400">
    No pipeline events available in this bundle
  </div>
{/if}
