<script lang="ts">
  import PipelineMemoryGraph from '$lib/components/layout/pipelines/PipelineMemoryGraph.svelte'
  import { useAggregatePipelineStats } from '$lib/compositions/useAggregatePipelineStats.svelte'

  let { pipelineName }: { pipelineName: string } = $props()

  const pipelineStats = useAggregatePipelineStats(pipelineName, 'Running', 1000, 5000)
</script>

<!-- <div>TabPerformance {pipelineName}</div> -->
<!-- <div>{JSON.stringify(pipelineStats.metrics.global)}</div> -->
<div class="relative h-full w-full">
  <div class="absolute h-full w-full">
    some content
    <!-- <PipelineMemoryGraph metrics={pipelineStats.metrics}></PipelineMemoryGraph> -->
  </div>
</div>
