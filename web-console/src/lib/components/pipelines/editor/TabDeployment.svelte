<script lang="ts">
    import { useSkeletonTheme } from '$lib/compositions/useSkeletonTheme.svelte'
    import type { ExtendedPipeline } from '$lib/services/pipelineManager'
    import { format } from 'd3-format'
    const formatQty = (v: number) => format(v >= 1000 ? '.3s' : '.0f')(v)
    let { pipeline }: { pipeline: { current: ExtendedPipeline } } = $props()
    const theme = useSkeletonTheme()
  </script>

  <div class="h-full gap-4 sm:pt-4">
    <div
      class="bg-white-dark h-full overflow-y-auto whitespace-pre-wrap rounded p-4 scrollbar"
      style="font-family: {theme.config.monospaceFontFamily}"
    >
      {pipeline.current.deploymentCheck || 'Pipeline is not deployed'}
    </div>
  </div>