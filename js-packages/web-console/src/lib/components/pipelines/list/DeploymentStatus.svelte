<script lang="ts">
  import { pipelineStatusColor } from '#lib/functions/pipelineStatusColor.ts'
  import { getDeploymentStatusLabel } from '#lib/functions/pipelines/status.ts'
  import type { PipelineStatus } from '#lib/services/pipelineManager.ts'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()

  const chipClass = $derived(pipelineStatusColor(status).chip)
</script>

<div class={'pointer-events-none chip text-[0.66rem] uppercase ' + chipClass + ' ' + _class}>
  {getDeploymentStatusLabel(status)}
</div>
