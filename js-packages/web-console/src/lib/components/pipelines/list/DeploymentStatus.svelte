<script lang="ts">
  import { pipelineStatusColor } from '$lib/functions/pipelineStatusColor'
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import type { PipelineStatus } from '$lib/services/pipelineManager'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()

  const chipClass = $derived(pipelineStatusColor(status).deploymentChip)
</script>

<div class={'pointer-events-none chip text-[0.66rem] uppercase ' + chipClass + ' ' + _class}>
  {getDeploymentStatusLabel(status)}
</div>
