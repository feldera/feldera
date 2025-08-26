<script lang="ts">
  import { getDeploymentStatusLabel } from '$lib/functions/pipelines/status'
  import { pipelineStatusColor } from '$lib/functions/pipelineStatusColor'
  import { type PipelineStatus } from '$lib/services/pipelineManager'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()

  const chipClass = $derived(pipelineStatusColor(status).deploymentChip)
</script>

<div class={'chip pointer-events-none text-[0.66rem] uppercase ' + chipClass + ' ' + _class}>
  {getDeploymentStatusLabel(status)}
</div>
