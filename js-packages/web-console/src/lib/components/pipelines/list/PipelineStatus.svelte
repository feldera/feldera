<script lang="ts">
  import { pipelineStatusColor } from '$lib/functions/pipelineStatusColor'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import type { PipelineStatus } from '$lib/services/pipelineManager'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(pipelineStatusColor(status).chip)
</script>

<div class={'pointer-events-none chip w-32 uppercase ' + chipClass + ' ' + _class}>
  {getPipelineStatusLabel(status)}
</div>
