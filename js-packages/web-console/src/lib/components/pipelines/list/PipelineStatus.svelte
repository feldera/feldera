<script lang="ts">
  import { pipelineStatusColor } from '$lib/functions/pipelineStatusColor'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import type { PipelineStatus } from '$lib/services/pipelineManager'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(pipelineStatusColor(status).chip)
</script>

<div class={'chip w-28 uppercase transition-none ' + chipClass + ' ' + _class}>
  {getPipelineStatusLabel(status)}
</div>
