<script lang="ts">
  import { pipelineStatusColor, statusChipClass } from '$lib/functions/pipelineStatusColor'
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import type { PipelineStatus } from '$lib/services/pipelineManager'

  const {
    status,
    class: _class = '',
    'data-testid': testid,
    deleted
  }: {
    status: PipelineStatus
    class?: string
    'data-testid'?: string
    deleted?: boolean
  } = $props()
  const chipClass = $derived(pipelineStatusColor(deleted ? 'SystemError' : status).chip)
</script>

<div data-testid={testid} class="{statusChipClass} {chipClass} {_class}">
  {deleted ? 'Deleted' : getPipelineStatusLabel(status)}
</div>
