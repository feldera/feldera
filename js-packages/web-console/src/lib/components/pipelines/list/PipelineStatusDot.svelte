<script lang="ts">
  import { Tooltip } from 'common-ui'
  import { pipelineStatusColor } from '#lib/functions/pipelineStatusColor.ts'
  import { getPipelineStatusLabel } from '#lib/functions/pipelines/status.ts'
  import type { PipelineStatus } from '#lib/services/pipelineManager.ts'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(pipelineStatusColor(status).dot)
</script>

<div class="p-2">
  <div class="h-2 w-2 flex-none rounded-full {chipClass} {_class}"></div>
</div>
<Tooltip class="pointer-events-none ml-2 whitespace-nowrap" placement="left"
  >{getPipelineStatusLabel(status)}</Tooltip
>
