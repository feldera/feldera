<script lang="ts">
  import { getPipelineStatusLabel } from '$lib/functions/pipelines/status'
  import { pipelineStatusColor } from '$lib/functions/pipelineStatusColor'
  import { type PipelineStatus } from '$lib/services/pipelineManager'
  import { Tooltip } from '$lib/components/common/Tooltip.svelte'

  const { status, class: _class = '' }: { status: PipelineStatus; class?: string } = $props()
  const chipClass = $derived(pipelineStatusColor(status).dot)
</script>

<div class="p-2">
  <div class="h-3 w-3 flex-none rounded-full text-[0.66rem] uppercase {chipClass} {_class}"></div>
</div>
<Tooltip
  class="pointer-events-none ml-2 whitespace-nowrap rounded bg-white text-surface-950-50 dark:bg-black"
  placement="left">{getPipelineStatusLabel(status)}</Tooltip
>
