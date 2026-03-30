<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import { humanSize } from '$lib/functions/common/string'

  const {
    progress,
    label
  }: {
    progress: { percent: number | null; bytes: { downloaded: number; total: number } }
    label: string
  } = $props()
</script>

<div class="flex flex-col items-center gap-3 py-4">
  <Progress class="h-1" value={progress.percent} max={100}>
    <Progress.Track>
      <Progress.Range class="bg-primary-500" />
    </Progress.Track>
  </Progress>
  <div class="flex w-full justify-between gap-2">
    <span>{label}</span>
    {#if progress.bytes.downloaded > 0}
      <span>
        {humanSize(progress.bytes.downloaded)}{progress.bytes.total > 0
          ? ` / ${humanSize(progress.bytes.total)}`
          : ''}
      </span>
    {/if}
  </div>
</div>
