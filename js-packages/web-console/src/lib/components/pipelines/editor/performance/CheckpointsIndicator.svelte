<script lang="ts">
  import { format } from 'd3-format'
  import { slide } from 'svelte/transition'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { useElapsedTime } from '$lib/functions/format'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { CheckpointMetadata, CheckpointStatus } from '$lib/services/manager'
  import CheckpointActivityStatus, {
    isCheckpointActivityVisible
  } from './CheckpointActivityStatus.svelte'

  const formatQty = (v: number) => format(',.0f')(v)

  const {
    pipelineName,
    checkpoints,
    metrics,
    checkpointStatus,
    lastCheckpointAt,
    onShowCheckpoints
  }: {
    pipelineName: string
    checkpoints: CheckpointMetadata[]
    metrics: { current: PipelineMetrics }
    checkpointStatus: CheckpointStatus | null
    lastCheckpointAt: Date | null
    onShowCheckpoints: () => void
  } = $props()

  const api = usePipelineManager()
  const isEnterprise = usePremiumFeatures()
  const { formatElapsedTime } = useElapsedTime()

  let checkpointRequested = $state(false)

  const handleCheckpoint = () => {
    checkpointRequested = true
    api.checkpointPipeline(pipelineName).finally(() => {
      checkpointRequested = false
    })
  }

  const last = $derived(checkpoints.at(-1))
  const activityVisible = $derived(isCheckpointActivityVisible(metrics, checkpointStatus))
</script>

{#snippet allCheckpoints()}
  {#if checkpoints.length > 0}
    <div class="ml-auto">
      <button class="btn preset-outlined-surface-800-200 btn-sm" onclick={onShowCheckpoints}>
        All checkpoints
      </button>
    </div>
  {/if}
  <div class="hr"></div>
{/snippet}

{#if activityVisible || last || isEnterprise.value}
  <div class="flex min-h-16 w-full flex-wrap items-center gap-x-8 gap-y-2" transition:slide>
    {#if activityVisible}
      <div class="hr"></div>
      <CheckpointActivityStatus {metrics} {checkpointStatus} />
      {@render allCheckpoints()}
    {:else if last}
      <div class="hr"></div>
      <div class="flex items-start gap-2">
        <div
          class="flex h-5 w-5 flex-none items-center justify-center rounded-full text-success-600"
          aria-hidden="true"
        >
          <span class="fd fd-check text-lg leading-none"></span>
        </div>
        <div class="flex flex-col">
          <div class="text-sm text-nowrap">
            Last checkpoint{lastCheckpointAt
              ? `: ${formatElapsedTime(lastCheckpointAt, 'dhm').trim()} ago`
              : ''}
          </div>
          <div class="pt-2 text-nowrap">
            {humanSize(last.size ?? 0)}
            <span class="text-surface-500">·</span>
            <button
              type="button"
              class="anchor underline-offset-2 hover:underline"
              onclick={onShowCheckpoints}
            >
              Show
            </button>
          </div>
        </div>
      </div>
      <div class="flex flex-col">
        <div class="text-sm text-nowrap">Fingerprint</div>
        <div class="pt-2 font-dm-mono text-nowrap">
          {last.fingerprint}
        </div>
      </div>
      <div class="flex flex-col">
        <div class="text-sm text-nowrap">Processed records</div>
        <div class="pt-2 font-dm-mono text-nowrap">
          {last.processed_records != null ? formatQty(last.processed_records) : '—'}
        </div>
      </div>
      <div class="flex flex-col">
        <div class="text-sm text-nowrap">Steps</div>
        <div class="pt-2 font-dm-mono text-nowrap">
          {last.steps != null ? formatQty(last.steps) : '—'}
        </div>
      </div>
      {@render allCheckpoints()}
    {:else if isEnterprise.value}
      <div class="hr"></div>
      <button
        class="btn preset-outlined-surface-800-200 btn-sm"
        onclick={handleCheckpoint}
        disabled={checkpointRequested}
      >
        {checkpointRequested ? 'Requesting...' : 'Make first checkpoint'}
      </button>
      {@render allCheckpoints()}
    {/if}
  </div>
{/if}