<script lang="ts">
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import type { CheckpointActivity, CheckpointMetadata } from '$lib/services/manager'

  const {
    pipelineName,
    checkpoints,
    checkpointActivity,
    onShowCheckpoints
  }: {
    pipelineName: string
    checkpoints: CheckpointMetadata[]
    checkpointActivity: CheckpointActivity
    onShowCheckpoints: () => void
  } = $props()

  const api = usePipelineManager()
  const isEnterprise = usePremiumFeatures()

  let checkpointRequested = $state(false)

  const handleCheckpoint = () => {
    checkpointRequested = true
    api.checkpointPipeline(pipelineName).finally(() => {
      checkpointRequested = false
    })
  }
</script>

{#if checkpoints.length > 0}
  <button class="-my-2 btn preset-outlined-surface-800-200 btn-sm" onclick={onShowCheckpoints}>
    {checkpoints.length} checkpoints, total: {humanSize(
      checkpoints.reduce((s, c) => s + (c.size ?? 0), 0)
    )}
  </button>
{:else if isEnterprise.value && checkpointActivity.status === 'idle'}
  <button
    class="-my-2 btn preset-outlined-surface-800-200 btn-sm"
    onclick={handleCheckpoint}
    disabled={checkpointRequested}
  >
    {checkpointRequested ? 'Requesting...' : 'Make first checkpoint'}
  </button>
{/if}
