<script lang="ts">
  import { slide } from 'svelte/transition'
  import ClickFeedback from '$lib/components/common/ClickFeedback.svelte'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { usePremiumFeatures } from '$lib/compositions/usePremiumFeatures.svelte'
  import { uuidV7Timestamp } from '$lib/functions/common/date'
  import { humanSize } from '$lib/functions/common/string'
  import { formatQty } from '$lib/functions/format'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { CheckpointMetadata, CheckpointStatus } from '$lib/services/manager'
  import CheckpointActivityStatus from './CheckpointActivityStatus.svelte'
  import CheckpointDialog from './CheckpointDialog.svelte'

  const {
    pipelineName,
    checkpoints,
    metrics,
    checkpointStatus,
    onShowCheckpoints
  }: {
    pipelineName: string
    checkpoints: CheckpointMetadata[]
    metrics: { current: PipelineMetrics }
    checkpointStatus: CheckpointStatus | null
    onShowCheckpoints: () => void
  } = $props()

  const lastCheckpointAt = $derived(
    uuidV7Timestamp(checkpoints.at(-1)?.uuid ?? '')?.toDate() ?? null
  )

  const api = usePipelineManager()
  const isEnterprise = usePremiumFeatures()
  const { formatElapsedTime } = useElapsedTime()
  const globalDialog = useGlobalDialog()

  let clickFeedback = $state<() => void>()

  const last = $derived(checkpoints.at(-1))
  const totalBytes = $derived(checkpoints.reduce((sum, cp) => sum + (cp.size ?? 0), 0))

  const checkpointFailure = $derived(checkpointStatus?.failure)
  const lastCheckpointSuccess = $derived(checkpointStatus?.success)
  const showCheckpointFailure = $derived(
    checkpointFailure != null &&
      (lastCheckpointSuccess == null || checkpointFailure.sequence_number > lastCheckpointSuccess)
  )
  const isPermanentlyUnavailable = $derived.by(() => {
    const errors = metrics.current.permanent_checkpoint_errors
    return (
      errors != null &&
      errors.length > 0 &&
      !(errors.length === 1 && errors[0] === 'EnterpriseFeature')
    )
  })
  const activityVisible = $derived(
    isPermanentlyUnavailable ||
      metrics.current.checkpoint_activity.status !== 'idle' ||
      showCheckpointFailure
  )
</script>

{#snippet checkpointDialog()}
  <CheckpointDialog
    onConfirm={() => {
      clickFeedback?.()
      api.checkpointPipeline(pipelineName)
    }}
  />
{/snippet}

{#snippet allCheckpoints()}
  {#if checkpoints.length > 0}
    <div class="ml-auto flex items-center gap-4">
      <span class=""
        >{checkpoints.length}
        {checkpoints.length === 1 ? 'checkpoint' : 'checkpoints'} using {humanSize(
          totalBytes
        )}</span
      >
      <button class="btn preset-filled-surface-100-900" onclick={onShowCheckpoints}>
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
      <CheckpointActivityStatus
        {metrics}
        {checkpointStatus}
        {isPermanentlyUnavailable}
        showFailure={showCheckpointFailure}
      />
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
          {formatQty(last.processed_records)}
        </div>
      </div>
      <div class="flex flex-col">
        <div class="text-sm text-nowrap">Steps</div>
        <div class="pt-2 font-dm-mono text-nowrap">
          {formatQty(last.steps)}
        </div>
      </div>
      {@render allCheckpoints()}
    {:else if isEnterprise.value}
      <div class="hr"></div>
      <ClickFeedback
        active={metrics.current.checkpoint_activity.status !== 'idle'}
        bind:clickFeedback
      >
        {#snippet children({ active })}
          <button
            class="ml-auto btn preset-outlined-primary-500 btn-sm"
            onclick={() => (globalDialog.dialog = checkpointDialog)}
            disabled={active}
          >
            {active ? 'Creating checkpoint...' : 'Create first checkpoint'}
          </button>
        {/snippet}
      </ClickFeedback>
      {@render allCheckpoints()}
    {/if}
  </div>
{/if}
