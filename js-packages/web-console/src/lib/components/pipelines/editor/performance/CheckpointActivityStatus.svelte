<script lang="ts" module>
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { CheckpointStatus } from '$lib/services/manager'

  export const isCheckpointActivityVisible = (
    metrics: { current: PipelineMetrics },
    checkpointStatus: CheckpointStatus | null
  ) => {
    const activity = metrics.current.checkpoint_activity
    const permanentErrors = metrics.current.permanent_checkpoint_errors
    const failure = checkpointStatus?.failure
    const lastSuccess = checkpointStatus?.success
    const showFailure =
      failure != null && (lastSuccess == null || failure.sequence_number > lastSuccess)
    const isPermanentlyUnavailable =
      permanentErrors != null &&
      permanentErrors.length > 0 &&
      !(permanentErrors.length === 1 && permanentErrors[0] === 'EnterpriseFeature')
    return isPermanentlyUnavailable || activity.status !== 'idle' || showFailure
  }
</script>

<script lang="ts">
  import Dayjs from 'dayjs'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useElapsedTime } from '$lib/functions/format'

  const { formatElapsedTime } = useElapsedTime()

  let {
    metrics,
    checkpointStatus
  }: {
    metrics: { current: PipelineMetrics }
    checkpointStatus: CheckpointStatus | null
  } = $props()

  const activity = $derived(metrics.current.checkpoint_activity)
  const permanentErrors = $derived(metrics.current.permanent_checkpoint_errors)
  const failure = $derived(checkpointStatus?.failure)
  const lastSuccess = $derived(checkpointStatus?.success)

  const showFailure = $derived(
    failure != null && (lastSuccess == null || failure.sequence_number > lastSuccess)
  )

  const isPermanentlyUnavailable = $derived(
    permanentErrors != null &&
      permanentErrors.length > 0 &&
      !(permanentErrors.length === 1 && permanentErrors[0] === 'EnterpriseFeature')
  )

  const delayDuration = $derived(
    activity.status === 'delayed' ? Dayjs().diff(Dayjs(activity.delayed_since), 'second') : 0
  )

  const formatPermanentError = (e: (typeof permanentErrors & {})[number]) => {
    if (typeof e === 'string')
      return e === 'StorageRequired'
        ? 'Storage must be configured'
        : e === 'EnterpriseFeature'
          ? 'Enterprise feature'
          : e
    if ('UnsupportedInputEndpoint' in e) {
      return `Input "${e.UnsupportedInputEndpoint}" does not support checkpointing`
    }
    return String(e)
  }

  const formatTemporaryReason = (r: unknown) => {
    if (typeof r === 'string') {
      switch (r) {
        case 'Replaying':
          return 'Replaying journal'
        case 'Bootstrapping':
          return 'Bootstrapping'
        case 'TransactionInProgress':
          return 'Transaction in progress'
        case 'Coordination':
          return 'Waiting for coordinator'
        default:
          return r
      }
    }
    if (r && typeof r === 'object' && 'InputEndpointBarrier' in r)
      return `Input "${(r as { InputEndpointBarrier: string }).InputEndpointBarrier}" barrier`
    return String(r)
  }
</script>

{#if isPermanentlyUnavailable}
  {@const errors = permanentErrors ?? []}
  {@const firstReason = formatPermanentError(errors[0])}
  {@const extraCount = errors.length - 1}
  <div class="flex items-center gap-3">
    <div class="pointer-events-none chip bg-error-50-950 uppercase">Unavailable</div>
    <div class="text-sm">
      {firstReason}{#if extraCount > 0}
        {' '}+ {extraCount} other{extraCount !== 1 ? 's' : ''}{/if}
      {#if extraCount > 0}
        <Tooltip placement="top">
          <ul class="list-disc pl-4">
            {#each errors as err}
              <li>{formatPermanentError(err)}</li>
            {/each}
          </ul>
        </Tooltip>
      {/if}
    </div>
  </div>
{:else if activity.status === 'in_progress'}
  <div class="flex items-center gap-3">
    <div class="pointer-events-none chip bg-warning-200-800 uppercase">Checkpoint in progress</div>
    <div class="font-dm-mono text-sm">
      for {formatElapsedTime(new Date(activity.started_at))} since {Dayjs(
        activity.started_at
      ).format('HH:mm:ss')}
    </div>
  </div>
{:else if activity.status === 'delayed'}
  <div class="flex items-center gap-3">
    <div class="pointer-events-none chip bg-tertiary-50-950 uppercase">Checkpoint delayed</div>
    <div class="font-dm-mono text-sm">for {delayDuration}s</div>
    <div class="text-sm">
      {#if activity.reasons.length === 1}
        Reason: {formatTemporaryReason(activity.reasons[0])}
      {:else}
        {activity.reasons.length} reasons (hover to view)
      {/if}
      <Tooltip placement="top">
        <ul class="list-disc pl-4">
          {#each activity.reasons as reason}
            <li>{formatTemporaryReason(reason)}</li>
          {/each}
        </ul>
      </Tooltip>
    </div>
  </div>
{:else if showFailure && failure}
  <div class="flex items-center gap-3">
    <div class="pointer-events-none chip bg-error-50-950 uppercase">Last failed</div>
    <Tooltip placement="top">
      <div class="max-w-xs whitespace-pre-wrap">{failure.error}</div>
    </Tooltip>
    <div class="text-sm">
      <span class="font-dm-mono">#{failure.sequence_number}</span>
      at {Dayjs(failure.failed_at).format('HH:mm:ss')}
    </div>
  </div>
{/if}
