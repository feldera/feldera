<script lang="ts">
  import Dayjs from 'dayjs'
  import { slide } from 'svelte/transition'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import { useElapsedTime } from '$lib/functions/format'
  import type { PipelineMetrics } from '$lib/functions/pipelineMetrics'
  import type { CheckpointStatus } from '$lib/services/manager'

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

  // Only show the failure if it's more recent than the last success.
  // A success with a higher sequence number means the pipeline recovered.
  const showFailure = $derived(
    failure != null && (lastSuccess == null || failure.sequence_number > lastSuccess)
  )

  const isPermanentlyUnavailable = $derived(permanentErrors != null && permanentErrors.length > 0)

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
    if ('UnsupportedInputEndpoint' in e)
      return `Input "${e.UnsupportedInputEndpoint}" does not support checkpointing`
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

{#if isPermanentlyUnavailable || activity.status !== 'idle' || showFailure}
  <div
    class="bg-white-dark scrollbar flex h-20 w-96 overflow-clip overflow-x-auto rounded pt-1"
    transition:slide={{ axis: 'x' }}
  >
    <table class="table h-min rounded text-base">
      <thead>
        <tr>
          <th class="w-36">Checkpoint status</th>
          <th>Details</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {#if isPermanentlyUnavailable}
          <tr>
            <td>
              <div class="pointer-events-none chip w-full bg-error-50-950 uppercase">
                Unavailable
              </div>
              <Tooltip placement="top">
                <ul class="list-disc pl-4">
                  {#each permanentErrors ?? [] as err}
                    <li>{formatPermanentError(err)}</li>
                  {/each}
                </ul>
              </Tooltip>
            </td>
            <td class="text-surface-500"> Checkpointing not supported </td>
            <td></td>
          </tr>
        {:else if activity.status === 'delayed'}
          <tr>
            <td>
              <div class="pointer-events-none chip w-full bg-tertiary-50-950 uppercase">
                Delayed
              </div>
            </td>
            <td class="font-dm-mono">
              {delayDuration}s
            </td>
            <td class="text-surface-500">
              {activity.reasons.length} reason{activity.reasons.length !== 1 ? 's' : ''} (hover to view)
              <Tooltip placement="top">
                <ul class="list-disc pl-4">
                  {#each activity.reasons as reason}
                    <li>{formatTemporaryReason(reason)}</li>
                  {/each}
                </ul>
              </Tooltip>
            </td>
          </tr>
        {:else if activity.status === 'in_progress'}
          <tr>
            <td>
              <div class="pointer-events-none chip w-full bg-warning-200-800 uppercase">
                In Progress
              </div>
            </td>
            <td class="font-dm-mono">
              {formatElapsedTime(new Date(activity.started_at))} since {Dayjs(
                activity.started_at
              ).format('HH:mm:ss')}
            </td>
            <td></td>
          </tr>
        {:else if showFailure && failure}
          <tr>
            <td>
              <div class="pointer-events-none chip w-full bg-error-50-950 uppercase">
                Last Failed
              </div>
              <Tooltip placement="top">
                <div class="max-w-xs whitespace-pre-wrap">{failure.error}</div>
              </Tooltip>
            </td>
            <td class="">
              <span class="font-dm-mono">#{failure.sequence_number}</span>
              at {Dayjs(failure.failed_at).format('HH:mm:ss')}
            </td>
            <td></td>
          </tr>
        {/if}
      </tbody>
    </table>
  </div>
{/if}
