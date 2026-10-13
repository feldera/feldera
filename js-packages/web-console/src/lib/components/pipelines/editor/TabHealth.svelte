<script lang="ts">
  import { PipelineEventHistory } from 'common-ui'
  import { useElapsedTime } from '$lib/compositions/common/useElapsedTime'
  import { useInterval } from '$lib/compositions/common/useInterval.svelte'
  import { ServerDate } from '$lib/compositions/serverTime'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import type { PipelineMonitorEventSelectedInfo } from '$lib/services/manager'
  import type { ExtendedPipeline } from '$lib/services/pipelineManager'

  let {
    pipeline,
    deleted = false
  }: {
    pipeline: { current: ExtendedPipeline }
    deleted?: boolean
  } = $props()

  const pipelineName = $derived(pipeline.current.name)
  const api = usePipelineManager()

  let events: PipelineMonitorEventSelectedInfo[] | null = $state(null)
  let lastUpdated = $state<Date | null>(null)
  const { formatUpdatedAgo } = useElapsedTime()

  async function refreshEvents(name: string) {
    const fetched = await api.getPipelineEvents(name)
    if (name === pipelineName) {
      events = fetched
      lastUpdated = new ServerDate()
    }
  }

  // useInterval's $state(f()) is the initial fetch; subsequent ticks are periodic polls.
  useInterval(() => refreshEvents(pipelineName), 30_000)

  // Fires only on pipeline name changes. The first run is skipped because useInterval
  // already fired the initial fetch via $state(f()) before any effects run.
  let nameEffectInitialized = false
  $effect(() => {
    const name = pipelineName
    if (!nameEffectInitialized) {
      nameEffectInitialized = true
      return
    }
    events = null
    lastUpdated = null
    refreshEvents(name)
  })
</script>

<div data-testid="box-pipeline-health" class="flex h-full flex-nowrap overflow-hidden">
  <PipelineEventHistory
    {events}
    loading={!events && !deleted}
    now={lastUpdated ?? new ServerDate()}
    updatedText={formatUpdatedAgo(lastUpdated)}
    loadEvent={(eventId) => api.getPipelineEvent(pipelineName, eventId)}
    localStorageKey="layout/drawer/pipelineHealth"
  />
</div>
