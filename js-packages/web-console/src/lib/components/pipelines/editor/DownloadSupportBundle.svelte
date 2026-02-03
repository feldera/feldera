<script lang="ts">
  import { Progress, Switch } from '@skeletonlabs/skeleton-svelte'
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { humanSize } from '$lib/functions/common/string'
  import type { SupportBundleOptions } from '$lib/services/pipelineManager'

  const { pipelineName }: { pipelineName: string } = $props()

  const api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  let isDownloading = $state(false)
  let cancelDownload: (() => void) | null = null

  const submitHandler = async () => {
    isDownloading = true
    progress.reset()
    cancelDownload = null

    {
      const result = api.downloadPipelineSupportBundle(pipelineName, data, progress.onProgress)
      cancelDownload = () => {
        result.cancel()
        isDownloading = false
      }

      await result.dataPromise
    }
    isDownloading = false
    cancelDownload = null
    globalDialog.dialog = null
  }

  const defaultData: SupportBundleOptions = {
    circuit_profile: true,
    heap_profile: true,
    logs: true,
    metrics: true,
    pipeline_config: true,
    stats: true,
    system_config: true,
    dataflow_graph: true,
    collect: true
  }
  let data: SupportBundleOptions = $state(defaultData)

  const fields = {
    circuit_profile: {
      label: 'Circuit profile',
      description: 'Include circuit profiling data'
    },
    heap_profile: {
      label: 'Heap profile',
      description: 'Include heap profiling data'
    },
    logs: {
      label: 'Logs',
      description: 'Include logs'
    },
    metrics: {
      label: 'Metrics',
      description: 'Include metrics'
    },
    pipeline_config: {
      label: 'Pipeline config',
      description: 'Include pipeline config'
    },
    stats: {
      label: 'Stats',
      description: 'Include stats'
    },
    system_config: {
      label: 'System config',
      description: 'Include system config'
    },
    dataflow_graph: {
      label: 'Dataflow graph',
      description: 'Include SQL program dataflow graph'
    },
    collect: {
      label: 'Collect new data',
      description: 'Collect latest data. May take some time.'
    }
  }

  const progress = useDownloadProgress()
</script>

<button
  class="btn preset-tonal-surface"
  onclick={async () => {
    data = defaultData
    globalDialog.dialog = supportBundleDialog
  }}
>
  <span class="fd fd-file-down text-[20px] text-primary-500"></span>
  Support bundle
</button>
<Tooltip placement="top" class="w-[204px] text-wrap">
  Generate a bundle with logs, metrics and configs to help troubleshoot issues
</Tooltip>

{#snippet supportBundleDialog()}
  <GenericDialog
    onApply={submitHandler}
    onClose={() => {
      cancelDownload?.()
      globalDialog.dialog = null
    }}
    confirmLabel="Download"
    disabled={isDownloading}
  >
    {#snippet title()}
      Download Support Bundle
    {/snippet}
    <div class="-mt-2 pb-2 font-semibold">{pipelineName}</div>
    {#if isDownloading}
      <div class="flex flex-col items-center gap-3 py-4">
        <Progress class="h-1" value={progress.percent} max={100}>
          <Progress.Track>
            <Progress.Range class="bg-primary-500" />
          </Progress.Track>
        </Progress>
        <div class="flex w-full justify-between gap-2">
          <span>Downloading support bundle...</span>
          {#if progress.percent}
            <span>{humanSize(progress.bytes.downloaded)} / {humanSize(progress.bytes.total)}</span>
          {/if}
        </div>
      </div>
    {:else}
      Select the details you want to include in the bundle
      {@render supportBundleForm()}
    {/if}
  </GenericDialog>
{/snippet}

{#snippet supportBundleForm()}
  <!-- svelte-ignore a11y_no_noninteractive_element_interactions -->
  <form class="flex flex-col gap-3">
    {#each Object.entries(fields) as [key, { label }]}
      <div class="flex items-center gap-4">
        <input
          type="checkbox"
          id={key}
          bind:checked={data[key as keyof SupportBundleOptions]}
          class="checkbox"
        />
        <div class="flex flex-col">
          <label for={key} class="cursor-pointer font-medium">{label}</label>
        </div>
      </div>
    {/each}
  </form>
{/snippet}
