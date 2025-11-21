<script lang="ts">
  import Tooltip from '$lib/components/common/Tooltip.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { SupportBundleOptions } from '$lib/services/pipelineManager'
  import { Progress, Switch } from '@skeletonlabs/skeleton-svelte'
  import { humanSize } from '$lib/functions/common/string'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'

  let { pipelineName }: { pipelineName: string } = $props()

  let api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const toast = useToast()
  let isDownloading = $state(false)

  const submitHandler = async () => {
    try {
      isDownloading = true
      progress.reset()
      await api.downloadPipelineSupportBundle(pipelineName, data, progress.onProgress)
      globalDialog.dialog = null
    } catch (error) {
      toast.toastError(new Error(`Failed to download support bundle: ${error}`))
    } finally {
      isDownloading = false
    }
  }

  let defaultData: SupportBundleOptions = {
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

  let fields = {
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

  let progress = useDownloadProgress()
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
<Tooltip
  placement="top"
  class="z-10 w-[204px] text-wrap rounded-container bg-white text-base text-surface-950-50 dark:bg-black"
>
  Generate a bundle with logs, metrics, and configs to help troubleshoot issues
</Tooltip>

{#snippet supportBundleDialog()}
  <GenericDialog
    onApply={submitHandler}
    onClose={() => {
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
        <Progress value={progress.percent} meterBg="bg-primary-500" base="h-2"></Progress>
        <div class="flex w-full justify-between gap-2">
          <span>Downloading support bundle...</span>
          {#if progress.percent}
            <span>{humanSize(progress.bytes.downloaded)} / {humanSize(progress.bytes.total)}</span>
          {/if}
        </div>
        Close the popup to continue the download in the background.
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
