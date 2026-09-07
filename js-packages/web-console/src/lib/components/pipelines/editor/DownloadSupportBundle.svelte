<script lang="ts">
  import { Tooltip } from 'common-ui'
  import DownloadProgressDisplay from '$lib/components/dialogs/DownloadProgressDisplay.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { openRemoteBundleTab } from '$lib/compositions/profileBundleHandoff'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import type { SupportBundleOptions } from '$lib/services/pipelineManager'
  import SupportBundlePopup from './SupportBundlePopup.svelte'

  const { pipelineName }: { pipelineName: string } = $props()

  const api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const collectNewData = useLocalStorage('layout/pipelines/supportBundle/collect', true)

  let isDownloading = $state(false)
  let cancelDownload: (() => void) | null = null

  // The form exposes only the boolean toggles; `limit` is set server-side.
  type BundleToggles = Omit<SupportBundleOptions, 'limit'>
  const defaultData: BundleToggles = {
    circuit_profile: true,
    heap_profile: true,
    logs: true,
    metrics: true,
    pipeline_config: true,
    stats: true,
    system_config: true,
    dataflow_graph: true,
    pipeline_events: true,
    collect: true
  }
  let downloadData: BundleToggles = $state(defaultData)

  const progress = useDownloadProgress()

  const submitDownload = async () => {
    isDownloading = true
    progress.reset()
    cancelDownload = null

    const result = api.downloadPipelineSupportBundle(
      pipelineName,
      downloadData,
      progress.onProgress
    )
    cancelDownload = () => {
      result.cancel()
      isDownloading = false
    }
    await result.dataPromise

    isDownloading = false
    cancelDownload = null
    globalDialog.dialog = null
  }

  const fields = {
    circuit_profile: { label: 'Circuit profile' },
    heap_profile: { label: 'Heap profile' },
    logs: { label: 'Logs' },
    metrics: { label: 'Metrics' },
    pipeline_config: { label: 'Pipeline config' },
    stats: { label: 'Stats' },
    system_config: { label: 'System config' },
    dataflow_graph: { label: 'Dataflow graph' },
    pipeline_events: { label: 'Pipeline events' },
    collect: { label: 'Collect new data' }
  }
</script>

<!-- Split button: [View Support Bundle] [▾] -->
<SupportBundlePopup
  bind:collectNewData={collectNewData.value}
  onDownload={() => {
    downloadData = { ...defaultData, collect: collectNewData.value }
    globalDialog.dialog = supportBundleDialog
  }}
>
  {#snippet trigger(toggle)}
    <div class="flex">
      <!-- Primary action button -->
      <button
        class="btn h-8! rounded-r-none border-r-2 border-surface-50-950 bg-surface-100-900"
        onclick={() => openRemoteBundleTab(pipelineName, collectNewData.value)}
        title="Open profile viewer in a new tab"
        data-testid="btn-view-profile"
      >
        <span class="fd fd-file-search text-[20px] text-primary-500"></span>
        <span class="hidden sm:inline">View profile</span>
        <span class="inline sm:hidden">Profile</span>
      </button>
      <!-- Dropdown chevron -->
      <button
        class="btn-icon h-4! rounded-l-none bg-surface-100-900"
        onclick={toggle}
        aria-label="Support bundle options"
      >
        <span class="fd fd-chevron-down text-[24px]"></span>
      </button>
    </div>
  {/snippet}
</SupportBundlePopup>
<Tooltip placement="top" class="w-[240px] text-wrap">
  View pipeline support bundle (logs, metrics, profile) in a new tab
</Tooltip>

{#snippet supportBundleDialog()}
  <GenericDialog
    content={{
      title: 'Download Support Bundle',
      onSuccess: { name: 'Download', callback: submitDownload },
      onCancel: { callback: () => cancelDownload?.() }
    }}
    disabled={isDownloading}
  >
    <div class="-mt-2 pb-2 font-semibold">{pipelineName}</div>
    {#if isDownloading}
      <DownloadProgressDisplay {progress} label="Downloading support bundle..." />
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
          bind:checked={downloadData[key as keyof BundleToggles]}
          class="checkbox"
        />
        <label for={key} class="cursor-pointer font-medium">{label}</label>
      </div>
    {/each}
  </form>
{/snippet}
