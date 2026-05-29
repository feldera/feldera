<script lang="ts">
  import { Tooltip } from 'common-ui'
  import { fly, slide } from 'svelte/transition'
  import Popup from '$lib/components/common/Popup.svelte'
  import DownloadProgressDisplay from '$lib/components/dialogs/DownloadProgressDisplay.svelte'
  import GenericDialog from '$lib/components/dialogs/GenericDialog.svelte'
  import { useGlobalDialog } from '$lib/compositions/layout/useGlobalDialog.svelte'
  import { useLocalStorage } from '$lib/compositions/localStore.svelte'
  import { openRemoteBundleTab, openUploadBundleTab } from '$lib/compositions/profileBundleHandoff'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import type { SupportBundleOptions } from '$lib/services/pipelineManager'
  import SupportBundleMenu from './SupportBundleMenu.svelte'

  const { pipelineName }: { pipelineName: string } = $props()

  const api = usePipelineManager()
  const globalDialog = useGlobalDialog()
  const toast = useToast()
  const collectNewData = useLocalStorage('layout/pipelines/supportBundle/collect', true)

  let isDownloading = $state(false)
  let cancelDownload: (() => void) | null = null

  // ── Download support bundle (existing dialog flow) ──────────────────────────

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

  // ── Upload bundle → open viewer tab ─────────────────────────────────────────

  // Two-step UX so the new tab opens from a direct user click (the only way
  // every browser reliably allows `window.open`):
  //   1. User picks a file → dropdown switches to a confirmation view.
  //   2. User clicks "View profile" → window.open runs synchronously in that
  //      click handler. The file read is kicked off in step 1 and awaited
  //      after the tab is already open, so popup blockers never trigger.
  let pickedFile: File | null = $state(null)
  let pickedBufferPromise: Promise<ArrayBuffer> | null = null

  function handleFilePicked(file: File) {
    pickedFile = file
    // Start reading immediately so the buffer is likely ready by the time the
    // user confirms — but the user can confirm even before it resolves.
    pickedBufferPromise = file.arrayBuffer()
  }

  function resetUpload() {
    pickedFile = null
    pickedBufferPromise = null
  }

  function confirmOpenViewer() {
    const bufferPromise = pickedBufferPromise
    resetUpload()
    if (!bufferPromise) {
      return
    }

    let handoff: ReturnType<typeof openUploadBundleTab>
    try {
      handoff = openUploadBundleTab()
    } catch (e) {
      toast.toastError('Opening support bundle viewer')(
        e instanceof Error ? e : new Error(String(e)),
        8000
      )
      return
    }

    ;(async () => {
      try {
        const buffer = await bufferPromise
        await handoff.send(buffer)
      } catch (e) {
        handoff.cancel()
        toast.toastError('Opening support bundle viewer')(
          e instanceof Error ? e : new Error(String(e)),
          8000
        )
      }
    })()
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
<Popup>
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
  {#snippet content(close)}
    <div
      transition:slide={{ duration: 100 }}
      class="bg-white-dark absolute top-10 right-0 z-30 flex min-w-[220px] flex-col overflow-hidden rounded shadow-md"
    >
      <!-- Grid with both pages in (1,1) so they overlap during the transition
           and slide past each other like carousel slides. Each page has a fixed
           direction (page 1 left, page 2 right) so forward/back animate as
           mirrored sweeps without tracking direction state. -->
      <div class="grid">
        {#if pickedFile}
          <!-- Confirmation view: opens the viewer tab on a direct click so the
               browser preserves user activation through window.open. -->
          <div
            in:fly={{ x: 220, duration: 200 }}
            out:fly={{ x: 220, duration: 200 }}
            class="col-start-1 row-start-1 flex flex-col"
          >
            <div class="flex items-center gap-2 px-2 py-2">
              <button class="btn-icon h-7 w-7" onclick={resetUpload} aria-label="Back" title="Back">
                <span class="fd fd-chevron-left text-[20px]"></span>
              </button>
              <span class="min-w-0 flex-1 truncate text-sm" title={pickedFile.name}>
                {pickedFile.name}
              </span>
            </div>
            <div class="px-2 pb-2">
              <button
                class="btn h-8! w-full preset-filled-primary-500"
                onclick={() => {
                  confirmOpenViewer()
                  close()
                }}
                data-testid="btn-confirm-view-profile"
              >
                <span class="fd fd-file-search text-[18px]"></span>
                <span>View profile</span>
              </button>
            </div>
          </div>
        {:else}
          <div
            in:fly={{ x: -220, duration: 200 }}
            out:fly={{ x: -220, duration: 200 }}
            class="col-start-1 row-start-1 flex flex-col"
          >
            <SupportBundleMenu
              bind:collectNewData={collectNewData.value}
              onDownload={() => {
                downloadData = { ...defaultData, collect: collectNewData.value }
                globalDialog.dialog = supportBundleDialog
                close()
              }}
              onFilePicked={handleFilePicked}
            />
          </div>
        {/if}
      </div>
    </div>
  {/snippet}
</Popup>
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
