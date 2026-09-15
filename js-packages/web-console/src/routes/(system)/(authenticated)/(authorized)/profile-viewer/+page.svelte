<script lang="ts">
  import triagePlugins, { createBundle, TriageResults } from 'virtual:feldera-triage-plugins'
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import {
    createLoadGuard,
    type GlobalMetrics,
    getSuitableProfiles,
    processProfileFiles,
    SupportBundleViewerLayout,
    type ZipItem
  } from 'profiler-layout'
  import type { Dataflow, JsonProfiles } from 'profiler-lib'
  import { fade } from 'svelte/transition'
  import { replaceState } from '$app/navigation'
  import Popup from '$lib/components/common/Popup.svelte'
  import AppHeader from '$lib/components/layout/AppHeader.svelte'
  import PipelineBreadcrumbs from '$lib/components/layout/PipelineBreadcrumbs.svelte'
  import SupportBundleConfirm from '$lib/components/pipelines/editor/SupportBundleConfirm.svelte'
  import SupportBundleMenu from '$lib/components/pipelines/editor/SupportBundleMenu.svelte'
  import { useLayoutSettings } from '$lib/compositions/layout/useLayoutSettings.svelte'
  import { receiveUploadedBundle } from '$lib/compositions/profileBundleHandoff'
  import { type PickedBundle, useBundlePicker } from '$lib/compositions/useBundlePicker.svelte'
  import { useDownloadProgress } from '$lib/compositions/useDownloadProgress.svelte'
  import { usePipelineManager } from '$lib/compositions/usePipelineManager.svelte'
  import { useToast } from '$lib/compositions/useToastNotification'
  import { enclosure, nonNull } from '$lib/functions/common/function'
  import { resolve } from '$lib/functions/svelte'
  import {
    readStoredBundle,
    requestBundleReadPermission,
    resolveStoredBundle,
    type StoredSupportBundle
  } from '$lib/services/supportBundleHistory'

  const { data } = $props()
  const { pipelineName, source, collect, channel, bundle: storedBundleId } = data

  const api = usePipelineManager()
  const toast = useToast()
  const layoutSettings = useLayoutSettings()
  const picker = useBundlePicker()

  let downloadProgress = useDownloadProgress()
  // If the URL provides no pipelineName, the remote-download path has no
  // target. Surface the empty state immediately instead of issuing a request
  // that would 404 — the user can still upload a bundle from disk.
  let isLoading = $state(source === 'remote' ? !!pipelineName : true)
  let errorMessage = $state('')
  // Pipeline name from the loaded bundle's pipeline_config.json. Used as a
  // fallback in the breadcrumb when the URL didn't supply one.
  let bundlePipelineName: string | undefined = $state(undefined)
  const displayPipelineName = $derived(pipelineName || bundlePipelineName || '')

  let getProfileFiles: () => [Date, ZipItem[]][] = $state(() => [])
  let selectedProfile: Date | null = $state(null)
  let getProfileData:
    | (() => {
        profile: JsonProfiles | undefined
        dataflow: Dataflow | undefined
        sources: string[] | undefined
        logText: string | undefined
        globalMetrics: GlobalMetrics | undefined
        runtimeConfig: unknown
      })
    | null = $state(null)
  let triageResults: TriageResults = $state(new TriageResults())
  // `true` while ELK is running the layout pass on the current profile. The flag flips back
  // to `false` once `layoutstop` fires inside profiler-lib.
  let isRendering = $state(false)

  let collectNewData = $state(collect)
  let fileInput: HTMLInputElement | null = $state(null)
  // Set when the URL names a bundle in the history. Where to read the file is known,
  // but reading it may need the user to grant access, because browsers drop file
  // grants between sessions.
  let pendingBundle: StoredSupportBundle | null = $state(null)

  const withLoadGuard = createLoadGuard({
    setLoading: (loading) => {
      isLoading = loading
    },
    onFinally: () => downloadProgress.reset()
  })

  const onLoadError = (fallback: string) => (e: unknown) => {
    errorMessage = e instanceof Error && e.message ? e.message : fallback
  }

  async function loadProfile(timestamp: Date) {
    const files = getProfileFiles().find(([d]) => d.getTime() === timestamp.getTime())
    if (!files) {
      return
    }
    const processed = await processProfileFiles(files[1])

    let tr: TriageResults
    if (triagePlugins.length > 0) {
      try {
        const bundle = await createBundle(files[1])
        tr = new TriageResults()
        triagePlugins.forEach((p) => p.triage(bundle, tr))
      } catch (e) {
        tr = new TriageResults()
        toast.toastError('Running triage plugins')(
          e instanceof Error ? e : new Error(String(e)),
          10000
        )
      }
    } else {
      tr = new TriageResults()
    }
    triageResults = tr
    selectedProfile = timestamp
    if (processed.pipelineName) {
      bundlePipelineName = processed.pipelineName
    }
    getProfileData = enclosure({
      profile: processed.profile,
      dataflow: processed.dataflow,
      sources: processed.sources,
      logText: processed.logText,
      globalMetrics: processed.globalMetrics,
      runtimeConfig: processed.runtimeConfig
    })
  }

  const processZipBundle = async (zipData: Uint8Array, emptyMessage: string) => {
    const suitableProfiles = getSuitableProfiles(zipData)
    if (suitableProfiles.length === 0) {
      throw new Error(emptyMessage)
    }
    getProfileFiles = () => suitableProfiles
    await loadProfile(suitableProfiles.at(-1)![0])
  }

  errorMessage = ''
  if (source === 'upload' || pipelineName) {
    withLoadGuard(async () => {
      if (source === 'upload') {
        if (storedBundleId) {
          // The viewer reads the bundle from the history itself, which is what makes
          // this tab reloadable.
          const { bundle, needsPermission } = await resolveStoredBundle(storedBundleId)
          pendingBundle = bundle
          if (needsPermission) {
            // Granting read access needs a click, so the empty state offers one.
            return
          }
          await loadStoredBundle(bundle)
          return
        }
        // No history entry, so the tab that picked the bundle hands the bytes over.
        // The last resort, for an archive too big to keep a copy of.
        const buffer = await receiveUploadedBundle(channel)
        await processZipBundle(
          new Uint8Array(buffer),
          'No readable data found in the uploaded bundle.'
        )
      } else {
        downloadProgress.onProgress(0, 1)
        const { dataPromise } = api.getPipelineSupportBundle(
          pipelineName,
          {
            collect,
            circuit_profile: true,
            pipeline_config: true,
            dataflow_graph: true,
            logs: true
          },
          downloadProgress.onProgress
        )
        const supportBundle = await dataPromise
        await processZipBundle(
          new Uint8Array(await supportBundle.data.arrayBuffer()),
          'No readable data found. Try enabling "Collect new data".'
        )
      }
    }, onLoadError('Failed to load the profile bundle.'))
  }

  async function handleLoadRemote() {
    getProfileData = null
    errorMessage = ''
    await withLoadGuard(async () => {
      downloadProgress.onProgress(0, 1)
      const { dataPromise } = api.getPipelineSupportBundle(
        pipelineName,
        {
          collect: collectNewData,
          circuit_profile: true,
          pipeline_config: true,
          dataflow_graph: true,
          logs: true
        },
        downloadProgress.onProgress
      )
      const bundle = await dataPromise
      await processZipBundle(
        new Uint8Array(await bundle.data.arrayBuffer()),
        'No readable data found. Try enabling "Collect new data".'
      )
    }, onLoadError('Failed to download the profile bundle.'))
  }

  /**
   * Loads a bundle the user picked in this tab. A bundle in the history is named in
   * the URL, so reloading this tab reopens that bundle.
   */
  async function handlePickedBundle(bundle: PickedBundle) {
    getProfileData = null
    errorMessage = ''
    pendingBundle = null
    if (bundle.bundleId !== undefined) {
      replaceState(`${resolve('/profile-viewer')}?source=upload&bundle=${bundle.bundleId}`, {})
    }
    await withLoadGuard(async () => {
      downloadProgress.onProgress(0, 1)
      await processZipBundle(
        await bundle.read(),
        'No suitable profiles found in the selected bundle.'
      )
    }, onLoadError('Failed to load the selected bundle.'))
  }

  /** Picks a bundle from disk, through the file picker or the input fallback. */
  async function pickBundle() {
    if (!picker.isSupported) {
      fileInput?.click()
      return
    }
    try {
      const bundle = await picker.pick()
      if (bundle) {
        await handlePickedBundle(bundle)
      }
    } catch (e) {
      toast.toastError('Opening support bundle')(
        e instanceof Error ? e : new Error(String(e)),
        8000
      )
    }
  }

  async function loadStoredBundle(stored: StoredSupportBundle) {
    getProfileData = null
    downloadProgress.onProgress(0, 1)
    await processZipBundle(
      await readStoredBundle(stored),
      'No suitable profiles found in the stored bundle.'
    )
  }

  /** Asks for read access from within the click, then loads the bundle. */
  async function grantAndLoadStoredBundle(stored: StoredSupportBundle) {
    errorMessage = ''
    await withLoadGuard(async () => {
      if (!(await requestBundleReadPermission(stored))) {
        throw new Error(
          `Reading ${stored.name} needs access to the file. Click "Open from disk" and ` +
            'allow access when the browser asks, or open the bundle from disk again.'
        )
      }
      await loadStoredBundle(stored)
    }, onLoadError('Failed to open the support bundle.'))
  }

  async function handleSelectTimestamp(timestamp: Date) {
    errorMessage = ''
    await withLoadGuard(
      () => loadProfile(timestamp),
      onLoadError('Failed to load the selected profile snapshot.')
    )
  }
</script>

<svelte:head>
  <title>{displayPipelineName ? `${displayPipelineName} — ` : ''}Profile Viewer</title>
</svelte:head>

<input
  type="file"
  accept=".zip"
  bind:this={fileInput}
  onchange={async (e) => {
    const file = (e.currentTarget as HTMLInputElement).files?.[0]
    if (file) {
      ;(e.currentTarget as HTMLInputElement).value = ''
      handlePickedBundle(await picker.fromFile(file))
    }
  }}
  class="hidden"
  data-testid="input-open-support-bundle"
/>

<div
  class="font-dm-sans flex h-screen flex-col overflow-hidden"
  data-testid="box-profile-viewer-page"
>
  <AppHeader>
    {#snippet afterStart()}
      <PipelineBreadcrumbs
        breadcrumbs={[
          { text: 'Home', href: resolve('/') },
          ...(displayPipelineName
            ? [
                {
                  text: displayPipelineName,
                  href: pipelineName
                    ? resolve(`/pipelines/${encodeURIComponent(pipelineName)}/`)
                    : undefined
                }
              ]
            : []),
          { text: 'Profile Viewer', href: resolve('/profile-viewer') }
        ]}
      />
    {/snippet}
  </AppHeader>

  <!-- Thin progress bar shown above the layout while a profile is loading or being drawn.
       `percent === null` keeps it indeterminate (the layout engine has no progress signal,
       so the "rendering" pass cannot report a fraction). `visible === false` collapses the
       row to zero height instead of unmounting, so successive uses (download → render) flow
       without layout jumps. -->
  {#snippet progressBar(visible: boolean, percent: number | null)}
    <div class=" px-4 {visible ? '' : 'h-0 opacity-0'} transition-opacity">
      <Progress class="-mt-1 h-1" value={percent} max={100}>
        <Progress.Track>
          <Progress.Range class="bg-primary-500" />
        </Progress.Track>
      </Progress>
    </div>
  {/snippet}
  <!-- Download takes precedence: while the bundle is still arriving there is nothing to
       render yet, and once it has, `downloadProgress.percent` is reset to null and we display
       the indeterminate rendering bar while the diagram layout is computed. -->
  {#if nonNull(downloadProgress.percent)}
    {@render progressBar(true, downloadProgress.percent ?? null)}
  {:else}
    {@render progressBar(isRendering, null)}
  {/if}

  {#if isLoading && !getProfileData}
    <div class="flex flex-1 flex-col items-center justify-center gap-4">
      {#if errorMessage}
        <div class="max-w-lg rounded preset-outlined-error-600-400 p-4 text-center">
          {errorMessage}
        </div>
        {#if pipelineName}
          <button class="btn preset-filled-primary-500" onclick={handleLoadRemote}> Retry </button>
        {/if}
      {:else}
        <div class="flex items-center gap-3">
          <div
            class="h-6 w-6 animate-spin rounded-full border-2 border-primary-500 border-t-transparent"
          ></div>
          <span class="text-surface-600-400">Loading profile bundle…</span>
        </div>
      {/if}
    </div>
  {:else if getProfileData}
    {@const { profile, dataflow, sources, logText, globalMetrics, runtimeConfig } =
      getProfileData()}
    <div class="min-h-0 flex-1 px-4 pb-4">
      <SupportBundleViewerLayout
        profileData={profile}
        dataflowData={dataflow}
        programCode={sources}
        {logText}
        {globalMetrics}
        {runtimeConfig}
        {triageResults}
        profileFiles={getProfileFiles()}
        selectedTimestamp={selectedProfile}
        onSelectTimestamp={handleSelectTimestamp}
        bind:sqlPanelFullHeight={layoutSettings.sqlPanelFullHeight.value}
        onRenderingChange={(rendering) => (isRendering = rendering)}
      >
        {#snippet loadProfileControl()}
          <Popup>
            {#snippet trigger(toggle)}
              <button class="btn h-6 !bg-surface-100-900 px-3 text-sm" onclick={toggle}>
                Load profile
              </button>
            {/snippet}
            {#snippet content(close)}
              <div
                transition:fade={{ duration: 100 }}
                class="absolute top-10 left-0 z-30 w-max min-w-[200px]"
              >
                <div class="bg-white-dark flex flex-col overflow-hidden rounded shadow-md">
                  <SupportBundleMenu
                    bind:collectNewData
                    onDownload={() => {
                      handleLoadRemote()
                      close()
                    }}
                    onPickBundle={() => {
                      pickBundle()
                      close()
                    }}
                    disabled={isLoading}
                    downloadLabel="Download profile"
                  />
                </div>
              </div>
            {/snippet}
          </Popup>
        {/snippet}
      </SupportBundleViewerLayout>
    </div>
  {:else}
    <!-- Empty / error state when there's no data yet and not loading -->
    <div class="flex flex-1 flex-col items-center justify-center gap-4">
      {#if errorMessage}
        <div class="max-w-lg rounded preset-outlined-error-600-400 p-4 text-center">
          {errorMessage}
        </div>
      {/if}
      {#if pendingBundle}
        {@const stored = pendingBundle}
        <!-- Shown when the browser holds no grant for the file, which is the case for
             this URL opened directly, such as after a reload. A bundle opened from the
             home page was granted there, so the profile is already loading and there is
             nothing to confirm. -->
        <SupportBundleConfirm
          name={stored.name}
          confirmLabel="Open from disk"
          onConfirm={() => grantAndLoadStoredBundle(stored)}
          variant="page"
          data-testid="btn-open-stored-bundle"
        />
      {/if}
      {#if pipelineName}
        <button class="btn preset-filled-primary-500" onclick={handleLoadRemote}>
          Download profile
        </button>
      {/if}
      <!-- Also the only control on a cold start, where nothing has been opened yet, so
           the label cannot say "another". -->
      <button class="link p-2 hover:underline" onclick={pickBundle}> Open a support bundle </button>
    </div>
  {/if}
</div>
