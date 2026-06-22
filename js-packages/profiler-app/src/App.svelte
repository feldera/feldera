<script lang="ts">
  import {
    createLoadGuard,
    getSuitableProfiles,
    type GlobalMetrics,
    processProfileFiles,
    SupportBundleViewerLayout,
    type ZipItem
  } from 'profiler-layout'
  import type { JsonProfiles, Dataflow } from 'profiler-lib'
  import { TriageResults } from 'triage-types'
  import faviconDataUrl from '$assets/favicon.svg?dataurl=enc'
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogomarkColorDark from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'

  let isLoading = $state(false)
  let errorMessage = $state('')
  let profileFiles: [Date, ZipItem[]][] = $state([])
  let selectedTimestamp: Date | null = $state(null)
  let sqlPanelFullHeight = $state(false)

  let profileData: {
    profile: JsonProfiles
    dataflow: Dataflow | undefined
    sources: string[] | undefined
    logText: string | undefined
    globalMetrics: GlobalMetrics | undefined
  } | null = $state(null)

  let fileInput: HTMLInputElement | null = $state(null)

  const withLoadGuard = createLoadGuard({
    setLoading: (loading) => {
      isLoading = loading
    }
  })

  async function loadProfile(timestamp: Date) {
    const selected = profileFiles.find(([d]) => d.getTime() === timestamp.getTime())
    if (!selected) {
      return
    }
    const data = await processProfileFiles(selected[1])
    selectedTimestamp = timestamp
    profileData = {
      profile: data.profile,
      dataflow: data.dataflow,
      sources: data.sources,
      logText: data.logText,
      globalMetrics: data.globalMetrics
    }
  }

  const handleFileUpload = async () => {
    const file = fileInput?.files?.[0]
    if (!file) {
      return
    }
    errorMessage = ''
    await withLoadGuard(
      async () => {
        const zipData = new Uint8Array(await file.arrayBuffer())
        const suitableProfiles = getSuitableProfiles(zipData)
        if (suitableProfiles.length === 0) {
          throw new Error(
            'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile and dataflow graph (optional).'
          )
        }
        profileFiles = suitableProfiles
        await loadProfile(suitableProfiles.at(-1)![0])
      },
      (e) => {
        errorMessage =
          e instanceof Error && e.message
            ? e.message
            : 'An error occurred while processing the bundle'
      }
    )
    if (fileInput) {
      fileInput.value = ''
    }
  }

  async function handleSelectTimestamp(timestamp: Date) {
    errorMessage = ''
    await withLoadGuard(
      () => loadProfile(timestamp),
      (e) => {
        errorMessage =
          e instanceof Error && e.message
            ? e.message
            : 'An error occurred while processing the profile'
      }
    )
  }
</script>

<svelte:head>
  <link rel="icon" type="image/svg+xml" href={faviconDataUrl} />
</svelte:head>

<input
  type="file"
  accept=".zip"
  bind:this={fileInput}
  onchange={handleFileUpload}
  class="hidden"
/>

<div class="flex h-full w-full flex-col font-dm-sans relative" data-theme="feldera-modern-theme">
  {#if profileData}
    <SupportBundleViewerLayout
      profileData={profileData.profile}
      dataflowData={profileData.dataflow}
      programCode={profileData.sources}
      logText={profileData.logText}
      globalMetrics={profileData.globalMetrics}
      triageResults={new TriageResults()}
      {profileFiles}
      {selectedTimestamp}
      onSelectTimestamp={handleSelectTimestamp}
      bind:sqlPanelFullHeight
    >
      {#snippet loadProfileControl()}
        <FelderaModernLogoColorDark class="h-8 px-2" />
        <button
          class="rounded-base bg-primary-500 px-4 py-1.5 text-sm font-medium text-white transition-colors hover:bg-primary-600"
          onclick={() => fileInput?.click()}
        >
          Load Bundle
        </button>
      {/snippet}
    </SupportBundleViewerLayout>
  {:else}
    <!-- Welcome screen -->
    <div class="flex h-full w-full flex-col items-center justify-center gap-6 p-8">
      <FelderaModernLogomarkColorDark class="h-10" />
      <div class="text-center">
        <h1 class="mb-2 text-4xl font-bold text-surface-900">Feldera Profiler</h1>
        <p class="text-lg text-surface-600">
          Visualize DBSP circuit profiles from support bundles
        </p>
      </div>

      <div class="bg-surface-50 max-w-md rounded-container p-6 shadow-lg">
        <p class="mb-6 text-sm text-surface-700">
          Load a Feldera support bundle (.zip file) containing circuit profile data to begin
          visualization.
        </p>

        {#if isLoading}
          <div class="flex items-center justify-center gap-3 py-4">
            <div
              class="h-6 w-6 animate-spin rounded-full border-2 border-primary-500 border-t-transparent"
            ></div>
            <span class="text-sm text-surface-600">Processing bundle…</span>
          </div>
        {:else}
          <button
            class="w-full rounded-base bg-primary-500 px-6 py-2 font-medium text-white transition-colors hover:bg-primary-600"
            onclick={() => fileInput?.click()}
          >
            Load Bundle
          </button>
        {/if}

        {#if errorMessage}
          <div
            class="mt-4 rounded-base border border-error-200 bg-error-50/50 p-3 text-sm text-error-800"
          >
            <strong>Error:</strong>
            {errorMessage}
          </div>
        {/if}
      </div>
    </div>
  {/if}
</div>
