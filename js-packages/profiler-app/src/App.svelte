<script lang="ts">
  import { Progress } from '@skeletonlabs/skeleton-svelte'
  import type { ConnectorMetrics, ConnectorStatus } from 'common-ui/connectorMetrics'
  import type { PipelineMonitorEvent } from 'common-ui/pipelineHealth'
  import {
    createLoadGuard,
    type GlobalMetrics,
    getSuitableProfiles,
    processProfileFiles,
    type SourceFile,
    SupportBundleViewerLayout,
    type ZipItem
  } from 'profiler-layout'
  import type { Dataflow, JsonProfiles } from 'profiler-lib'
  import { MediaQuery } from 'svelte/reactivity'
  import { TriageResults } from 'triage-types'
  import faviconDataUrl from '$assets/favicon.svg?dataurl=enc'
  import FelderaModernLogoBlack from '$assets/images/feldera-modern/Feldera Logo Black Solid.svg?component'
  import FelderaModernLogomarkBlack from '$assets/images/feldera-modern/Feldera Logomark Black.svg?component'

  const prefersDark = new MediaQuery('(prefers-color-scheme: dark)')
  // Null until the user picks a theme. Until then the theme follows the system setting.
  let chosenTheme: 'light' | 'dark' | null = $state(null)
  const theme = $derived(chosenTheme ?? (prefersDark.current ? 'dark' : 'light'))

  // As in web-console, the `dark` class on <html> switches the theme of all components.
  $effect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark')
  })

  let isLoading = $state(false)
  // True while the diagram computes its layout.
  let isRendering = $state(false)
  let errorMessage = $state('')
  let profileFiles: [Date, ZipItem[]][] = $state([])
  let selectedTimestamp: Date | null = $state(null)
  let sqlPanelFullHeight = $state(false)

  let profileData: {
    profile: JsonProfiles | undefined
    dataflow: Dataflow | undefined
    logText: string | undefined
    globalMetrics: GlobalMetrics | undefined
    connectorMetrics: ConnectorMetrics | undefined
    connectorStatus: ConnectorStatus | undefined
    runtimeConfig: unknown
    pipelineEvents: PipelineMonitorEvent[] | undefined
    sourceFiles: SourceFile[] | undefined
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
      logText: data.logText,
      globalMetrics: data.globalMetrics,
      connectorMetrics: data.connectorMetrics,
      connectorStatus: data.connectorStatus,
      runtimeConfig: data.runtimeConfig,
      pipelineEvents: data.pipelineEvents,
      sourceFiles: data.sourceFiles
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
            'No readable data found in the uploaded support bundle. Check that it contains a circuit profile, config, logs or stats.'
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

{#snippet themeToggle()}
  <button
    class="btn-icon text-[20px] hover:bg-row-hover fd {theme === 'dark' ? 'fd-sun' : 'fd-moon'}"
    title={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
    aria-label={theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode'}
    onclick={() => (chosenTheme = theme === 'dark' ? 'light' : 'dark')}
  ></button>
{/snippet}

<div class="flex h-full w-full flex-col font-dm-sans relative" data-theme="feldera-modern-theme">
  <!-- Thin bar at the top of the page while a bundle loads or the diagram computes its layout,
       as in the web-console profile viewer. -->
  <div
    class="pointer-events-none absolute inset-x-0 top-0 z-50 px-4 transition-opacity {isLoading ||
    isRendering
      ? ''
      : 'opacity-0'}"
  >
    <Progress class="h-1" value={null} max={100}>
      <Progress.Track>
        <Progress.Range class="bg-secondary-500" />
      </Progress.Track>
    </Progress>
  </div>
  {#if profileData}
    <SupportBundleViewerLayout
      profileData={profileData.profile}
      dataflowData={profileData.dataflow}
      logText={profileData.logText}
      globalMetrics={profileData.globalMetrics}
      connectorMetrics={profileData.connectorMetrics}
      connectorStatus={profileData.connectorStatus}
      runtimeConfig={profileData.runtimeConfig}
      pipelineEvents={profileData.pipelineEvents}
      sourceFiles={profileData.sourceFiles}
      triageResults={new TriageResults()}
      {theme}
      {profileFiles}
      {selectedTimestamp}
      onSelectTimestamp={handleSelectTimestamp}
      bind:sqlPanelFullHeight
      onRenderingChange={(rendering) => (isRendering = rendering)}
      headerEnd={themeToggle}
    >
      {#snippet loadProfileControl()}
        <FelderaModernLogoBlack class="h-8 fill-logo px-2 dark:fill-white" />
        <button
          class="rounded-base bg-primary-500 px-4 py-1.5 text-sm font-medium text-primary-contrast-500 transition-colors hover:bg-primary-500/85"
          onclick={() => fileInput?.click()}
        >
          Load Bundle
        </button>
      {/snippet}
    </SupportBundleViewerLayout>
  {:else}
    <!-- Welcome screen -->
    <div class="absolute top-4 right-4">
      {@render themeToggle()}
    </div>
    <div class="flex h-full w-full flex-col items-center justify-center gap-6 p-8">
      <FelderaModernLogomarkBlack class="h-10 fill-logo dark:fill-white" />
      <div class="text-center">
        <h1 class="mb-2 text-4xl font-bold text-surface-950-50">Feldera Profiler</h1>
        <p class="text-lg text-surface-600-400">
          Visualize DBSP circuit profiles from support bundles
        </p>
      </div>

      <div class="max-w-md rounded-container bg-surface-50-950 p-6 shadow-lg">
        <p class="mb-6 text-sm text-surface-700-300">
          Load a Feldera support bundle (.zip file) to explore the circuit profile,
          program code, logs and the pipeline config.
        </p>

        {#if isLoading}
          <div class="flex items-center justify-center gap-3 py-4">
            <div
              class="h-6 w-6 animate-spin rounded-full border-2 border-secondary-500 border-t-transparent"
            ></div>
            <span class="text-sm text-surface-600-400">Processing bundle…</span>
          </div>
        {:else}
          <button
            class="w-full rounded-base bg-primary-500 px-6 py-2 font-medium text-primary-contrast-500 transition-colors hover:bg-primary-500/85"
            onclick={() => fileInput?.click()}
          >
            Load Bundle
          </button>
        {/if}

        {#if errorMessage}
          <div
            class="mt-4 rounded-base border border-error-200-800 bg-error-50-950 p-3 text-sm text-error-800-200"
          >
            <strong>Error:</strong>
            {errorMessage}
          </div>
        {/if}
      </div>
    </div>
  {/if}
</div>
