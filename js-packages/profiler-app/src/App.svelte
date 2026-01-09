<script lang="ts">
  import {
    getSuitableProfiles,
    type ProcessedProfile,
    ProfilerLayout,
    ProfileTimestampSelector,
    processProfileFiles,
    type ZipItem
  } from 'profiler-layout'
  import faviconDataUrl from '$assets/favicon.svg?dataurl=enc'
  import FelderaModernLogoColorDark from '$assets/images/feldera-modern/Feldera Logo Color Dark.svg?component'
  import FelderaModernLogomarkColorDark from '$assets/images/feldera-modern/Feldera Logomark Color Dark.svg?component'

  let profileData = $state<ProcessedProfile | null>(null)
  let errorMessage = $state('')
  let fileInput: HTMLInputElement | null = $state(null)
  let isLoading = $state(false)
  let profileFiles: [Date, ZipItem[]][] = $state([])
  let selectedTimestamp: Date | null = $state(null)

  const handleFileUpload = async () => {
    if (!fileInput) {
      return
    }

    const file = fileInput.files?.[0]
    if (!file) {
      return
    }

    errorMessage = ''
    isLoading = true

    try {
      const arrayBuffer = await file.arrayBuffer()
      const zipData = new Uint8Array(arrayBuffer)

      // Unzip once and extract all suitable profiles
      const suitableProfiles = getSuitableProfiles(zipData)

      if (suitableProfiles.length === 0) {
        errorMessage =
          'No suitable profiles found in the uploaded support bundle. Check if it contains the circuit profile and dataflow graph (optional).'
      } else {
        // Store the profile files for efficient timestamp switching
        profileFiles = suitableProfiles
        selectedTimestamp = suitableProfiles.at(-1)![0] // Select most recent by default
      }
    } catch (error) {
      errorMessage =
        error instanceof Error ? error.message : 'An error occurred while processing the bundle'
    } finally {
      isLoading = false
      // Reset file input
      if (fileInput) {
        fileInput.value = ''
      }
    }
  }

  // Effect to load profile data when timestamp is selected
  $effect(() => {
    if (!selectedTimestamp) {
      return
    }

    const selectedProfile = profileFiles.find(
      (profile) => profile[0].getTime() === selectedTimestamp!.getTime()
    )

    if (!selectedProfile) {
      return
    }

    ;(async () => {
      isLoading = true
      try {
        const data = await processProfileFiles(selectedProfile[1])
        profileData = data
      } catch (error) {
        errorMessage =
          error instanceof Error ? error.message : 'An error occurred while processing the profile'
      } finally {
        isLoading = false
      }
    })()
  })

  const triggerFileUpload = () => {
    fileInput?.click()
  }

  // const handleReset = () => {
  //   profileData = null
  //   errorMessage = ''
  // }
</script>

<svelte:head>
  <link rel="icon" type="image/svg+xml" href={faviconDataUrl} />
</svelte:head>

<!-- Hidden file input -->
<input
  type="file"
  accept=".zip"
  bind:this={fileInput}
  onchange={handleFileUpload}
  class="hidden"
/>

<div class="flex h-full w-full flex-col font-dm-sans" data-theme="feldera-modern-theme">
  {#if profileData}
    <!-- Profile visualization -->
    <ProfilerLayout
      profileData={profileData.profile}
      dataflowData={profileData.dataflow}
      programCode={profileData.sources}
      toolbarClass="p-2 bg-surface-50-950"
    >
      {#snippet toolbarStart()}
        <FelderaModernLogoColorDark class='h-8 px-2'></FelderaModernLogoColorDark>
        <button
          class="rounded-base bg-primary-500 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-primary-600"
          onclick={triggerFileUpload}
        >
          📁 Load Bundle
        </button>

        <!-- Profile Timestamp Selector -->
        <ProfileTimestampSelector {profileFiles} bind:selectedTimestamp />
      {/snippet}
    </ProfilerLayout>
  {:else}
    <!-- Welcome screen -->
    <div class="flex h-full w-full flex-col items-center justify-center gap-6 p-8">
      <FelderaModernLogomarkColorDark class="h-10"></FelderaModernLogomarkColorDark>
      <!-- Title -->
      <div class="text-center">
        <h1 class="mb-2 text-4xl font-bold text-surface-900">
          Feldera Profiler
        </h1>
        <p class="text-surface-600 text-lg">
          Visualize DBSP circuit profiles from support bundles
        </p>
      </div>

      <!-- Instructions card -->
      <div class="bg-surface-50 rounded-container max-w-md p-6 shadow-lg">
        <p class="text-surface-700 mb-6 text-sm">
          Load a Feldera support bundle (.zip file) containing circuit profile data to begin visualization.
        </p>

        {#if isLoading}
          <div class="flex items-center justify-center gap-3 py-4">
            <div class="border-primary-500 h-6 w-6 animate-spin rounded-full border-2 border-t-transparent"></div>
            <span class="text-surface-600 text-sm">Processing bundle...</span>
          </div>
        {:else}
          <button
            class="bg-primary-500 hover:bg-primary-600 w-full rounded-base px-6 py-2 font-medium text-white transition-colors"
            onclick={triggerFileUpload}
          >
            📁 Load Bundle
          </button>
        {/if}

        {#if errorMessage}
          <div class="bg-error-50 text-error-800 mt-4 rounded-base border-error-200 border p-3 text-sm">
            <strong>Error:</strong> {errorMessage}
          </div>
        {/if}
      </div>
    </div>
  {/if}
</div>
