<script lang="ts">
  import { onMount, onDestroy } from 'svelte'
  import {
    Profiler,
    CircuitProfile,
    type ProfilerConfig,
    type JsonProfiles,
    type Dataflow
  } from 'profiler-lib'

  interface Props {
    /** Profile data from the pipeline manager */
    profileData: JsonProfiles
    /** Dataflow graph data from the SQL compiler */
    dataflowData: Dataflow
    /** Optional class for styling the container */
    class?: string
  }

  let { profileData, dataflowData, class: className }: Props = $props()

  // DOM element references
  let graphContainer: HTMLDivElement | undefined = $state()
  let selectorContainer: HTMLDivElement | undefined = $state()
  let navigatorContainer: HTMLDivElement | undefined = $state()
  let tooltipContainer: HTMLDivElement | undefined = $state()
  let errorContainer: HTMLDivElement | undefined = $state()

  // Profiler instance
  let profiler: Profiler | null = $state(null)
  let error: string | null = $state(null)

  // Initialize profiler when all containers are mounted and data is available
  $effect(() => {
    console.log('first effect a')
    // Wait for all DOM elements to be available
    if (
      !graphContainer ||
      !selectorContainer ||
      !navigatorContainer ||
      !tooltipContainer ||
      !errorContainer
    ) {
      return
    }
    $effect.root(() => {
      console.log('first effect b')

      // Wait for data to be available
      if (!profileData || !dataflowData) {
        return
      }
      console.log('first effect c')

      // Clean up previous profiler instance if exists
      if (profiler) {
        profiler.dispose()
        profiler = null
      }
      console.log('first effect d')

      // Clear any previous errors
      error = null

      try {
        // Parse the profile data
        const profile = CircuitProfile.fromJson(profileData)
        profile.setDataflow(dataflowData)

        // Create profiler configuration
        const config: ProfilerConfig = {
          graphContainer: graphContainer!,
          selectorContainer: selectorContainer!,
          navigatorContainer: navigatorContainer!,
          tooltipContainer: tooltipContainer!,
          errorContainer
        }

        // Create and render profiler
        profiler = new Profiler(config)
        profiler.render(profile)
        console.log('first effect e')
      } catch (e) {
        error = e instanceof Error ? e.message : String(e)
        console.error('Failed to initialize profiler:', e)
      }
    })
  })
  console.log('a')

  // Cleanup on component destruction
  onDestroy(() => {
    console.log('onDestroy')
    if (profiler) {
      console.log('onDestroy profiler')
      profiler.dispose()
      profiler = null
    }
  })
</script>

<div class="profiler-wrapper {className || ''}" data-testid="profiler-diagram">
  {#if error}
    <div class="error-banner" role="alert">
      <strong>Error loading profiler:</strong>
      {error}
    </div>
  {/if}

  <!-- Main graph visualization (full size) -->
  <div bind:this={graphContainer} class="profiler-graph"></div>

  <!-- Overlay menus (positioned on top of graph) -->
  <div class="profiler-menus">
    <!-- Selector controls (metric selection, worker filters) -->
    <div bind:this={selectorContainer} class="profiler-selector">
      <table class="selection-tools">
        <!-- Profiler will inject UI here -->
      </table>
    </div>

    <!-- Navigator minimap -->
    <div bind:this={navigatorContainer} class="profiler-navigator"></div>

    <!-- Error container (managed by profiler-lib) -->
    <div bind:this={errorContainer} class="profiler-error" style="display: none;"></div>
  </div>

  <!-- Tooltip container (positioned in top-right) -->
  <div bind:this={tooltipContainer} class="profiler-tooltip-container"></div>
</div>

<style>
  .profiler-wrapper {
    width: 100%;
    height: 100%;
    position: relative;
  }

  .error-banner {
    background-color: #fee;
    color: #c00;
    padding: 1rem;
    border: 1px solid #fcc;
    border-radius: 4px;
    margin-bottom: 1rem;
    font-family: monospace;
    white-space: pre-wrap;
  }

  .profiler-graph {
    width: 100%;
    height: 100%;
  }

  .profiler-menus {
    position: absolute;
    top: 0;
    left: 0;
    z-index: 1;
    display: flex;
    flex-direction: column;
    align-items: start;
  }

  .profiler-selector {
    background-color: rgba(100, 100, 100, 0.3);
    z-index: 1;
  }

  .profiler-navigator {
    width: 100px;
    height: 100px;
  }

  .profiler-error {
    background-color: white;
    color: red;
    display: none;
    font-family: monospace;
    border: 1px solid red;
    text-align: center;
    width: 350px;
    white-space: pre-wrap;
  }

  .profiler-tooltip-container {
    position: absolute;
    top: 0;
    right: 0;
    z-index: 2;
    pointer-events: none;
  }

  /* Selector UI styling (applied to injected content) */
  .profiler-selector :global(table) {
    width: 100%;
    border-collapse: collapse;
  }

  .profiler-selector :global(td) {
    padding: 0.5rem;
    vertical-align: top;
  }

  .profiler-selector :global(select) {
    width: 100%;
    padding: 0.25rem;
    border: 1px solid #ccc;
    border-radius: 4px;
  }

  .profiler-selector :global(button) {
    padding: 0.25rem 0.5rem;
    border: 1px solid #ccc;
    border-radius: 4px;
    background-color: white;
    cursor: pointer;
  }

  .profiler-selector :global(button:hover) {
    background-color: #f0f0f0;
  }

  .profiler-selector :global(input[type='checkbox']) {
    cursor: pointer;
  }
</style>
