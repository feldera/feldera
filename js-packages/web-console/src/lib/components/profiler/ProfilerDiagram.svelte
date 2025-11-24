<script lang="ts">
  import { onDestroy } from 'svelte'
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
    /** Lines of user SQL code */
    programCode: string[]
    /** Optional class for styling the container */
    class?: string
    /** Metric selector element (dependency injection) */
    metricSelector: HTMLSelectElement
    /** Worker checkboxes container element (dependency injection) */
    workerCheckboxesContainer: HTMLElement
    /** Toggle workers button element (dependency injection) */
    toggleWorkersButton: HTMLButtonElement
    /** Optional search input element */
    searchInput?: HTMLInputElement
  }

  let {
    profileData,
    dataflowData,
    programCode,
    class: className,
    metricSelector,
    workerCheckboxesContainer,
    toggleWorkersButton,
    searchInput
  }: Props = $props()

  // DOM element references
  let graphContainer: HTMLDivElement | undefined = $state()
  let navigatorContainer: HTMLDivElement | undefined = $state()
  let tooltipContainer: HTMLDivElement | undefined = $state()
  let errorContainer: HTMLDivElement | undefined = $state()
  let messageContainer: HTMLDivElement | undefined = $state()

  // Profiler instance
  let profiler: Profiler | null = $state(null)
  let error: string | null = $state(null)

  // Initialize profiler when all containers are mounted and data is available
  $effect(() => {
    // Wait for all DOM elements to be available
    if (
      !graphContainer ||
      !navigatorContainer ||
      !tooltipContainer ||
      !errorContainer ||
      !metricSelector ||
      !workerCheckboxesContainer ||
      !toggleWorkersButton
    ) {
      return
    }

    // Wait for data to be available
    if (!profileData || !dataflowData) {
      return
    }
    $effect.root(() => {
      // Clean up previous profiler instance if exists
      if (profiler) {
        profiler.dispose()
        profiler = null
      }

      // Clear any previous errors
      error = null

      try {
        // Parse the profile data
        const profile = CircuitProfile.fromJson(profileData)
        profile.setDataflow(dataflowData, programCode)

        // Create profiler configuration with dependency-injected UI elements
        const config: ProfilerConfig = {
          graphContainer: graphContainer!,
          navigatorContainer: navigatorContainer!,
          metricSelector: metricSelector,
          workerCheckboxesContainer: workerCheckboxesContainer,
          toggleWorkersButton: toggleWorkersButton,
          tooltipContainer: tooltipContainer!,
          errorContainer,
          messageContainer,
          searchInput
        }

        // Create and render profiler
        profiler = new Profiler(config)
        profiler.render(profile)
      } catch (e) {
        error = e instanceof Error ? e.message : String(e)
        console.error('Failed to initialize profiler:', e)
      }
    })
  })

  // Cleanup on component destruction
  onDestroy(() => {
    if (profiler) {
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
    <!-- Navigator minimap -->
    <div bind:this={navigatorContainer} class="profiler-navigator"></div>

    <!-- Message container (managed by profiler-lib) -->
    <div bind:this={messageContainer} class="profiler-message" style="display: none;"></div>

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
    position: absolute;
    overflow: hidden;
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
    top: 0.5rem;
    left: 0.5rem;
    z-index: 1;
    display: flex;
    flex-direction: column;
    align-items: start;
    gap: 0.5rem;
  }

  .profiler-navigator {
    width: 108px;
    height: 108px;
    background-color: rgba(255, 255, 255, 0.95);
    border: 2px solid rgb(var(--color-surface-100));
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    padding: 2px;
  }

  .profiler-message {
    display: none;
    font-family: monospace;
    background-color: rgba(255, 255, 255, 0.9);
    padding: 0.5rem;
    font-size: 14px;
    border-radius: 4px;
    box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  }

  .profiler-error {
    background-color: white;
    color: red;
    display: none;
    font-family: monospace;
    border: 1px solid red;
    border-radius: 4px;
    text-align: center;
    max-width: 350px;
    white-space: pre-wrap;
    padding: 0.5rem;
    box-shadow: 0 2px 4px rgba(255, 0, 0, 0.2);
  }

  .profiler-tooltip-container {
    position: absolute;
    top: 0.5rem;
    right: 0.5rem;
    z-index: 2;
    pointer-events: none;
    max-width: 600px;
    max-height: calc(100vh - 1rem);
  }

  /* Tooltip styling */
  .profiler-tooltip-container :global(> div) {
    background-color: rgba(0, 0, 0, 0.9);
    border-radius: 8px;
    box-shadow: 0 4px 16px rgba(0, 0, 0, 0.4);
  }

  .profiler-tooltip-container :global(table) {
    background-color: transparent;
    border-collapse: collapse;
    font-size: 12px;
    width: 100%;
  }

  .profiler-tooltip-container :global(table td),
  .profiler-tooltip-container :global(table th) {
    padding: 0px 4px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    color: white;
  }

  .profiler-tooltip-container :global(table th) {
    background-color: rgba(102, 126, 234, 0.4);
    font-weight: 600;
    text-align: center;
  }

  .profiler-tooltip-container :global(table td) {
    background-color: rgba(0, 0, 0, 0.3);
  }

  /* Scrollbar styling */
  .profiler-tooltip-container::-webkit-scrollbar {
    width: 8px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-track {
    background: rgba(0, 0, 0, 0.1);
    border-radius: 4px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-thumb {
    background: rgba(102, 126, 234, 0.5);
    border-radius: 4px;
  }

  .profiler-tooltip-container::-webkit-scrollbar-thumb:hover {
    background: rgba(102, 126, 234, 0.7);
  }
</style>
