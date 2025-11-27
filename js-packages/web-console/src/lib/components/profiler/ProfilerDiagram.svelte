<script lang="ts">
  import { onDestroy } from 'svelte'
  import {
    Profiler,
    CircuitProfile,
    type ProfilerConfig,
    type ProfilerCallbacks,
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
    /** Callbacks from ProfilerLayout */
    callbacks: ProfilerCallbacks
    /** Optional class for styling the container */
    class?: string
  }

  let { profileData, dataflowData, programCode, callbacks, class: className }: Props = $props()

  // DOM element references
  let graphContainer: HTMLDivElement | undefined = $state()
  let navigatorContainer: HTMLDivElement | undefined = $state()

  // Profiler instance
  let profiler: Profiler | null = $state(null)

  // Initialize profiler when all containers are mounted and data is available
  $effect(() => {
    // Wait for all DOM elements to be available
    if (!graphContainer || !navigatorContainer) {
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

      try {
        // Parse the profile data
        const profile = CircuitProfile.fromJson(profileData)
        profile.setDataflow(dataflowData, programCode)

        // Create profiler configuration with callbacks
        const config: ProfilerConfig = {
          graphContainer: graphContainer!,
          navigatorContainer: navigatorContainer!,
          callbacks
        }

        // Create and render profiler
        profiler = new Profiler(config)
        profiler.render(profile)
      } catch (e) {
        const errorMsg = e instanceof Error ? e.message : String(e)
        callbacks.onError(`Failed to initialize profiler: ${errorMsg}`)
        console.error('Failed to initialize profiler:', e)
      }
    })
  })

  // Public methods that proxy to profiler-lib
  export function selectMetric(metricId: string): void {
    profiler?.selectMetric(metricId)
  }

  export function toggleWorker(workerId: string): void {
    profiler?.toggleWorker(workerId)
  }

  export function toggleAllWorkers(): void {
    profiler?.toggleAllWorkers()
  }

  export function search(query: string): void {
    profiler?.search(query)
  }

  // Cleanup on component destruction
  onDestroy(() => {
    if (profiler) {
      profiler.dispose()
      profiler = null
    }
  })
</script>

<div class="profiler-wrapper {className || ''}" data-testid="profiler-diagram">
  <!-- Main graph visualization (full size) -->
  <div bind:this={graphContainer} class="profiler-graph"></div>

  <!-- Overlay menus (positioned on top of graph) -->
  <div class="profiler-menus">
    <!-- Navigator minimap -->
    <div bind:this={navigatorContainer} class="profiler-navigator"></div>
  </div>
</div>

<style>
  .profiler-wrapper {
    width: 100%;
    height: 100%;
    position: absolute;
    overflow: hidden;
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
</style>
