<script lang="ts">
  import {
    CircuitProfile,
    type Dataflow,
    type JsonProfiles,
    type ProfilerCallbacks,
    Visualizer,
    type VisualizerConfig
  } from 'profiler-lib'
  import { onDestroy } from 'svelte'

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

  const { profileData, dataflowData, programCode, callbacks, class: className }: Props = $props()

  // DOM element references
  let graphContainer: HTMLDivElement | undefined = $state()
  let navigatorContainer: HTMLDivElement | undefined = $state()

  // Visualizer instance and profile data (combined as their lifecycle is connected)
  let instance = $state<{ visualizer: Visualizer; profile: CircuitProfile } | null>(null)

  // Public getter for profile (used by parent component)
  export function getProfile(): CircuitProfile | null {
    return instance?.profile ?? null
  }

  // Initialize visualizer when all containers are mounted and data is available
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
      // Clean up previous instance if exists
      if (instance) {
        instance.visualizer.dispose()
        instance = null
      }

      try {
        // Parse the profile data
        const circuit = CircuitProfile.fromJson(profileData)
        circuit.profile.setDataflow(dataflowData, programCode)

        // Create visualizer configuration with callbacks
        const config: VisualizerConfig = {
          graphContainer: graphContainer!,
          navigatorContainer: navigatorContainer!,
          callbacks
        }

        // Create and render visualizer
        const visualizer = new Visualizer(config)
        visualizer.render(circuit)

        instance = { visualizer, profile: circuit.profile }
      } catch (e) {
        const errorMsg = e instanceof Error ? e.message : String(e)
        callbacks.onError(`Failed to initialize visualizer: ${errorMsg}`)
        console.error('Failed to initialize visualizer:', e)
      }
    })
  })

  // Public methods that proxy to visualizer-lib
  export function selectMetric(metricId: string) {
    instance?.visualizer.selectMetric(metricId)
  }

  export function toggleWorker(workerId: string) {
    instance?.visualizer.toggleWorker(workerId)
  }

  export function toggleAllWorkers() {
    instance?.visualizer.toggleAllWorkers()
  }

  export function search(query: string) {
    instance?.visualizer.search(query)
  }

  export function showGlobalMetrics(isSticky?: boolean) {
    instance?.visualizer.showGlobalMetrics(isSticky)
  }

  export function hideNodeAttributes(hideSticky?: boolean) {
    instance?.visualizer.hideNodeAttributes(hideSticky)
  }

  export function showTopNodes(metric: string, n: number, isSticky?: boolean) {
    return instance?.visualizer.showTopNodes(metric, n, isSticky)
  }

  // Cleanup on component destruction
  onDestroy(() => {
    if (instance) {
      instance.visualizer.dispose()
      instance = null
    }
  })
</script>

<div class="visualizer-wrapper {className || ''}" data-testid="visualizer-diagram">
  <!-- Main graph visualization (full size) -->
  <div bind:this={graphContainer} class="visualizer-graph"></div>

  <!-- Overlay menus (positioned on top of graph) -->
  <div class="visualizer-menus">
    <!-- Navigator minimap -->
    <div bind:this={navigatorContainer} class="visualizer-navigator"></div>
  </div>
</div>

<style>
  .visualizer-wrapper {
    width: 100%;
    height: 100%;
    position: absolute;
    overflow: hidden;
  }

  .visualizer-graph {
    width: 100%;
    height: 100%;
  }

  .visualizer-menus {
    position: absolute;
    top: 0.5rem;
    left: 0.5rem;
    z-index: 1;
    display: flex;
    flex-direction: column;
    align-items: start;
    gap: 0.5rem;
  }

  .visualizer-navigator {
    width: 108px;
    height: 108px;
    background-color: rgba(255, 255, 255, 0.95);
    border: 2px solid rgb(var(--color-surface-100));
    border-radius: 8px;
    box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
    padding: 2px;
  }
</style>
