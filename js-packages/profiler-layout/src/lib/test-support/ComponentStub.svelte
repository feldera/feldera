<script lang="ts" module>
  import type { CircuitProfile, ProfilerCallbacks } from 'profiler-lib'

  /** What the layout handed the diagram, and what the diagram hands back. A test writes
   *  `profile`, then drives the layout through `callbacks` as the Visualizer would. */
  export const capture: {
    callbacks?: ProfilerCallbacks
    profile: CircuitProfile | null
  } = { profile: null }
</script>

<script lang="ts">
  // Stand-in for the heavy leaf components (ProfilerDiagram, tab panels) in layout tests.
  // Rendering the real children would pull in cytoscape and Monaco. It exposes the
  // ProfilerDiagram imperative API the layout invokes on its `bind:this` instance, so those
  // calls stay harmless no-ops.
  const props: { callbacks?: ProfilerCallbacks } = $props()
  // Only the diagram is handed callbacks, so recording them cannot pick up a tab panel's props.
  // The layout builds the object once, at init, so reading it once is the whole of it.
  $effect(() => {
    if (props.callbacks) {
      capture.callbacks = props.callbacks
    }
  })

  export function getProfile() {
    return capture.profile
  }
  export function selectMetric(_metricId: string) {}
  export function search(_query: string) {}
  export function showGlobalMetrics(_isSticky?: boolean) {}
  export function showTopNodes(_isSticky?: boolean) {}
</script>
