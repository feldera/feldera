<script lang="ts">
  // Stand-in for the heavy leaf components (ProfilerDiagram, tab panels) in layout tests.
  // It swallows every prop and exposes the ProfilerDiagram imperative API the layout invokes
  // on its `bind:this` instance, so those calls stay harmless no-ops. Rendering the real
  // children would pull in cytoscape and Monaco, which the pane-ordering test does not need.
  // Accept and ignore whatever props the parent passes. Never read, so Svelte emits no
  // reactive-capture warning; underscore marks it intentionally unused for the linter.
  const _props = $props()

  export function getProfile() {
    return null
  }
  export function selectMetric(_metricId: string) {}
  export function search(_query: string) {}
  export function showGlobalMetrics(_isSticky?: boolean) {}
  export function showTopNodes(_isSticky?: boolean) {}
</script>
