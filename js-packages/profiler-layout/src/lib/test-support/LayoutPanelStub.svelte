<script lang="ts" module>
  import type { CircuitProfile, ProfilerCallbacks, SourcePositionRange } from 'profiler-lib'
  import type { SourceFile } from '../functions/processZipBundle'

  /** What the layout handed the diagram, and what the diagram hands back. A test writes
   *  `profile`, then drives the layout through `callbacks` as the Visualizer would. */
  export const capture: {
    callbacks?: ProfilerCallbacks
    /** The SQL lines the layout handed the diagram. */
    programCode?: string[]
    profile: CircuitProfile | null
    /** The latest props of the code panel's tab, the only panel handed a `file`. */
    sourceTab?: { file: SourceFile; highlightRanges: SourcePositionRange[] }
  } = { profile: null }
</script>

<script lang="ts">
  // Stand-in for the panels of SupportBundleViewerLayout (ProfilerDiagram, tab panels) in its
  // tests. Rendering the real panels would pull in cytoscape and Monaco. It exposes the
  // ProfilerDiagram imperative API the layout invokes on its `bind:this` instance, so those
  // calls stay harmless no-ops.
  const props: {
    callbacks?: ProfilerCallbacks
    programCode?: string[]
    file?: SourceFile
    highlightRanges?: SourcePositionRange[]
  } = $props()
  // Only the diagram gets `callbacks`, so this effect records only the props of the diagram.
  $effect(() => {
    if (props.callbacks) {
      capture.callbacks = props.callbacks
      capture.programCode = props.programCode
    }
  })

  $effect(() => {
    if (props.file) {
      capture.sourceTab = { file: props.file, highlightRanges: props.highlightRanges ?? [] }
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
