<script lang="ts">
  import BundleLogsView from '../BundleLogsView.svelte'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { logText, lookup, onSearchShortcut }: AnalysisTabProps = $props()
</script>

<BundleLogsView {logText} {lookup} {onSearchShortcut} />
