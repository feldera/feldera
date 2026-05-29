<script lang="ts">
  import BundleLogsView from '../BundleLogsView.svelte'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { logText, lookup }: AnalysisTabProps = $props()
</script>

<BundleLogsView {logText} {lookup} />
