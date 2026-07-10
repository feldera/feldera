<script lang="ts">
  import ConfigView from '../ConfigView.svelte'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { runtimeConfig }: AnalysisTabProps = $props()
</script>

<div class="relative min-h-0 flex-1">
  <ConfigView {runtimeConfig} />
</div>
