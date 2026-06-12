<script lang="ts">
  import TriageResultsView from '../TriageResultsView.svelte'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { triageResults, lookup, issueSeverityFilter, issueCategoryFilter }: AnalysisTabProps =
    $props()
</script>

<TriageResultsView
  results={triageResults}
  {lookup}
  severityFilter={issueSeverityFilter}
  categoryFilter={issueCategoryFilter}
/>
