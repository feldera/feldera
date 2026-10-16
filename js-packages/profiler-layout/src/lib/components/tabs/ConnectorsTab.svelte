<script lang="ts">
  import { ConnectorErrors, ConnectorMetricsTables, Drawer } from 'common-ui'
  import type { ConnectorErrorFilter } from 'common-ui/connectorMetrics'
  import type { AnalysisTabProps } from './MetricsTab.svelte'

  let { connectorMetrics, connectorStatus }: AnalysisTabProps = $props()

  let selected = $state<{
    relationName: string
    connectorName: string
    direction: 'input' | 'output'
    filter: ConnectorErrorFilter
  } | null>(null)

  // A new bundle or timestamp closes the drawer of the previous connector.
  $effect(() => {
    connectorStatus
    selected = null
  })

  const selectedStatus = $derived.by(() => {
    if (!selected || !connectorStatus) {
      return null
    }
    const { connectorName, direction } = selected
    const endpoints = direction === 'input' ? connectorStatus.inputs : connectorStatus.outputs
    return endpoints.find((endpoint) => endpoint.endpoint_name === connectorName) ?? null
  })
</script>

{#if connectorMetrics && (connectorMetrics.tables.size || connectorMetrics.views.size)}
  <div class="flex min-h-0 flex-1 flex-nowrap overflow-hidden">
    <Drawer
      open={selected !== null}
      side="right"
      inlineClass="rounded pt-4 pl-4"
      onClose={() => (selected = null)}
      localStorageKey="profiler/drawer/connectorErrors"
    >
      {#snippet main()}
        <div class="scrollbar flex min-h-0 flex-1 flex-col gap-4 overflow-y-auto">
          <ConnectorMetricsTables
            metrics={{ current: connectorMetrics }}
            onConnectorSelect={(relationName, connectorName, direction, filter) =>
              (selected = { relationName, connectorName, direction, filter })}
          />
        </div>
      {/snippet}
      {#if selected}
        <ConnectorErrors
          relationName={selected.relationName}
          connectorName={selected.connectorName}
          direction={selected.direction}
          filter={selected.filter}
          status={selectedStatus}
          onClose={() => (selected = null)}
        />
      {/if}
    </Drawer>
  </div>
{:else}
  <div class="flex h-full items-center justify-center text-sm text-surface-600-400">
    No connector statistics available in this bundle
  </div>
{/if}
