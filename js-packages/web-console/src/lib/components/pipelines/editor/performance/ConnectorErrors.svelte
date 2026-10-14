<script lang="ts">
  import { ConnectorErrors } from 'common-ui'
  import type { ConnectorErrorFilter } from 'common-ui/connectorMetrics'
  import { getCaseDependentName } from 'common-ui/felderaRelation'
  import type { InputEndpointStatus, OutputEndpointStatus } from '$lib/services/manager'
  import { getInputConnectorStatus, getOutputConnectorStatus } from '$lib/services/pipelineManager'

  let {
    pipelineName,
    relationName,
    connectorName,
    direction,
    filter = 'all',
    onClose
  }: {
    pipelineName: string
    relationName: string
    connectorName: string
    direction: 'input' | 'output'
    filter?: ConnectorErrorFilter
    onClose: () => void
  } = $props()

  let status: InputEndpointStatus | OutputEndpointStatus | null = $state(null)
  let loading = $state(false)

  $effect(() => {
    pipelineName
    relationName
    connectorName
    direction
    loading = true
    status = null

    const strippedConnectorName = connectorName.slice(
      getCaseDependentName(relationName).name.length + 1
    )
    const request =
      direction === 'input'
        ? getInputConnectorStatus(pipelineName, relationName, strippedConnectorName)
        : getOutputConnectorStatus(pipelineName, relationName, strippedConnectorName)

    request.then((s) => {
      status = s
      loading = false
    })
  })
</script>

<ConnectorErrors {relationName} {connectorName} {direction} {filter} {status} {loading} {onClose} />
