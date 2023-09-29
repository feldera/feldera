'use client'

import { BreadcrumbsHeader } from '$lib/components/common/BreadcrumbsHeader'
import { GridItems } from '$lib/components/common/GridItems'
import {
  ConfigEditorDialog,
  KafkaInputConnectorDialog,
  KafkaOutputConnectorDialog,
  UrlConnectorDialog
} from '$lib/components/connectors/dialogs'
import { AddConnectorCard } from '$lib/components/connectors/dialogs/AddConnectorCard'
import { DebeziumInputConnectorDialog } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { useHashPart } from '$lib/compositions/useHashPart'
import { connectorTypeToLogo } from '$lib/functions/connectors'
import { showOnHashPart } from '$lib/functions/urlHash'
import { ConnectorType } from '$lib/types/connectors'

import { Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const ConnectorCreateGrid = () => {
  const showOnHash = showOnHashPart(useHashPart())
  // id is referenced by webui-tester
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/connectors/list`}>Connectors</Link>
        <Link href={`/connectors/create`}>Create</Link>
      </BreadcrumbsHeader>
      <Grid id='connector-creator-content' container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
        <GridItems xs={12} sm={6} md={4}>
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.URL)}
            title='Load Data from an HTTP URL'
            addInput={{ href: '#input/url' }}
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.KAFKA_IN)}
            title='Connect to a Kafka topic'
            addInput={{ href: '#input/kafka' }}
            addOutput={{ href: '#output/kafka' }}
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.DEBEZIUM_IN)}
            title='Connect to a Debezium topic'
            addInput={{ href: '#input/debezium' }}
          />
          <AddConnectorCard
            id='generic-connector'
            icon={connectorTypeToLogo(ConnectorType.UNKNOWN)}
            title='Configure a generic connector'
            addInput={{ href: '#generic' }}
            addOutput={{ href: '#generic' }}
          />
        </GridItems>
      </Grid>
      <UrlConnectorDialog {...showOnHash('input/url')}></UrlConnectorDialog>
      <KafkaInputConnectorDialog {...showOnHash('input/kafka')}></KafkaInputConnectorDialog>
      <KafkaOutputConnectorDialog {...showOnHash('output/kafka')}></KafkaOutputConnectorDialog>
      <DebeziumInputConnectorDialog {...showOnHash('input/debezium')}></DebeziumInputConnectorDialog>
      <ConfigEditorDialog {...showOnHash('generic')}></ConfigEditorDialog>
    </>
  )
}

export default ConnectorCreateGrid
