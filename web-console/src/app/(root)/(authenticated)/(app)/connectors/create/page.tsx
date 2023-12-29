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
import { SnowflakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { useHashPart } from '$lib/compositions/useHashPart'
import { connectorTypeToLogo } from '$lib/functions/connectors'
import { showOnHashPart } from '$lib/functions/urlHash'
import { ConnectorType } from '$lib/types/connectors'
import { match } from 'ts-pattern'
import IconCheck from '~icons/bx/check'

import { Button, Link } from '@mui/material'
import Grid from '@mui/material/Grid'

const ConnectorCreateGrid = () => {
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])

  // id is referenced by webui-tester
  return (
    <>
      <BreadcrumbsHeader>
        <Link href={`/connectors/list`} data-testid='button-breadcrumb-connectors'>
          Connectors
        </Link>
        <Link href={`/connectors/create`} data-testid='button-breadcrumb-create-connectors'>
          Create
        </Link>
      </BreadcrumbsHeader>
      <Grid id='connector-creator-content' container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
        <GridItems xs={12} sm={6} md={4}>
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.URL)}
            title='Load Data from an HTTP URL'
            addInput={{ href: '#input/url' }}
            data-testid='box-connector-url'
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.KAFKA_IN)}
            title='Connect to a Kafka topic'
            addInput={{ href: '#input/kafka' }}
            addOutput={{ href: '#output/kafka' }}
            data-testid='box-connector-kafka'
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.DEBEZIUM_IN)}
            title='Connect to a Debezium topic'
            addInput={{ href: '#input/debezium' }}
            data-testid='box-connector-debezium'
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.SNOWFLAKE_OUT)}
            title='Connect to a Snowflake table'
            addOutput={{ href: '#output/snowflake' }}
            data-testid='box-connector-snowflake'
          />
          <AddConnectorCard
            id='generic-connector'
            icon={connectorTypeToLogo(ConnectorType.UNKNOWN)}
            title='Configure a generic connector'
            addInput={{ href: '#generic' }}
            addOutput={{ href: '#generic' }}
            data-testid='box-connector-generic'
          />
        </GridItems>
      </Grid>
      {(Dialog =>
        Dialog ? (
          <Dialog
            {...showOnHash(hash)}
            existingTitle={null}
            submitButton={
              <Button variant='contained' color='success' endIcon={<IconCheck />} type='submit'>
                Create
              </Button>
            }
          />
        ) : (
          <></>
        ))(
        match(hash)
          .with('input/url', () => UrlConnectorDialog)
          .with('input/kafka', () => KafkaInputConnectorDialog)
          .with('output/kafka', () => KafkaOutputConnectorDialog)
          .with('input/debezium', () => DebeziumInputConnectorDialog)
          .with('output/snowflake', () => SnowflakeOutputConnectorDialog)
          .with('generic', () => ConfigEditorDialog)
          .otherwise(() => null)
      )}
    </>
  )
}

export default ConnectorCreateGrid
