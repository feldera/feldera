'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import { GridItems } from '$lib/components/common/GridItems'
import { AddConnectorCard } from '$lib/components/connectors/dialogs/AddConnectorCard'
import { DebeziumInputConnectorDialog } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { DeltaLakeInputConnectorDialog } from '$lib/components/connectors/dialogs/DeltaLakeInputConnector'
import { DeltaLakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/DeltaLakeOutputConnector'
import { ConfigEditorDialog } from '$lib/components/connectors/dialogs/GenericEditorConnector'
import { KafkaInputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaInputConnector'
import { KafkaOutputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaOutputConnector'
import { SnowflakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { UrlConnectorDialog } from '$lib/components/connectors/dialogs/UrlConnector'
import { useHashPart } from '$lib/compositions/useHashPart'
import { connectorTypeToLogo } from '$lib/functions/connectors'
import { showOnHashPart } from '$lib/functions/urlHash'
import { ConnectorType } from '$lib/types/connectors'
import { match } from 'ts-pattern'

import { Button } from '@mui/material'
import Grid from '@mui/material/Grid'

const ConnectorCreateGrid = () => {
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])

  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/connectors/list`} data-testid='button-breadcrumb-connectors'>
          Connectors
        </Breadcrumbs.Link>
        <Breadcrumbs.Link href={`/connectors/create`} data-testid='button-breadcrumb-create-connectors'>
          Create
        </Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <Grid container spacing={6} className='match-height' sx={{ pl: 6, pt: 6 }}>
        <GridItems xs={12} sm={6} md={4}>
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.URL_IN)}
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
            icon={connectorTypeToLogo(ConnectorType.DELTALAKE_IN)}
            title='Connect with Delta Lake'
            addInput={{ href: '#input/deltalake' }}
            addOutput={{ href: '#output/deltalake' }}
            data-testid='box-connector-deltalake'
          />
          <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.SNOWFLAKE_OUT)}
            title='Connect to a Snowflake table'
            addOutput={{ href: '#output/snowflake' }}
            data-testid='box-connector-snowflake'
          />
          {/* <AddConnectorCard
            icon={connectorTypeToLogo(ConnectorType.S3_IN)}
            title='Connect to a S3 compatible bucket'
            addInput={{ href: '#input/s3' }}
            data-testid='box-connector-s3'
          /> */}
          <AddConnectorCard
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
              <Button
                variant='contained'
                color='success'
                endIcon={<i className={`bx bx-check`} style={{}} />}
                type='submit'
                data-testid='button-create'
              >
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
          .with('input/deltalake', () => DeltaLakeInputConnectorDialog)
          .with('output/deltalake', () => DeltaLakeOutputConnectorDialog)
          .with('output/snowflake', () => SnowflakeOutputConnectorDialog)
          .with('generic', () => ConfigEditorDialog)
          .otherwise(() => null)
      )}
    </>
  )
}

export default ConnectorCreateGrid
