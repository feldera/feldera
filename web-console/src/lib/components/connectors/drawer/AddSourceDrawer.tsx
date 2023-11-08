// The drawer component that opens when the user wants to add a connector in the
// pipeline builder.

import { DebeziumInputConnectorDialog } from '$lib/components/connectors/dialogs/DebeziumInputConnector'
import { KafkaInputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaInputConnector'
import { KafkaOutputConnectorDialog } from '$lib/components/connectors/dialogs/KafkaOutputConnector'
import { SnowflakeOutputConnectorDialog } from '$lib/components/connectors/dialogs/SnowflakeOutputConnector'
import { UrlConnectorDialog } from '$lib/components/connectors/dialogs/UrlConnector'
import { useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useHashPart } from '$lib/compositions/useHashPart'
import { randomString } from '$lib/functions/common/string'
import {
  connectorDescrToType,
  connectorTypeToDirection,
  connectorTypeToLogo,
  connectorTypeToTitle
} from '$lib/functions/connectors'
import { showOnHashPart } from '$lib/functions/urlHash'
import { AttachedConnector, ConnectorDescr } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { ConnectorType, Direction } from '$lib/types/connectors'
import Image from 'next/image'
import { useEffect, useState } from 'react'
import IconX from '~icons/bx/x'

import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Breadcrumbs, Button, Card, CardContent, Chip, Grid, Link } from '@mui/material'
import Box, { BoxProps } from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

import SelectSourceTable from './SelectSourceTable'

const Header = styled(Box)<BoxProps>(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  padding: theme.spacing(3, 4),
  justifyContent: 'space-between',
  backgroundColor: theme.palette.background.paper
}))

const IoSelectBox = (props: { icon: string; howMany: number; onNew: string; onSelect: () => void }) => {
  const countColor = props.howMany === 0 ? 'secondary' : 'success'

  return (
    <>
      <Grid item xs={6}>
        <Card
          sx={{
            color: 'secondary.main',
            display: 'flex',
            textAlign: 'center',
            alignItems: 'center',
            flexDirection: 'column'
          }}
        >
          <CardContent sx={{ width: '100%', display: 'flex', flexDirection: 'column', gap: 2, alignItems: 'center' }}>
            <Image
              src={props.icon}
              alt='An icon'
              style={{ height: 64, objectFit: 'cover', width: 'fit-content', padding: 6 }}
            />
            <Button fullWidth variant='outlined' color='secondary' href={`#${props.onNew}`}>
              New
            </Button>
            <Button
              fullWidth
              variant='outlined'
              color='secondary'
              disabled={props.howMany === 0}
              onClick={props.onSelect}
            >
              Select&nbsp;
              <Chip sx={{ p: 0, m: 0 }} label={props.howMany} size='small' color={countColor} variant='outlined' />
            </Button>
          </CardContent>
        </Card>
      </Grid>
    </>
  )
}

const shouldDisplayConnector = (direction: Direction, connectorType: ConnectorType) =>
  (d => d === Direction.INPUT_OUTPUT || d === direction)(connectorTypeToDirection(connectorType))

const SideBarAddIo = () => {
  const [hash, setHash] = useHashPart()
  const showOnHash = showOnHashPart([hash, setHash])

  const drawer = (path =>
    path
      ? (([nodeType, connectorType]) => ({
          nodeType: nodeType as 'add_input' | 'add_output',
          direction: nodeType === 'add_input' ? Direction.INPUT : Direction.OUTPUT,
          connectorType: connectorType as ConnectorType | undefined
        }))(path)
      : undefined)(/(add_input|add_output)(\/?\w+)?/.exec(hash)?.[0].split('/'))

  const [sourceCounts, setSourceCounts] = useState<{ [key in ConnectorType]?: number }>({})
  const { data, isPending, isError } = useQuery(PipelineManagerQuery.connector())
  useEffect(() => {
    if (isPending || isError) {
      return
    }
    const counts = data
      .map(s => connectorDescrToType(s))
      .reduce((acc: typeof sourceCounts, typ: ConnectorType) => {
        acc[typ] = (acc[typ] ?? 0) + 1
        return acc
      }, {})

    setSourceCounts(counts)
  }, [data, isPending, isError])

  const openSelectTable = (ioTable: ConnectorType | undefined) => {
    setHash(`${drawer!.nodeType}/${ioTable ?? ''}`)
  }

  const addConnector = useAddConnector()
  const onAddClick = (direction: Direction) => (connector: ConnectorDescr) => {
    setHash('')
    const ac: AttachedConnector = {
      name: randomString(),
      connector_id: connector.connector_id,
      relation_name: '',
      is_input: direction === Direction.INPUT
    }
    addConnector(connector, ac)
  }

  return (
    <Drawer
      id='connector-drawer' // referenced by webui-tester
      open={!!drawer}
      anchor='right'
      variant='temporary'
      onClose={() => setHash('')}
      ModalProps={{ keepMounted: true }}
      sx={{ '& .MuiDrawer-paper': { width: { xs: 300, sm: 600 }, backgroundColor: 'background.default' } }}
    >
      <Header>
        <Typography variant='h6'>
          {!!drawer && (
            <Breadcrumbs aria-label='breadcrumb' separator={<NavigateNextIcon fontSize='small' />}>
              <Link underline='hover' color='inherit' href={`#${drawer.nodeType}`}>
                {drawer.nodeType === 'add_input' ? 'Add Input Source' : 'Add Output Destination'}
              </Link>
              <Typography color='text.primary'>
                {drawer.connectorType && connectorTypeToTitle(drawer.connectorType)}
              </Typography>
            </Breadcrumbs>
          )}
        </Typography>

        <IconButton size='small' href='#' sx={{ color: 'text.primary' }}>
          <IconX fontSize={20} />
        </IconButton>
      </Header>
      {drawer && !drawer.connectorType && (
        <Grid container spacing={6} sx={{ p: 6 }}>
          {[
            ConnectorType.KAFKA_IN,
            ConnectorType.KAFKA_OUT,
            ConnectorType.DEBEZIUM_IN,
            ConnectorType.SNOWFLAKE_OUT,
            ConnectorType.URL
          ].map(
            type =>
              shouldDisplayConnector(drawer.direction, type) && (
                <IoSelectBox
                  key={type}
                  icon={connectorTypeToLogo(type)}
                  howMany={sourceCounts[type] ?? 0}
                  onNew={`new/connector/${drawer.direction}/${type}`}
                  onSelect={() => openSelectTable(type)}
                />
              )
          )}
        </Grid>
      )}
      {!!drawer?.connectorType &&
        (direction => (
          <Grid container spacing={3}>
            <Grid item xs={12}>
              <SelectSourceTable direction={direction} typ={drawer.connectorType} onAddClick={onAddClick(direction)} />
            </Grid>
          </Grid>
        ))(connectorTypeToDirection(drawer.connectorType!))}
      {(() => {
        const dialogs = {
          [ConnectorType.KAFKA_IN]: KafkaInputConnectorDialog,
          [ConnectorType.KAFKA_OUT]: KafkaOutputConnectorDialog,
          [ConnectorType.DEBEZIUM_IN]: DebeziumInputConnectorDialog,
          [ConnectorType.SNOWFLAKE_OUT]: SnowflakeOutputConnectorDialog,
          [ConnectorType.URL]: UrlConnectorDialog
        }
        const res = /new\/connector\/(\w+)\/(\w+)/.exec(hash)
        if (!res) {
          return <></>
        }
        const [, direction, type] = res
        const Dialog = dialogs[type as keyof typeof dialogs]
        return <Dialog {...showOnHash('new/connector/')} onSuccess={onAddClick(direction as Direction)} />
      })()}
    </Drawer>
  )
}

export default SideBarAddIo
