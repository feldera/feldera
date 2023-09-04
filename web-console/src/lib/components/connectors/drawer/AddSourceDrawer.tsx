// The drawer component that opens when the user wants to add a connector in the
// pipeline builder.

import { useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useHashPart } from '$lib/compositions/useHashPart'
import { inUnion } from '$lib/functions/common/array'
import { randomString } from '$lib/functions/common/string'
import {
  connectorDescrToType,
  connectorTypeToDirection,
  connectorTypeToIcon,
  connectorTypeToTitle
} from '$lib/functions/connectors'
import { AttachedConnector, ConnectorDescr } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { ConnectorType, Direction } from '$lib/types/connectors'
import { useEffect, useState } from 'react'
import { tuple } from 'src/lib/functions/common/tuple'

import { Icon } from '@iconify/react'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Breadcrumbs, Button, Card, CardContent, CardHeader, Chip, Grid, Link } from '@mui/material'
import Box, { BoxProps } from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import { styled } from '@mui/material/styles'
import Typography from '@mui/material/Typography'
import { useQuery } from '@tanstack/react-query'

import { KafkaInputConnectorDialog } from '../dialogs/KafkaInputConnector'
import { KafkaOutputConnectorDialog } from '../dialogs/KafkaOutputConnector'
import { UrlConnectorDialog } from '../dialogs/UrlConnector'
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
            m: 5,
            display: 'flex',
            textAlign: 'center',
            alignItems: 'center',
            flexDirection: 'column'
          }}
        >
          <CardHeader avatar={<Icon icon={props.icon} fontSize='3rem' />} />
          <CardContent>
            <Button fullWidth sx={{ mb: 1 }} variant='outlined' color='secondary' href={`#${props.onNew}`}>
              New
            </Button>
            <Button
              fullWidth
              sx={{ mb: 1 }}
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

const shouldDisplayConnector = (direction: Direction, connectorType: ConnectorType) => {
  return (
    connectorTypeToDirection(connectorType) == Direction.INPUT_OUTPUT ||
    direction === connectorTypeToDirection(connectorType)
  )
}

const SideBarAddIo = () => {
  const [hash, setHash] = useHashPart()

  const [nodeType, connectorType] = (([nodeType, connectorType]) =>
    tuple(
      inUnion(['add_input', 'add_output'] as const, nodeType) ? nodeType : undefined,
      connectorType ? (connectorType as ConnectorType) : undefined
    ))(hash.split('/'))

  const drawerTitle = nodeType === 'add_input' ? 'Add Input Source' : 'Add Output Destination'
  const direction = nodeType === 'add_input' ? Direction.INPUT : Direction.OUTPUT

  const [sourceCounts, setSourceCounts] = useState<{ [key in ConnectorType]: number }>({
    [ConnectorType.KAFKA_IN]: 0,
    [ConnectorType.KAFKA_OUT]: 0,
    [ConnectorType.URL]: 0,
    [ConnectorType.UNKNOWN]: 0
  })
  const { data, isLoading, isError } = useQuery(PipelineManagerQuery.connector())
  useEffect(() => {
    if (!isLoading && !isError) {
      const counts = data
        .map(s => connectorDescrToType(s))
        .reduce(
          (acc: { [Key in ConnectorType]: number }, typ: ConnectorType) => {
            acc[typ] += 1
            return acc
          },
          {
            [ConnectorType.KAFKA_IN]: 0,
            [ConnectorType.KAFKA_OUT]: 0,
            [ConnectorType.URL]: 0,
            [ConnectorType.UNKNOWN]: 0
          }
        )

      setSourceCounts(counts)
    }
  }, [data, isLoading, isError])

  const openSelectTable = (ioTable: ConnectorType | undefined) => {
    setHash(`${nodeType}/${ioTable ?? ''}`)
  }

  const addConnector = useAddConnector()
  const onAddClick = (connector: ConnectorDescr) => {
    setHash('')
    const ac: AttachedConnector = {
      name: randomString(),
      connector_id: connector.connector_id,
      relation_name: '',
      is_input: nodeType === 'add_input'
    }
    addConnector(connector, ac)
  }

  return (
    <Drawer
      id='connector-drawer' // referenced by webui-tester
      open={!!nodeType}
      anchor='right'
      variant='temporary'
      onClose={() => setHash('')}
      ModalProps={{ keepMounted: true }}
      sx={{ '& .MuiDrawer-paper': { width: { xs: 300, sm: 600 }, backgroundColor: 'background.default' } }}
    >
      <Header>
        <Typography variant='h6'>
          <Breadcrumbs aria-label='breadcrumb' separator={<NavigateNextIcon fontSize='small' />}>
            <Link underline='hover' color='inherit' href={`#${nodeType}`}>
              {drawerTitle}
            </Link>
            {connectorType && <Typography color='text.primary'>{connectorTypeToTitle(connectorType)}</Typography>}
          </Breadcrumbs>
        </Typography>

        <IconButton size='small' href='#' sx={{ color: 'text.primary' }}>
          <Icon icon='bx:x' fontSize={20} />
        </IconButton>
      </Header>
      {!connectorType && (
        <Grid container spacing={3}>
          {[ConnectorType.KAFKA_IN, ConnectorType.KAFKA_OUT, ConnectorType.URL].map(
            type =>
              shouldDisplayConnector(direction, type) && (
                <IoSelectBox
                  key={type}
                  icon={connectorTypeToIcon(type)}
                  howMany={sourceCounts[type]}
                  onNew={type}
                  onSelect={() => openSelectTable(type)}
                />
              )
          )}
        </Grid>
      )}
      {connectorType && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <SelectSourceTable direction={direction} typ={connectorType} onAddClick={onAddClick} />
          </Grid>
        </Grid>
      )}
      {[
        tuple(ConnectorType.KAFKA_IN, KafkaInputConnectorDialog),
        tuple(ConnectorType.KAFKA_OUT, KafkaOutputConnectorDialog),
        tuple(ConnectorType.URL, UrlConnectorDialog)
      ].map(([type, dialog]) => dialog({ show: hash === type, setShow: () => setHash(''), onSuccess: onAddClick }))}
    </Drawer>
  )
}

export default SideBarAddIo
