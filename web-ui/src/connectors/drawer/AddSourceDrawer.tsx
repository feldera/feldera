// The drawer component that opens when the user wants to add a connector in the
// pipeline builder.

import Drawer from '@mui/material/Drawer'
import { styled } from '@mui/material/styles'
import IconButton from '@mui/material/IconButton'
import Typography from '@mui/material/Typography'
import Box, { BoxProps } from '@mui/material/Box'
import NavigateNextIcon from '@mui/icons-material/NavigateNext'

import { Icon } from '@iconify/react'
import useDrawerState from 'src/streaming/builder/hooks/useDrawerState'
import { Breadcrumbs, Button, Card, CardContent, CardHeader, Chip, Grid, Link } from '@mui/material'
import { useState, Dispatch, useEffect } from 'react'
import { AttachedConnector, ConnectorDescr, ConnectorType, Direction } from 'src/types/manager'
import { useQuery } from '@tanstack/react-query'
import { connectorTypeToDirection, connectorTypeToTitle } from 'src/types/connectors'
import { randomString } from 'src/utils'
import { useAddConnector } from 'src/streaming/builder/hooks/useAddIoNode'
import SelectSourceTable from './SelectSourceTable'

import { CsvFileConnectorDialog } from '../dialogs/CsvFileConnector'
import { KafkaInputConnectorDialog } from '../dialogs/KafkaInputConnector'
import { KafkaOutputConnectorDialog } from '../dialogs/KafkaOutputConnector'
import ConnectorDialogProps from '../dialogs/ConnectorDialogProps'

const Header = styled(Box)<BoxProps>(({ theme }) => ({
  display: 'flex',
  alignItems: 'center',
  padding: theme.spacing(3, 4),
  justifyContent: 'space-between',
  backgroundColor: theme.palette.background.paper
}))

interface IoSelectBoxProps {
  icon: string
  howMany: number
  onSuccess: Dispatch<ConnectorDescr>
  openSelectTable: () => void
  closeDrawer: () => void
  dialog: (props: ConnectorDialogProps) => React.ReactNode
}

const IoSelectBox = (props: IoSelectBoxProps) => {
  const [show, setShow] = useState<boolean>(false)

  const openDialog = () => {
    props.closeDrawer()
    setShow(true)
  }
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
            <Button fullWidth sx={{ mb: 1 }} variant='outlined' color='secondary' onClick={openDialog}>
              New
            </Button>
            <Button
              fullWidth
              sx={{ mb: 1 }}
              variant='outlined'
              color='secondary'
              disabled={props.howMany === 0}
              onClick={props.openSelectTable}
            >
              Select&nbsp;
              <Chip sx={{ p: 0, m: 0 }} label={props.howMany} size='small' color={countColor} variant='outlined' />
            </Button>
          </CardContent>
          {props.dialog({ show, setShow, onSuccess: props.onSuccess })}
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
  const [selectTable, setSelectTable] = useState<ConnectorType | undefined>()
  const { isOpen, forNodes, close } = useDrawerState()
  const drawerTitle = forNodes === 'inputNode' ? 'Add Input Source' : 'Add Output Destination'
  const direction = forNodes === 'inputNode' ? Direction.INPUT : Direction.OUTPUT

  const [sourceCounts, setSourceCounts] = useState<{ [key in ConnectorType]: number }>({
    [ConnectorType.KAFKA_IN]: 0,
    [ConnectorType.KAFKA_OUT]: 0,
    [ConnectorType.FILE]: 0
  })
  const { data, isLoading, isError } = useQuery<ConnectorDescr[]>({ queryKey: ['connector'] })
  useEffect(() => {
    if (!isLoading && !isError) {
      const counts = data
        .map(s => s.typ)
        .reduce(
          (acc: { [Key in ConnectorType]: number }, typ: ConnectorType) => {
            acc[typ] += 1
            return acc
          },
          { [ConnectorType.KAFKA_IN]: 0, [ConnectorType.KAFKA_OUT]: 0, [ConnectorType.FILE]: 0 }
        )

      setSourceCounts(counts)
    }
  }, [data, isLoading, isError])

  const closeDrawer = () => {
    close()
    setSelectTable(undefined)
  }
  const openSelectTable = (ioTable: ConnectorType | undefined) => {
    setSelectTable(ioTable)
  }

  const addConnector = useAddConnector()
  const onAddClick = (connector: ConnectorDescr) => {
    closeDrawer()
    const ac: AttachedConnector = {
      uuid: randomString(),
      connector_id: connector.connector_id,
      config: '',
      direction: forNodes === 'inputNode' ? Direction.INPUT : Direction.OUTPUT
    }
    addConnector(connector, ac)
  }

  return (
    <Drawer
      id='connector-drawer' // referenced by webui-tester
      open={isOpen}
      anchor='right'
      variant='temporary'
      onClose={closeDrawer}
      ModalProps={{ keepMounted: true }}
      sx={{ '& .MuiDrawer-paper': { width: { xs: 300, sm: 600 }, backgroundColor: 'background.default' } }}
    >
      <Header>
        <Typography variant='h6'>
          <Breadcrumbs aria-label='breadcrumb' separator={<NavigateNextIcon fontSize='small' />}>
            <Link underline='hover' color='inherit' onClick={() => setSelectTable(undefined)}>
              {drawerTitle}
            </Link>
            {selectTable && <Typography color='text.primary'>{connectorTypeToTitle(selectTable)}</Typography>}
          </Breadcrumbs>
        </Typography>

        <IconButton size='small' onClick={closeDrawer} sx={{ color: 'text.primary' }}>
          <Icon icon='bx:x' fontSize={20} />
        </IconButton>
      </Header>
      {!selectTable && (
        <Grid container spacing={3}>
          {shouldDisplayConnector(direction, ConnectorType.KAFKA_IN) && (
            <IoSelectBox
              dialog={KafkaInputConnectorDialog}
              closeDrawer={close}
              openSelectTable={() => openSelectTable(ConnectorType.KAFKA_IN)}
              howMany={sourceCounts[ConnectorType.KAFKA_IN]}
              onSuccess={onAddClick}
              icon='logos:kafka'
            />
          )}
          {shouldDisplayConnector(direction, ConnectorType.KAFKA_OUT) && (
            <IoSelectBox
              dialog={KafkaOutputConnectorDialog}
              closeDrawer={close}
              openSelectTable={() => openSelectTable(ConnectorType.KAFKA_OUT)}
              howMany={sourceCounts[ConnectorType.KAFKA_OUT]}
              onSuccess={onAddClick}
              icon='logos:kafka'
            />
          )}
          {shouldDisplayConnector(direction, ConnectorType.FILE) && (
            <IoSelectBox
              dialog={CsvFileConnectorDialog}
              closeDrawer={close}
              openSelectTable={() => openSelectTable(ConnectorType.FILE)}
              howMany={sourceCounts[ConnectorType.FILE]}
              onSuccess={onAddClick}
              icon='ph:file-csv'
            />
          )}
        </Grid>
      )}
      {selectTable && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <SelectSourceTable direction={direction} typ={selectTable} onAddClick={onAddClick} />
          </Grid>
        </Grid>
      )}
    </Drawer>
  )
}

export default SideBarAddIo
