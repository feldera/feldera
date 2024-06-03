// The drawer component that opens when the user wants to add a connector in the
// pipeline builder.

import { getConnectorDialogComponent } from '$lib/components/connectors/dialogs/AnyConnector'
import { useAddConnector } from '$lib/compositions/streaming/builder/useAddIoNode'
import { useHashPart } from '$lib/compositions/useHashPart'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { randomString } from '$lib/functions/common/string'
import {
  connectorDescrToType,
  connectorTypeToDirection,
  connectorTypeToLogo,
  connectorTypeToTitle
} from '$lib/functions/connectors'
import { showOnHashPart } from '$lib/functions/urlHash'
import { AttachedConnector, ConnectorDescr } from '$lib/services/manager'
import { ConnectorType, Direction } from '$lib/types/connectors'
import { SVGImport } from '$lib/types/imports'
import Image from 'next/image'
import { useEffect, useState } from 'react'

import NavigateNextIcon from '@mui/icons-material/NavigateNext'
import { Breadcrumbs, Button, Card, CardContent, Chip, Grid, Link } from '@mui/material'
import Box, { BoxProps } from '@mui/material/Box'
import Drawer from '@mui/material/Drawer'
import IconButton from '@mui/material/IconButton'
import { styled, useTheme } from '@mui/material/styles'
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

const IoSelectBox = (props: {
  icon: string | SVGImport
  howMany: number
  newButtonProps: { href: string }
  selectButtonProps: { onClick?: () => void; href?: string }
  'data-testid'?: string
}) => {
  const countColor = props.howMany === 0 ? 'secondary' : 'success'
  const theme = useTheme()
  return (
    <>
      <Grid item xs={6} data-testid={props['data-testid']}>
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
            {typeof props.icon === 'string' ? (
              <Box sx={{ height: 64, position: 'relative', width: '100%' }}>
                <Image
                  src={props.icon}
                  alt={'A connector logo'}
                  fill={true}
                  style={{
                    fill: theme.palette.text.primary
                  }}
                />
              </Box>
            ) : (
              (Icon => (
                <Icon
                  style={{
                    height: 64,
                    objectFit: 'cover',
                    width: '100%',
                    padding: 6,
                    fill: theme.palette.text.primary
                  }}
                ></Icon>
              ))(props.icon)
            )}
            <Button
              fullWidth
              variant='outlined'
              color='secondary'
              LinkComponent={Link}
              {...props.newButtonProps}
              data-testid='button-add-connector'
            >
              New
            </Button>
            <Button
              fullWidth
              variant='outlined'
              color='secondary'
              disabled={props.howMany === 0}
              LinkComponent={Link}
              {...props.selectButtonProps}
              data-testid='button-select-connector'
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

const shouldDisplayConnector = (direction: Direction) => (connectorType: ConnectorType) =>
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
  const pipelineManagerQuery = usePipelineManagerQuery()
  const { data, isPending, isError } = useQuery(pipelineManagerQuery.connectors())
  useEffect(() => {
    if (isPending || isError) {
      return
    }
    const counts = data
      .map(s => connectorDescrToType(s.config))
      .reduce((acc: typeof sourceCounts, typ: ConnectorType) => {
        acc[typ] = (acc[typ] ?? 0) + 1
        return acc
      }, {})

    setSourceCounts(counts)
  }, [data, isPending, isError])

  const addConnector = useAddConnector()
  const onAddClick = (direction: Direction) => (connector: ConnectorDescr) => {
    setHash('')
    const ac: AttachedConnector = {
      name: randomString(),
      connector_name: connector.name,
      relation_name: '',
      is_input: direction === Direction.INPUT
    }
    addConnector(connector, ac)
  }

  return (
    <Drawer
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
                {drawer.connectorType && connectorTypeToTitle(drawer.connectorType).full}
              </Typography>
            </Breadcrumbs>
          )}
        </Typography>

        <IconButton size='small' href='#' sx={{ color: 'text.primary' }}>
          <i className={`bx bx-x`} style={{ fontSize: 24 }} />
        </IconButton>
      </Header>
      {drawer && !drawer.connectorType && (
        <Grid container spacing={6} sx={{ p: 6 }}>
          {[
            ConnectorType.KAFKA_IN,
            ConnectorType.KAFKA_OUT,
            ConnectorType.DEBEZIUM_IN,
            ConnectorType.SNOWFLAKE_OUT,
            ConnectorType.DELTALAKE_IN,
            ConnectorType.DELTALAKE_OUT,
            ConnectorType.S3_IN,
            ConnectorType.URL_IN,
            ConnectorType.UNKNOWN
          ]
            .filter(shouldDisplayConnector(drawer.direction))
            .map(type => (
              <IoSelectBox
                key={type}
                data-testid={'box-connector-' + type}
                icon={connectorTypeToLogo(type)}
                howMany={sourceCounts[type] ?? 0}
                newButtonProps={{ href: `#new/connector/${drawer.direction}/${type}` }}
                selectButtonProps={{ href: `#${drawer!.nodeType}/${type ?? ''}` }}
              />
            ))}
        </Grid>
      )}
      {!!drawer?.connectorType && (
        <Grid container spacing={3}>
          <Grid item xs={12}>
            <SelectSourceTable
              direction={drawer.direction}
              typ={drawer.connectorType}
              onAddClick={onAddClick(drawer.direction)}
            />
          </Grid>
        </Grid>
      )}
      {(() => {
        const res = /new\/connector\/(\w+)\/(\w+)/.exec(hash)
        if (!res) {
          return <></>
        }
        const [, direction, type] = res
        const Dialog = getConnectorDialogComponent(type as ConnectorType)
        return (
          <Dialog
            {...showOnHash('new/connector/')}
            onSuccess={onAddClick(direction as Direction)}
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
        )
      })()}
    </Drawer>
  )
}

export default SideBarAddIo
