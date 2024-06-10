'use client'

import { Breadcrumbs } from '$lib/components/common/BreadcrumbsHeader'
import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import { DataGridPro } from '$lib/components/common/table/DataGridProDeclarative'
import DataGridSearch from '$lib/components/common/table/DataGridSearch'
import DataGridToolbar from '$lib/components/common/table/DataGridToolbar'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { ServiceTypeSelectDialog } from '$lib/components/services/dialogs/ServiceTypeSelect'
import { UnknownServiceDialog } from '$lib/components/services/dialogs/UnknownService'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { useHashPart } from '$lib/compositions/useHashPart'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { toLibrdkafkaConfig } from '$lib/functions/kafka/librdkafkaOptions'
import { showOnHashPart } from '$lib/functions/urlHash'
import { ServiceDescr } from '$lib/services/manager'
import { mutationCreateService, mutationDeleteService, mutationUpdateService } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { ServiceType } from '$lib/types/xgressServices'
import { ServiceProps } from '$lib/types/xgressServices/ServiceDialog'
import { useEffect } from 'react'
import invariant from 'tiny-invariant'
import { match, P } from 'ts-pattern'

import { Box, Button, Card, Chip, IconButton } from '@mui/material'
import { GridColDef } from '@mui/x-data-grid-pro'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const getServiceConfigThumb = ({ config }: ServiceDescr) => {
  if ('kafka' in config) {
    const suffix = config.kafka.bootstrap_servers.length > 1 ? ', ...' : ''
    return config.kafka.bootstrap_servers[0] + suffix
  }
  invariant(false, 'getServiceThumb: Unexpected service config type: ' + JSON.stringify(config))
}
const ServiceActions = ({ row: service }: { row: ServiceDescr }) => {
  const { showDeleteDialog } = useDeleteDialog()
  const queryClient = useQueryClient()
  const { mutate: deleteService } = useMutation(mutationDeleteService(queryClient))

  const actions = {
    edit: () => (
      <IconButton className='editButton' size='small' href={`#edit/` + service.name} data-testid='button-edit'>
        <i className={`bx bx-pencil`} style={{ fontSize: 24 }} />
      </IconButton>
    ),
    delete: () => (
      <IconButton
        className='deleteButton'
        size='small'
        onClick={showDeleteDialog('Delete', `${service.name} service`, () =>
          deleteService({ serviceName: service.name })
        )}
        data-testid='button-delete'
      >
        <i className={`bx bx-trash-alt`} style={{ fontSize: 24 }} />
      </IconButton>
    )
  }
  return (
    <Box>
      {actions['edit']()}
      {actions['delete']()}
    </Box>
  )
}

const ServiceList = () => {
  const PipelineManagerQuery = usePipelineManagerQuery()
  const servicesQuery = useQuery(PipelineManagerQuery.listServices())

  const columns: GridColDef<ServiceDescr, string>[] = [
    {
      field: 'type',
      headerName: 'Type',
      display: 'flex',
      valueGetter: (_, row) => {
        return row.config_type
      },
      renderCell: row => {
        return <Chip label={row.row.config_type} sx={{ borderRadius: 1 }} />
      }
    },
    {
      field: 'name',
      headerName: 'Name',
      maxWidth: 180,
      flex: 1,
      display: 'flex'
    },
    {
      field: 'service',
      headerName: 'Service',
      maxWidth: 250,
      flex: 1,
      display: 'flex',
      valueGetter: (_, row) => {
        return getServiceConfigThumb(row)
      }
    },
    {
      field: 'description',
      headerName: 'Description',
      flex: 1,
      display: 'flex'
    },
    {
      field: 'actions',
      headerName: 'Actions',
      width: 120,
      display: 'flex',
      renderCell: ServiceActions
    }
  ]

  const defaultColumnVisibility = { connector_id: false }
  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + 'services/list/grid',
    defaultColumnVisibility
  })

  useEffect(() => {
    if (servicesQuery.isError) {
      throw servicesQuery.error
    }
  }, [servicesQuery.isError, servicesQuery.error])
  return (
    <DataGridPro
      autoHeight
      loading={servicesQuery.isPending}
      rows={servicesQuery.data ?? []}
      getRowId={row => row.service_id}
      columns={columns}
      slots={{
        toolbar: DataGridToolbar
      }}
      slotProps={{
        toolbar: {
          children: [
            <Button variant='contained' size='small' key='0' href='#create'>
              Register a service
            </Button>,
            <ResetColumnViewButton
              key='1'
              setColumnViewModel={gridPersistence.setColumnViewModel}
              setColumnVisibilityModel={() => gridPersistence.setColumnVisibilityModel(defaultColumnVisibility)}
            />,
            <div style={{ marginLeft: 'auto' }} key='2' />,
            <DataGridSearch fetchRows={servicesQuery} setFilteredData={() => {}} key='3' />
          ]
        }
      }}
    />
  )
}

const toNewServiceRequest = (
  serviceType: ServiceType,
  { config: { bootstrap_servers, ...options }, ...props }: ServiceProps
) => ({
  ...props,
  config: {
    [serviceType]: {
      bootstrap_servers: bootstrap_servers,
      options: toLibrdkafkaConfig(options)
    }
  }
})

const toUpdateServiceRequest = toNewServiceRequest

const ServiceDialogs = () => {
  const queryClient = useQueryClient()
  const PipelineManagerQuery = usePipelineManagerQuery()
  const servicesQuery = useQuery(PipelineManagerQuery.listServices())

  const { mutate: createService } = useMutation({
    ...mutationCreateService(queryClient),
    onSuccess(_data, _variables, _context) {
      pushMessage({ message: 'Service successfully created!', key: new Date().getTime(), color: 'success' })
      servicesQuery.refetch()
    },
    onError: error => {
      pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
    }
  })
  const { mutate: updateService } = useMutation({
    ...mutationUpdateService(queryClient),
    onSuccess(_data, _variables, _context) {
      pushMessage({ message: 'Service successfully updated!', key: new Date().getTime(), color: 'success' })
      servicesQuery.refetch()
    },
    onError: error => {
      pushMessage({ message: error.body.message ?? error.body, key: new Date().getTime(), color: 'error' })
    }
  })
  const [hash, setHash] = useHashPart()

  const dialogProps = match(hash)
    .with(P.string.startsWith('create/'), () => {
      const serviceType = hash.match(/create\/(.+)/)?.[1] as ServiceType | undefined
      invariant(serviceType, 'Unexpected serviceType: ' + serviceType)
      return {
        serviceType,
        existingTitle: null,
        onSubmit: (serviceType: ServiceType, form: ServiceProps) => {
          createService(toNewServiceRequest(serviceType, form))
        },
        submitButton: (
          <Button
            type='submit'
            variant='contained'
            color='success'
            endIcon={<i className={`bx bx-check`} style={{}} />}
          >
            Register
          </Button>
        )
      }
    })
    .with(P.string.startsWith('edit/'), () => {
      const serviceNameEncoded = hash.match(/edit\/(.+)/)?.[1]
      invariant(serviceNameEncoded, 'Unexpected serviceName: ' + serviceNameEncoded)
      const serviceName = decodeURI(serviceNameEncoded)
      return {
        serviceName,
        existingTitle: (name: string) => 'Edit ' + name + ' service',
        onSubmit: (serviceType: ServiceType, form: ServiceProps) => {
          updateService({
            serviceName,
            request: toUpdateServiceRequest(serviceType, form)
          })
        },
        submitButton: (
          <Button
            type='submit'
            variant='contained'
            color='success'
            endIcon={<i className={`bx bx-check`} style={{}} />}
          >
            Update
          </Button>
        )
      }
    })
    .otherwise(() => undefined)

  const { pushMessage } = useStatusNotification()

  return (
    <>
      {!!dialogProps && (
        <UnknownServiceDialog {...dialogProps} show={true} setShow={() => setHash('')}></UnknownServiceDialog>
      )}
      <ServiceTypeSelectDialog {...showOnHashPart([hash, setHash])(/create$/)}></ServiceTypeSelectDialog>
    </>
  )
}

export default () => {
  return (
    <>
      <Breadcrumbs.Header>
        <Breadcrumbs.Link href={`/connectors/list`}>Services</Breadcrumbs.Link>
      </Breadcrumbs.Header>
      <Card>
        <ServiceList></ServiceList>
      </Card>
      <ServiceDialogs></ServiceDialogs>
    </>
  )
}
