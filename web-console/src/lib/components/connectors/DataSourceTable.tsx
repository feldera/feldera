// The table that displays the connectors.
//
// Table allows to edit the name and description in the table directly and can
// delete/edit individual connectors.
'use client'

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import EntityTable from '$lib/components/common/table/EntityTable'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { AnyConnectorDialog } from '$lib/components/connectors/dialogs/AnyConnector'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { connectorDescrToType, getStatusObj } from '$lib/functions/connectors'
import { ApiError, ConnectorDescr, ConnectorsService } from '$lib/services/manager'
import { mutationUpdateConnector, PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { useCallback, useState } from 'react'
import CustomChip from 'src/@core/components/mui/chip'
import IconCheck from '~icons/bx/check'
import IconPencil from '~icons/bx/pencil'
import IconTrashAlt from '~icons/bx/trash-alt'

import { Button, IconButton, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import { GridColDef, GridRenderCellParams, GridToolbarFilterButton, useGridApiRef } from '@mui/x-data-grid-pro'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const DataSourceTable = () => {
  const [rows, setRows] = useState<ConnectorDescr[]>([])
  const { pushMessage } = useStatusNotification()
  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()

  // Query to retrieve table content
  const fetchQuery = useQuery(PipelineManagerQuery.connectors())

  // Update row name and description if edited in the cells:
  const { mutate: updateConnector } = useMutation(mutationUpdateConnector(queryClient))
  const processRowUpdate = useCallback(
    (newRow: ConnectorDescr, oldRow: ConnectorDescr) => {
      updateConnector(
        {
          connectorName: oldRow.name,
          request: {
            description: newRow.description,
            name: newRow.name
          }
        },
        {
          onError: (error: ApiError) => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            apiRef.current.updateRows([oldRow])
          }
        }
      )

      return newRow
    },
    [apiRef, updateConnector, pushMessage]
  )

  // Delete a connector entry
  const { mutate: deleteMutation } = useMutation<void, ApiError, string>({
    mutationFn: ConnectorsService.deleteConnector
  })
  const deleteSource = useCallback(
    (cur_row: ConnectorDescr) => {
      setTimeout(() => {
        const oldRow = rows.find(row => row.connector_id === cur_row.connector_id)
        if (oldRow !== undefined) {
          deleteMutation(cur_row.name, {
            onSettled: () => {
              invalidateQuery(queryClient, PipelineManagerQuery.connectors())
            },
            onSuccess: () => {
              setRows(prevRows => prevRows.filter(row => row.connector_id !== cur_row.connector_id))
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            }
          })
        }
      })
    },
    [queryClient, deleteMutation, rows, pushMessage]
  )

  const [showDialog, setShowDialog] = useState<boolean>(false)
  const [connector, setConnector] = useState<ConnectorDescr | undefined>(undefined)
  const editConnector = useCallback((cur_row: ConnectorDescr) => {
    setConnector(cur_row)
    setShowDialog(true)
  }, [])

  const { showDeleteDialog } = useDeleteDialog()
  const deleteConnector = showDeleteDialog('Delete', row => `${row.name || 'unnamed'} connector`, deleteSource)

  // Definition of the table columns
  const columns: GridColDef[] = [
    {
      flex: 0.05,
      minWidth: 50,
      field: 'connector_id',
      headerName: 'ID',
      renderCell: (params: GridRenderCellParams) => {
        const { row } = params

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography noWrap variant='body2' sx={{ color: 'text.primary', fontWeight: 600 }}>
                {row.connector_id}
              </Typography>
            </Box>
          </Box>
        )
      }
    },
    {
      flex: 0.3,
      minWidth: 150,
      headerName: 'Name',
      field: 'name',
      editable: true
    },
    {
      flex: 0.5,
      field: 'description',
      headerName: 'Description',
      renderCell: (params: GridRenderCellParams) => (
        <Typography variant='body2' sx={{ color: 'text.primary' }}>
          {params.row.description}
        </Typography>
      ),
      editable: true
    },
    {
      flex: 0.15,
      minWidth: 140,
      field: 'typ',
      headerName: 'Type',
      renderCell: (params: GridRenderCellParams<ConnectorDescr>) => {
        // Shows the connector type in a chip
        const status = getStatusObj(connectorDescrToType(params.row.config))
        return <CustomChip rounded size='small' skin='light' color={status.color} label={status.title} />
      }
    },
    {
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      renderCell: (params: GridRenderCellParams<ConnectorDescr>) => {
        return (
          <Box data-testid={'box-connector-actions-' + params.row.name}>
            <Tooltip title='Edit'>
              <IconButton size='small' onClick={() => editConnector(params.row)} data-testid='button-edit'>
                <IconPencil fontSize={20} />
              </IconButton>
            </Tooltip>
            <Tooltip title='Delete'>
              <IconButton size='small' onClick={() => deleteConnector(params.row)} data-testid='button-delete'>
                <IconTrashAlt fontSize={20} />
              </IconButton>
            </Tooltip>
          </Box>
        )
      }
    }
  ]

  const btnAdd = (
    <Button variant='contained' size='small' href='/connectors/create/' key='0' data-testid='button-add-connector'>
      Add connector
    </Button>
  )

  const defaultColumnVisibility = { connector_id: false }
  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + 'settings/connectors/list/grid',
    defaultColumnVisibility
  })

  return (
    <>
      <Card>
        <EntityTable
          tableProps={{
            getRowId: (row: ConnectorDescr) => row.connector_id,
            columns: columns,
            rows: rows,
            ...gridPersistence
          }}
          setRows={setRows}
          fetchRows={fetchQuery}
          onUpdateRow={processRowUpdate}
          hasSearch
          hasFilter
          toolbarChildren={[
            btnAdd,
            <GridToolbarFilterButton key='1' data-testid='button-filter' />,
            <ResetColumnViewButton
              key='2'
              setColumnViewModel={gridPersistence.setColumnViewModel}
              setColumnVisibilityModel={() => gridPersistence.setColumnVisibilityModel(defaultColumnVisibility)}
            />,
            <div style={{ marginLeft: 'auto' }} key='3' />
          ]}
          footerChildren={btnAdd}
        />
      </Card>

      {connector && (
        <AnyConnectorDialog
          show={showDialog}
          setShow={setShowDialog}
          connector={connector}
          existingTitle={name => 'Update ' + name}
          submitButton={
            <Button
              variant='contained'
              color='success'
              endIcon={<IconCheck />}
              type='submit'
              data-testid='button-update'
            >
              Update
            </Button>
          }
        />
      )}
    </>
  )
}

export default DataSourceTable
