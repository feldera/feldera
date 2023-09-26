// The table that displays the connectors.
//
// Table allows to edit the name and description in the table directly and can
// delete/edit individual connectors.
'use client'

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import EntityTable from '$lib/components/common/table/EntityTable'
import { AnyConnectorDialog } from '$lib/components/connectors/dialogs/AnyConnector'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { connectorDescrToType, getStatusObj } from '$lib/functions/connectors'
import {
  ApiError,
  ConnectorDescr,
  ConnectorId,
  ConnectorsService,
  UpdateConnectorRequest,
  UpdateConnectorResponse
} from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { useCallback, useState } from 'react'
import CustomChip from 'src/@core/components/mui/chip'

import { Button } from '@mui/material'
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
  const fetchQuery = useQuery(PipelineManagerQuery.connector())

  // Update row name and description if edited in the cells:
  const mutation = useMutation<
    UpdateConnectorResponse,
    ApiError,
    { connector_id: ConnectorId; request: UpdateConnectorRequest }
  >({
    mutationFn: args => ConnectorsService.updateConnector(args.connector_id, args.request)
  })
  const processRowUpdate = useCallback(
    (newRow: ConnectorDescr, oldRow: ConnectorDescr) => {
      mutation.mutate(
        {
          connector_id: newRow.connector_id,
          request: {
            description: newRow.description,
            name: newRow.name
          }
        },
        {
          onSettled: () => {
            invalidateQuery(queryClient, PipelineManagerQuery.connector())
            invalidateQuery(queryClient, PipelineManagerQuery.connectorStatus(newRow.connector_id))
          },
          onError: (error: ApiError) => {
            pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
            apiRef.current.updateRows([oldRow])
          }
        }
      )

      return newRow
    },
    [apiRef, mutation, pushMessage, queryClient]
  )

  // Delete a connector entry
  const deleteMutation = useMutation<void, ApiError, string>(ConnectorsService.deleteConnector)
  const deleteSource = useCallback(
    (cur_row: ConnectorDescr) => {
      setTimeout(() => {
        const oldRow = rows.find(row => row.connector_id === cur_row.connector_id)
        if (oldRow !== undefined) {
          deleteMutation.mutate(cur_row.connector_id, {
            onSettled: () => {
              invalidateQuery(queryClient, PipelineManagerQuery.connector())
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
      minWidth: 290,
      headerName: 'Name',
      field: 'name',
      editable: true
    },
    {
      flex: 0.5,
      minWidth: 110,
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
      renderCell: (params: GridRenderCellParams) => {
        // Shows the connector type in a chip
        const status = getStatusObj(connectorDescrToType(params.row))
        return <CustomChip rounded size='small' skin='light' color={status.color} label={status.title} />
      }
    }
  ]

  const tableProps = {
    getRowId: (row: ConnectorDescr) => row.connector_id,
    columnVisibilityModel: { connector_id: false },
    columns: columns,
    rows: rows
  }

  const [showDialog, setShowDialog] = useState<boolean>(false)
  const [connector, setConnector] = useState<ConnectorDescr | undefined>(undefined)
  const editConnector = useCallback((cur_row: ConnectorDescr) => {
    setConnector(cur_row)
    setShowDialog(true)
  }, [])

  const btnAdd = (
    <Button variant='contained' size='small' href='/connectors/create/' key='0'>
      Add connector
    </Button>
  )

  return (
    <>
      <Card>
        <EntityTable
          tableProps={tableProps}
          setRows={setRows}
          fetchRows={fetchQuery}
          onUpdateRow={processRowUpdate}
          onDeleteRow={deleteSource}
          onEditClicked={editConnector}
          hasSearch
          hasFilter
          addActions
          toolbarChildren={[
            btnAdd,
            <GridToolbarFilterButton key='1' />,
            <div style={{ marginLeft: 'auto' }} key='2' />
          ]}
          footerChildren={btnAdd}
        />
      </Card>

      {connector && <AnyConnectorDialog show={showDialog} setShow={setShowDialog} connector={connector} />}
    </>
  )
}

export default DataSourceTable
