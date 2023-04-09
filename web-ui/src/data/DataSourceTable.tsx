import { useState, useCallback } from 'react'

import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import CustomChip from 'src/@core/components/mui/chip'

import { GridColumns, GridRenderCellParams, useGridApiRef } from '@mui/x-data-grid-pro'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'

import { ConnectorService } from 'src/types/manager/services/ConnectorService'
import { ConnectorDescr } from 'src/types/manager/models/ConnectorDescr'
import { CancelError, UpdateConnectorRequest, UpdateConnectorResponse } from 'src/types/manager'
import EntityTable from 'src/components/table/data-grid/EntityTable'
import useStatusNotification from 'src/components/errors/useStatusNotification'
import { getStatusObj } from 'src/types/data'

const DataSourceTable = () => {
  const [rows, setRows] = useState<ConnectorDescr[]>([])
  const { pushMessage } = useStatusNotification()

  const fetchQuery = useQuery({ queryKey: ['connector'], queryFn: ConnectorService.listConnectors })

  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()
  const mutation = useMutation<UpdateConnectorResponse, CancelError, UpdateConnectorRequest>(
    ConnectorService.updateConnector
  )
  const processRowUpdate = (newRow: ConnectorDescr, oldRow: ConnectorDescr) => {
    mutation.mutate(
      {
        connector_id: newRow.connector_id,
        description: newRow.description,
        name: newRow.name
      },
      {
        onError: (error: CancelError) => {
          pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          queryClient.invalidateQueries({ queryKey: ['connector'] })
          apiRef.current.updateRows([oldRow])
        }
      }
    )

    return newRow
  }

  const deleteMutation = useMutation<void, CancelError, number>(ConnectorService.deleteConnector)
  const deleteSource = useCallback(
    (cur_row: ConnectorDescr) => {
      setTimeout(() => {
        const oldRow = rows.find(row => row.connector_id === cur_row.connector_id)
        if (oldRow !== undefined) {
          deleteMutation.mutate(cur_row.connector_id, {
            onSuccess: () => {
              setRows(prevRows => prevRows.filter(row => row.connector_id !== cur_row.connector_id))
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              queryClient.invalidateQueries({ queryKey: ['connector'] })
            }
          })
        }
      })
    },
    [queryClient, deleteMutation, rows, pushMessage]
  )

  const columns: GridColumns = [
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
        const status = getStatusObj(params.row.typ)

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

  return (
    <Card>
      <EntityTable
        tableProps={tableProps}
        setRows={setRows}
        fetchRows={fetchQuery}
        onUpdateRow={processRowUpdate}
        onDeleteRow={deleteSource}
        hasSearch
        hasFilter
        addActions
      />
    </Card>
  )
}

export default DataSourceTable
