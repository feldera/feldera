// Display a table with all SQL programs.
//
// Can edit name and description directly in the table.
// Also display status of the program.
'use client'

import useStatusNotification from '$lib/components/common/errors/useStatusNotification'
import EntityTable from '$lib/components/common/table/EntityTable'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { useDeleteDialog } from '$lib/compositions/useDialog'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import {
  ApiError,
  ProgramDescr,
  ProgramId,
  ProgramsService,
  ProgramStatus,
  UpdateProgramRequest,
  UpdateProgramResponse
} from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { useCallback, useState } from 'react'
import CustomChip from 'src/@core/components/mui/chip'
import { match, P } from 'ts-pattern'
import IconPencil from '~icons/bx/pencil'
import IconTrashAlt from '~icons/bx/trash-alt'

import { Button, IconButton, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import { GridColDef, GridRenderCellParams, GridToolbarFilterButton, useGridApiRef } from '@mui/x-data-grid-pro'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const getStatusChip = (status: ProgramStatus) =>
  match(status)
    .with({ SqlError: P._ }, () => {
      return { title: 'SQL Error', color: 'error' as const, tooltip: undefined }
    })
    .with({ RustError: P._ }, () => {
      return { title: 'Rust Error', color: 'error' as const, tooltip: undefined }
    })
    .with({ SystemError: P._ }, () => {
      return { title: 'System Error', color: 'error' as const, tooltip: undefined }
    })
    .with('Pending', () => {
      return { title: 'Queued', color: 'primary' as const, tooltip: 'Waiting on another program to finish compilation' }
    })
    .with('CompilingSql', () => {
      return { title: 'Compiling sql', color: 'primary' as const, tooltip: undefined }
    })
    .with('CompilingRust', () => {
      return { title: 'Compiling binary', color: 'primary' as const, tooltip: undefined }
    })
    .with('Success', () => {
      return { title: 'Ready', color: 'success' as const, tooltip: undefined }
    })
    .with('None', () => {
      return { title: 'Unused', color: 'primary' as const, tooltip: undefined }
    })
    .exhaustive()

const TableSqlPrograms = () => {
  const [rows, setRows] = useState<ProgramDescr[]>([])
  const fetchQuery = useQuery(PipelineManagerQuery.programs())
  const { pushMessage } = useStatusNotification()

  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()

  const { showDeleteDialog } = useDeleteDialog()
  // Deleting a row
  const deleteMutation = useMutation<void, ApiError, string>({ mutationFn: ProgramsService.deleteProgram })
  const deleteProject = useCallback(
    (curRow: ProgramDescr) => {
      setTimeout(() => {
        const oldRow = rows.find(row => row.program_id === curRow.program_id)
        if (oldRow !== undefined) {
          deleteMutation.mutate(curRow.program_id, {
            onSuccess: () => {
              setRows(prevRows => prevRows.filter(row => row.program_id !== curRow.program_id))
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
              invalidateQuery(queryClient, PipelineManagerQuery.programs())
            }
          })
        }
      })
    },
    [queryClient, deleteMutation, rows, pushMessage]
  )

  const deleteProgram = showDeleteDialog('Delete', row => `${row.name || 'unnamed'} program`, deleteProject)

  // Table columns
  const columns: GridColDef[] = [
    {
      flex: 0.05,
      minWidth: 50,
      field: 'program_id',
      headerName: 'ID',
      renderCell: (params: GridRenderCellParams) => {
        const { row } = params

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography noWrap variant='body2' sx={{ color: 'text.primary', fontWeight: 600 }}>
                {row.program_id}
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
      field: 'status',
      headerName: 'Status',
      renderCell: (params: GridRenderCellParams) => {
        const status = getStatusChip(params.row.status)
        return (
          <Tooltip title={status.tooltip}>
            <CustomChip rounded size='small' skin='light' color={status.color} label={status.title} />
          </Tooltip>
        )
      }
    },
    {
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      renderCell: (params: GridRenderCellParams<ProgramDescr>) => {
        return (
          <>
            <Tooltip title='Edit'>
              <IconButton size='small' href={`/analytics/editor/?program_id=${params.row.program_id}`}>
                <IconPencil fontSize={20} />
              </IconButton>
            </Tooltip>
            <Tooltip title='Delete'>
              <IconButton size='small' onClick={() => deleteProgram(params.row)}>
                <IconTrashAlt fontSize={20} />
              </IconButton>
            </Tooltip>
          </>
        )
      }
    }
  ]

  // Editing a row
  const mutation = useMutation<
    UpdateProgramResponse,
    ApiError,
    { program_id: ProgramId; request: UpdateProgramRequest }
  >({
    mutationFn: args => ProgramsService.updateProgram(args.program_id, args.request)
  })
  const processRowUpdate = (newRow: ProgramDescr, oldRow: ProgramDescr) => {
    mutation.mutate(
      {
        program_id: newRow.program_id,
        request: { description: newRow.description, name: newRow.name }
      },
      {
        onError: (error: ApiError) => {
          invalidateQuery(queryClient, PipelineManagerQuery.programs())
          invalidateQuery(queryClient, PipelineManagerQuery.programStatus(newRow.program_id))
          pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
          apiRef.current.updateRows([oldRow])
        }
      }
    )

    return newRow
  }

  const defaultColumnVisibility = { program_id: false }
  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + 'settings/analytics/programs/grid',
    defaultColumnVisibility
  })

  const btnAdd = (
    <Button variant='contained' size='small' href='/analytics/editor/' id='btn-add-sql-program' key='0'>
      Add SQL program
    </Button>
  )

  return (
    <Card>
      <EntityTable
        hasSearch
        hasFilter
        // Table properties, passed to the underlying grid-table
        tableProps={{
          getRowId: (row: ProgramDescr) => row.program_id,
          columns,
          rows,
          ...gridPersistence
        }}
        setRows={setRows}
        fetchRows={fetchQuery}
        onUpdateRow={processRowUpdate}
        apiRef={apiRef}
        toolbarChildren={[
          btnAdd,
          <GridToolbarFilterButton key='1' />,
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
  )
}

export default TableSqlPrograms
