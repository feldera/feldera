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
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { invalidateQuery } from '$lib/functions/common/tanstack'
import { ApiError, ProgramDescr, ProgramsService, ProgramStatus } from '$lib/services/manager'
import { mutationUpdateProgram, PipelineManagerQueryKey } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { useCallback, useState } from 'react'
import { match, P } from 'ts-pattern'

import CustomChip from '@core/components/mui/chip'
import { Button, IconButton, Tooltip } from '@mui/material'
import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import { GridColDef, GridRenderCellParams, GridToolbarFilterButton, useGridApiRef } from '@mui/x-data-grid-pro'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

const getStatusChipProps = (status: ProgramStatus) =>
  match(status)
    .with({ SqlError: P._ }, () => {
      return { label: 'SQL Error', color: 'error' as const, 'data-testid': 'box-status-error', tooltip: undefined }
    })
    .with({ RustError: P._ }, () => {
      return { label: 'Rust Error', color: 'error' as const, 'data-testid': 'box-status-error', tooltip: undefined }
    })
    .with({ SystemError: P._ }, () => {
      return { label: 'System Error', color: 'error' as const, 'data-testid': 'box-status-error', tooltip: undefined }
    })
    .with('Pending', () => {
      return {
        label: 'Queued',
        color: 'primary' as const,
        'data-testid': 'box-status-queued',
        tooltip: 'Waiting on another program to finish compilation'
      }
    })
    .with('CompilingSql', () => {
      return {
        label: 'Compiling sql',
        color: 'primary' as const,
        'data-testid': 'box-status-compiling-sql',
        tooltip: undefined
      }
    })
    .with('CompilingRust', () => {
      return {
        label: 'Compiling bin',
        color: 'primary' as const,
        'data-testid': 'box-status-compiling-binary',
        tooltip: undefined
      }
    })
    .with('Success', () => {
      return { label: 'Ready', color: 'success' as const, 'data-testid': 'box-status-ready', tooltip: undefined }
    })
    .exhaustive()

export const TableSqlPrograms = () => {
  const [rows, setRows] = useState<ProgramDescr[]>([])
  const pipelineManagerQuery = usePipelineManagerQuery()
  const fetchQuery = useQuery({
    ...pipelineManagerQuery.programs(),
    refetchInterval: 2000
  })
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
          deleteMutation.mutate(curRow.name, {
            onSuccess: () => {
              setRows(prevRows => prevRows.filter(row => row.program_id !== curRow.program_id))
            },
            onError: error => {
              pushMessage({ message: error.body.message, key: new Date().getTime(), color: 'error' })
              invalidateQuery(queryClient, PipelineManagerQueryKey.programs())
            }
          })
        }
      })
    },
    [queryClient, deleteMutation, rows, pushMessage]
  )

  const deleteProgram = showDeleteDialog('Delete', row => `${row.name} program`, deleteProject)

  // Table columns
  const columns: GridColDef[] = [
    {
      field: 'program_id'
    },
    {
      flex: 0.3,
      minWidth: 150,
      field: 'name',
      headerName: 'Name',
      display: 'flex',
      renderCell: (params: GridRenderCellParams) => (
        <Typography variant='body2' sx={{ color: 'text.primary' }} data-testid={`box-program-name-${params.row.name}`}>
          {params.row.name}
        </Typography>
      ),
      renderHeader(params) {
        return (
          <Typography
            fontSize={12}
            sx={{ textTransform: 'uppercase', fontWeight: '530' }}
            data-testid={`box-column-header-${params.field}`}
          >
            {params.field}
          </Typography>
        )
      },
      editable: true
    },
    {
      flex: 0.45,
      field: 'description',
      headerName: 'Description',
      display: 'flex',
      renderCell: (params: GridRenderCellParams) => (
        <Typography
          variant='body2'
          sx={{ color: 'text.primary' }}
          data-testid={`box-program-description-${params.row.name}`}
        >
          {params.row.description}
        </Typography>
      ),
      editable: true
    },
    {
      flex: 0.15,
      field: 'status',
      headerName: 'Status',
      display: 'flex',
      renderCell: (params: GridRenderCellParams) => {
        const { tooltip, ...statusChipProps } = getStatusChipProps(params.row.status)
        return (
          <Tooltip title={tooltip}>
            <CustomChip rounded size='small' skin='light' {...statusChipProps} sx={{ width: 120 }} />
          </Tooltip>
        )
      }
    },
    {
      flex: 0.1,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      display: 'flex',
      renderCell: (params: GridRenderCellParams<ProgramDescr>) => {
        return (
          <Box data-testid={'box-program-actions-' + params.row.name}>
            <Tooltip title='Edit'>
              <IconButton
                size='small'
                href={`/analytics/editor/?program_name=${params.row.name}`}
                data-testid='button-edit'
              >
                <i className={`bx bx-pencil`} style={{ fontSize: 24 }} />
              </IconButton>
            </Tooltip>
            <Tooltip title='Delete'>
              <IconButton size='small' onClick={() => deleteProgram(params.row)} data-testid='button-delete'>
                <i className={`bx bx-trash-alt`} style={{ fontSize: 24 }} />
              </IconButton>
            </Tooltip>
          </Box>
        )
      }
    }
  ]

  // Editing a row
  const { mutate: updateProgram } = useMutation(mutationUpdateProgram(queryClient))
  const processRowUpdate = (newRow: ProgramDescr, oldRow: ProgramDescr) => {
    updateProgram(
      {
        programName: oldRow.name,
        update_request: { description: newRow.description, name: newRow.name }
      },
      {
        onError: (error: ApiError) => {
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
    <Button variant='contained' size='small' href='/analytics/editor/' data-testid='button-add-sql-program' key='0'>
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
  )
}
