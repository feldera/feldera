import { useRouter } from 'next/router'
import { useState, useCallback } from 'react'

import Box from '@mui/material/Box'
import Card from '@mui/material/Card'
import Typography from '@mui/material/Typography'
import { GridColumns, GridRenderCellParams, useGridApiRef } from '@mui/x-data-grid-pro'

import CustomChip from 'src/@core/components/mui/chip'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'

import { ProjectService } from 'src/types/manager/services/ProjectService'
import { ProjectDescr } from 'src/types/manager/models/ProjectDescr'

import { match, P } from 'ts-pattern'
import { ProjectStatus } from 'src/types/manager/models/ProjectStatus'
import { CancelError, UpdateProjectRequest, UpdateProjectResponse } from 'src/types/manager'

import EntityTable from 'src/components/table/data-grid/EntityTable'
import useStatusNotification from 'src/components/errors/useStatusNotification'

const getStatusObj = (status: ProjectStatus) =>
  match(status)
    .with({ SqlError: P._ }, () => {
      return { title: 'SQL Error', color: 'error' as const }
    })
    .with({ RustError: P._ }, () => {
      return { title: 'Rust Error', color: 'error' as const }
    })
    .with({ SystemError: P._ }, () => {
      return { title: 'System Error', color: 'error' as const }
    })
    .with('Pending', () => {
      return { title: 'Compiling', color: 'primary' as const }
    })
    .with('CompilingSql', () => {
      return { title: 'Compiling', color: 'primary' as const }
    })
    .with('CompilingRust', () => {
      return { title: 'Building Pipeline', color: 'primary' as const }
    })
    .with('Success', () => {
      return { title: 'Ready', color: 'success' as const }
    })
    .with('None', () => {
      return { title: 'Unused', color: 'primary' as const }
    })
    .exhaustive()

const TableSqlPrograms = () => {
  const [rows, setRows] = useState<ProjectDescr[]>([])
  const fetchQuery = useQuery({ queryKey: ['project'], queryFn: ProjectService.listProjects })
  const { pushMessage } = useStatusNotification()

  const apiRef = useGridApiRef()
  const queryClient = useQueryClient()
  const mutation = useMutation<UpdateProjectResponse, CancelError, UpdateProjectRequest>(ProjectService.updateProject)
  const processRowUpdate = (newRow: ProjectDescr, oldRow: ProjectDescr) => {
    mutation.mutate(
      {
        project_id: newRow.project_id,
        description: newRow.description,
        name: newRow.name
      },
      {
        onError: (error: CancelError) => {
          queryClient.invalidateQueries({ queryKey: ['project'] })
          pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
          apiRef.current.updateRows([oldRow])
        }
      }
    )

    return newRow
  }

  // columns
  const deleteMutation = useMutation<void, CancelError, number>(ProjectService.deleteProject)
  const deleteProject = useCallback(
    (curRow: ProjectDescr) => {
      setTimeout(() => {
        const oldRow = rows.find(row => row.project_id === curRow.project_id)
        if (oldRow !== undefined) {
          deleteMutation.mutate(curRow.project_id, {
            onSuccess: () => {
              setRows(prevRows => prevRows.filter(row => row.project_id !== curRow.project_id))
            },
            onError: error => {
              pushMessage({ message: error.message, key: new Date().getTime(), color: 'error' })
              queryClient.invalidateQueries({ queryKey: ['project'] })
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
      field: 'project_id',
      headerName: 'ID',
      renderCell: (params: GridRenderCellParams) => {
        const { row } = params

        return (
          <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Box sx={{ display: 'flex', flexDirection: 'column' }}>
              <Typography noWrap variant='body2' sx={{ color: 'text.primary', fontWeight: 600 }}>
                {row.project_id}
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
        const status = getStatusObj(params.row.status)

        return <CustomChip rounded size='small' skin='light' color={status.color} label={status.title} />
      }
    }
  ]

  const tableProps = {
    getRowId: (row: ProjectDescr) => row.project_id,
    columnVisibilityModel: { project_id: false },
    columns: columns,
    rows: rows
  }

  const router = useRouter()

  return (
    <Card>
      <EntityTable
        hasSearch
        hasFilter
        addActions
        tableProps={tableProps}
        setRows={setRows}
        fetchRows={fetchQuery}
        onUpdateRow={processRowUpdate}
        onDeleteRow={deleteProject}
        onEditClicked={row => router.push('/analytics/editor/' + row.project_id)}
        apiRef={apiRef}
      />
    </Card>
  )
}

export default TableSqlPrograms
