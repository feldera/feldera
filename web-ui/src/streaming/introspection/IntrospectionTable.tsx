// Browse the contents of a table or a view.

import Card from '@mui/material/Card'
import { DataGridPro, GridColumns, useGridApiRef } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'
import { useEffect, useState } from 'react'
import { Pipeline, PipelineStatus, OpenAPI, PipelineRevision } from 'src/types/manager'
import useTableUpdater from './hooks/useTableUpdater'

export type IntrospectionTableProps = {
  pipeline: Pipeline | undefined
  name: string | undefined
}

export const IntrospectionTable = ({ pipeline, name }: IntrospectionTableProps) => {
  const apiRef = useGridApiRef()
  const [headers, setHeaders] = useState<GridColumns | undefined>(undefined)

  const pipelineRevisionQuery = useQuery<PipelineRevision>(
    ['pipelineLastRevision', { pipeline_id: pipeline?.descriptor.pipeline_id }],
    {
      enabled: pipeline !== undefined && pipeline.descriptor.program_id !== undefined
    }
  )
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && name) {
      if (pipelineRevisionQuery.data && pipelineRevisionQuery.data.program.schema) {
        const pipelineRevision = pipelineRevisionQuery.data
        const program = pipelineRevision.program
        const tables = program.schema?.inputs.find(v => v.name === name)
        const views = program.schema?.outputs.find(v => v.name === name)
        const relation = tables || views // name is unique in the schema

        if (relation) {
          const id = [{ field: 'genId', headerName: 'genId' }]
          setHeaders(
            id
              .concat(
                relation.fields.map((col: any) => {
                  return { field: col.name, headerName: col.name, flex: 1 }
                })
              )
              .concat([{ field: 'weight', headerName: 'weight' }])
          )
        }
      }
    }
  }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data, setHeaders, name])

  const tableUpdater = useTableUpdater()
  // Stream changes from backend and update the table
  useEffect(() => {
    if (
      pipeline &&
      pipeline.state.current_status == PipelineStatus.RUNNING &&
      name !== undefined &&
      headers !== undefined &&
      apiRef.current
    ) {
      const url = new URL(
        OpenAPI.BASE + '/pipelines/' + pipeline.descriptor.pipeline_id + '/egress/' + name + '?format=csv'
      )
      tableUpdater(url, apiRef, headers)
    }
  }, [pipeline, name, apiRef, headers, tableUpdater])

  return (
    <Card>
      <DataGridPro
        columnVisibilityModel={{ genId: false, weight: false }}
        getRowId={(row: any) => row.genId}
        apiRef={apiRef}
        autoHeight
        columns={headers || []}
        loading={headers === undefined}
        rowThreshold={0}
        rows={[]}
      />
    </Card>
  )
}
