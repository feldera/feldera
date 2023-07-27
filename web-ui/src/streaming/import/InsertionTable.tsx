// A staging table that can import and generate new data.
//
// The rows can be edited after generation/import and eventually be uploaded to
// the dbsp table.

import { useEffect, useState } from 'react'
import Card from '@mui/material/Card'
import {
  DataGridPro,
  GridPreProcessEditCellProps,
  GridValueFormatterParams,
  GridValueGetterParams,
  GridValueSetterParams,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import { Field, Pipeline, PipelineRevision, Relation } from 'src/types/manager'
import { Row, getValueFormatter, sqlTypeToDataGridType } from 'src/types/ddl'
import ImportToolbar from './ImportToolbar'

export type InspectionTableProps = {
  pipeline: Pipeline
  name: string
}

export const InsertionTable = ({ pipeline, name }: InspectionTableProps) => {
  const [pipelineRevision, setPipelineRevision] = useState<PipelineRevision | undefined>(undefined)
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [rows, setRows] = useState<Row[]>([])
  const [isLoading, setLoading] = useState<boolean>(false)
  const apiRef = useGridApiRef()

  const pipelineRevisionQuery = useQuery<PipelineRevision>([
    'pipelineLastRevision',
    { pipeline_id: pipeline?.descriptor.pipeline_id }
  ])

  // If a revision is loaded, find the requested relation that we want to insert
  // data into. We use it to display the table headers etc.
  useEffect(() => {
    if (!pipelineRevisionQuery.isLoading && !pipelineRevisionQuery.isError && pipelineRevisionQuery.data) {
      setRows([])

      const pipelineRevision = pipelineRevisionQuery.data
      const program = pipelineRevision.program
      const tables = program.schema?.inputs.find(v => v.name === name)
      const views = program.schema?.outputs.find(v => v.name === name)
      const relation = tables || views // name is unique in the schema
      if (!relation) {
        return
      }
      setPipelineRevision(pipelineRevision)
      setRelation(relation)
    }
  }, [pipelineRevisionQuery.isLoading, pipelineRevisionQuery.isError, pipelineRevisionQuery.data, name])

  return relation ? (
    <Card>
      <DataGridPro
        apiRef={apiRef}
        autoHeight
        editMode='cell'
        density='compact'
        loading={isLoading}
        rows={rows}
        disableColumnFilter
        initialState={{
          columns: {
            columnVisibilityModel: {
              genId: false
            }
          }
        }}
        getRowId={(row: Row) => row.genId}
        columns={relation.fields
          .map((col: Field) => {
            return {
              field: col.name,
              headerName: col.name,
              description: col.name,
              flex: 1,
              editable: true,
              type: sqlTypeToDataGridType(col),
              preProcessEditCellProps: (params: GridPreProcessEditCellProps) => {
                // We'll add support for this once we have JSON format + better
                // errors from backend
                const hasError = false
                return { ...params.props, error: hasError }
              },
              valueGetter: (params: GridValueGetterParams) => params.row.record[col.name],
              valueFormatter: (params: GridValueFormatterParams<any>) => {
                return getValueFormatter(col.columntype)(params.value)
              },
              valueParser: (value: any) => {
                // It looks like this doesn't do anything -- it just returns the
                // value, but I found without having defined this, if you edit a
                // cell and enter e.g., 'ab' into a number field it will throw
                // an exception, whereas with this it will do the 'right' thing
                // and set it to an empty string...
                return value
              },
              valueSetter: (params: GridValueSetterParams) => {
                const row = params.row
                row.record[col.name] = params.value
                return row
              }
            }
          })
          .concat([
            {
              field: 'genId',
              headerName: 'genId',
              description: 'Index relative to the current set of rows we are adding.',
              flex: 0.5,
              editable: false,
              type: 'number',
              preProcessEditCellProps: (params: GridPreProcessEditCellProps) => {
                const hasError = false
                return { ...params.props, error: hasError }
              },
              valueFormatter: (params: GridValueFormatterParams<any>) => {
                return params.value
              },
              valueParser: (value: any) => {
                return value
              },
              valueGetter: (params: any) => params.row.genId,
              valueSetter: (params: GridValueSetterParams) => {
                return params.row
              }
            }
          ])}
        slots={{
          toolbar: ImportToolbar
        }}
        slotProps={{
          toolbar: {
            relation,
            setRows,
            pipelineRevision,
            setLoading,
            apiRef,
            rows
          }
        }}
        //getCellClassName={(params: GridCellParams) =>
        //validationStatus[params.row.genId]?.length > 0 ? 'invalid-row' : 'invalid-row'
        //  'invalid-row'
        //}
      />
    </Card>
  ) : (
    <></>
  )
}
