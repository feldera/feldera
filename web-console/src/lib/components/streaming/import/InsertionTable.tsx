// A staging table that can import and generate new data.
//
// The rows can be edited after generation/import and eventually be uploaded to
// the dbsp table.

import { DataGridColumnViewModel, DataGridPro } from '$lib/components/common/table/DataGridPro'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { getValueFormatter, Row, sqlTypeToDataGridType } from '$lib/functions/ddl'
import { Field, Pipeline, PipelineRevision, Relation } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import invariant from 'tiny-invariant'

import { useLocalStorage } from '@mantine/hooks'
import Card from '@mui/material/Card'
import {
  GridColumnVisibilityModel,
  GridFilterModel,
  GridPreProcessEditCellProps,
  GridValueFormatterParams,
  GridValueGetterParams,
  GridValueSetterParams,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import ImportToolbar from './ImportToolbar'

export type { Row } from '$lib/functions/ddl'

const useInsertionTable = (props: {
  pipeline: Pipeline
  name: string
  insert: { rows: Row[]; setRows: Dispatch<SetStateAction<Row[]>> }
}) => {
  const [pipelineRevision, setPipelineRevision] = useState<PipelineRevision | undefined>(undefined)
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [isLoading, setLoading] = useState<boolean>(false)
  const apiRef = useGridApiRef()

  const pipelineRevisionQuery = useQuery(
    PipelineManagerQuery.pipelineLastRevision(props.pipeline.descriptor.pipeline_id)
  )

  // If a revision is loaded, find the requested relation that we want to insert
  // data into. We use it to display the table headers etc.
  useEffect(() => {
    if (pipelineRevisionQuery.isLoading || pipelineRevisionQuery.isError || !pipelineRevisionQuery.data) {
      return
    }
    const newPipelineRevision = pipelineRevisionQuery.data
    if (pipelineRevision && pipelineRevision.revision !== newPipelineRevision.revision) {
      props.insert.setRows([])
    }

    const program = newPipelineRevision.program
    const tables = program.schema?.inputs.find(v => v.name === props.name)
    const views = program.schema?.outputs.find(v => v.name === props.name)
    const relation = tables || views // name is unique in the schema
    if (!relation) {
      return
    }
    setPipelineRevision(newPipelineRevision)
    setRelation(relation)
  }, [
    pipelineRevisionQuery.isLoading,
    pipelineRevisionQuery.isError,
    pipelineRevisionQuery.data,
    props.name,
    pipelineRevision,
    props.insert
  ])

  return {
    relation,
    pipelineRevision,
    apiRef,
    isLoading,
    setLoading,
    insert: props.insert
  }
}

export const InsertionTable = (props: {
  pipeline: Pipeline
  name: string
  insert: { rows: Row[]; setRows: Dispatch<SetStateAction<Row[]>> }
}) => {
  const { relation, ...data } = useInsertionTable(props)

  if (!relation) {
    return <></>
  }

  return <InsertionTableImpl {...{ relation, ...data }} />
}

const InsertionTableImpl = ({
  relation,
  pipelineRevision,
  apiRef,
  isLoading,
  setLoading,
  insert
}: ReturnType<typeof useInsertionTable>) => {
  invariant(relation && pipelineRevision)

  const defaultColumnVisibility = {
    genId: false
  }
  const [columnVisibilityModel, setColumnVisibilityModel] = useLocalStorage<GridColumnVisibilityModel>({
    key:
      LS_PREFIX + `settings/streaming/insertion/${pipelineRevision.pipeline.pipeline_id}/${relation.name}/visibility`,
    defaultValue: defaultColumnVisibility
  })
  const [filterModel, setFilterModel] = useLocalStorage<GridFilterModel>({
    key: LS_PREFIX + `settings/streaming/insertion/${pipelineRevision.pipeline.pipeline_id}/${relation.name}/filters`
  })
  const [columnViewModel, setColumnViewModel] = useLocalStorage<DataGridColumnViewModel>({
    key:
      LS_PREFIX + `settings/streaming/inspection/${pipelineRevision.pipeline.pipeline_id}/${relation.name}/columnView`
  })

  return (
    <Card>
      <DataGridPro
        apiRef={apiRef}
        autoHeight
        editMode='cell'
        density='compact'
        loading={isLoading}
        rows={insert.rows}
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
            pipelineRevision,
            setLoading,
            apiRef,
            children: (
              <ResetColumnViewButton
                setColumnViewModel={setColumnViewModel}
                setColumnVisibilityModel={() => setColumnVisibilityModel(defaultColumnVisibility)}
              />
            ),
            ...insert
          }
        }}
        {...{
          columnVisibilityModel,
          setColumnVisibilityModel,
          filterModel,
          setFilterModel,
          columnViewModel,
          setColumnViewModel
        }}
      />
    </Card>
  )
}
