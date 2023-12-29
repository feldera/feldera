// A staging table that can import and generate new data.
//
// The rows can be edited after generation/import and eventually be uploaded to
// the dbsp table.

import { DataGridPro } from '$lib/components/common/table/DataGridProDeclarative'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { SQLTypeHeader } from '$lib/components/streaming/inspection/SQLTypeHeader'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { getDefaultValue } from '$lib/compositions/streaming/import/useDefaultRows'
import { getValueFormatter, Row } from '$lib/functions/ddl'
import { ColumnType, Field, PipelineRevision, Relation } from '$lib/services/manager'
import { PipelineManagerQuery } from '$lib/services/pipelineManagerQuery'
import { LS_PREFIX } from '$lib/types/localStorage'
import { Pipeline } from '$lib/types/pipeline'
import { Dispatch, SetStateAction, useEffect, useState } from 'react'
import invariant from 'tiny-invariant'

import Card from '@mui/material/Card'
import {
  GridPreProcessEditCellProps,
  GridRenderCellParams,
  GridRenderEditCellParams,
  GridValueFormatterParams,
  GridValueGetterParams,
  GridValueSetterParams,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import ImportToolbar from './ImportToolbar'
import { SQLValueInput } from './SQLValueInput'

export type { Row } from '$lib/functions/ddl'

const useInsertionTable = (props: {
  pipeline: Pipeline
  name: string
  insert: { rows: Row[]; setRows: Dispatch<SetStateAction<Row[]>> }
}) => {
  const [pipelineRevision, setPipelineRevision] = useState<PipelineRevision | undefined>(undefined)
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [isPending, setLoading] = useState<boolean>(false)
  const apiRef = useGridApiRef()

  const pipelineRevisionQuery = useQuery(
    PipelineManagerQuery.pipelineLastRevision(props.pipeline.descriptor.pipeline_id)
  )

  // If a revision is loaded, find the requested relation that we want to insert
  // data into. We use it to display the table headers etc.
  useEffect(() => {
    if (pipelineRevisionQuery.isPending || pipelineRevisionQuery.isError || !pipelineRevisionQuery.data) {
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
    pipelineRevisionQuery.isPending,
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
    isPending,
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

const EditSQLCell =
  (ct: ColumnType) =>
  ({ id, field, value, ...props }: GridRenderEditCellParams) => {
    const onChange = (event: React.ChangeEvent<HTMLInputElement>) => {
      props.api.setEditCellValue({ id, field, value: event.target.value })
    }
    return <SQLValueInput columnType={ct} value={value} onChange={onChange} sx={{ width: '100%' }} autoFocus={true} />
  }

const InsertionTableImpl = ({
  relation,
  pipelineRevision,
  apiRef,
  isPending,
  setLoading,
  insert
}: ReturnType<typeof useInsertionTable>) => {
  invariant(relation && pipelineRevision)

  const defaultColumnVisibility = { genId: false }
  const gridPersistence = useDataGridPresentationLocalStorage({
    key: LS_PREFIX + `settings/streaming/insertion/${pipelineRevision.pipeline.pipeline_id}/${relation.name}`,
    defaultColumnVisibility
  })

  return (
    <Card>
      <DataGridPro
        apiRef={apiRef}
        autoHeight
        editMode='cell'
        density='compact'
        loading={isPending}
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
              preProcessEditCellProps: (params: GridPreProcessEditCellProps) => {
                // We'll add support for this once we have JSON format + better
                // errors from backend
                const hasError = false
                return { ...params.props, error: hasError }
              },
              valueGetter: (params: GridValueGetterParams) => {
                return params.row.record[col.name]
              },
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
                row.record[col.name] = params.value !== undefined ? params.value : getDefaultValue(col.columntype)
                return row
              },
              renderHeader: () => <SQLTypeHeader col={col}></SQLTypeHeader>,
              renderCell: (props: GridRenderCellParams) => <>{props.formattedValue}</>,
              renderEditCell: EditSQLCell(col.columntype)
            }
          })
          .concat([
            {
              field: 'genId',
              headerName: 'genId',
              description: 'Index relative to the current set of rows we are adding.',
              flex: 0.5,
              editable: false,
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
              },
              renderHeader: () => <></>,
              renderCell: () => <></>,
              renderEditCell: () => <></>
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
                setColumnViewModel={gridPersistence.setColumnViewModel}
                setColumnVisibilityModel={() => gridPersistence.setColumnVisibilityModel(defaultColumnVisibility)}
              />
            ),
            ...insert
          },
          row: {
            'data-testid': 'box-data-row'
          }
        }}
        {...gridPersistence}
      />
    </Card>
  )
}
