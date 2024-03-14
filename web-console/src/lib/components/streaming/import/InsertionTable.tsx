// A staging table that can import and generate new data.
//
// The rows can be edited after generation/import and eventually be uploaded to
// the dbsp table.

import { DataGridPro } from '$lib/components/common/table/DataGridProDeclarative'
import { ResetColumnViewButton } from '$lib/components/common/table/ResetColumnViewButton'
import { SQLTypeHeader } from '$lib/components/streaming/inspection/SQLTypeHeader'
import { SQLValueDisplay } from '$lib/components/streaming/inspection/SQLValueDisplay'
import { useDataGridPresentationLocalStorage } from '$lib/compositions/persistence/dataGrid'
import { getDefaultValue } from '$lib/compositions/streaming/import/useDefaultRows'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { caseDependentNameEq, getCaseDependentName, getCaseIndependentName } from '$lib/functions/felderaRelation'
import { ColumnType, Field, PipelineRevision, Relation } from '$lib/services/manager'
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

import type { Row } from '$lib/functions/ddl'

const useInsertionTable = (props: {
  pipeline: Pipeline
  caseIndependentName: string
  insert: { rows: Row[]; setRows: Dispatch<SetStateAction<Row[]>> }
}) => {
  const [pipelineRevision, setPipelineRevision] = useState<PipelineRevision | undefined>(undefined)
  const [relation, setRelation] = useState<Relation | undefined>(undefined)
  const [isPending, setLoading] = useState<boolean>(false)
  const apiRef = useGridApiRef()

  const pipelineManagerQuery = usePipelineManagerQuery()
  const pipelineRevisionQuery = useQuery(pipelineManagerQuery.pipelineLastRevision(props.pipeline.descriptor.name))

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
    const tables = program.schema?.inputs.find(caseDependentNameEq(getCaseDependentName(props.caseIndependentName)))
    const views = program.schema?.outputs.find(caseDependentNameEq(getCaseDependentName(props.caseIndependentName)))
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
    props.caseIndependentName,
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
  caseIndependentName: string
  insert: { rows: Row[]; setRows: Dispatch<SetStateAction<Row[]>> }
}) => {
  const { relation, ...data } = useInsertionTable(props)

  if (!relation) {
    return <></>
  }

  return <InsertionTableImpl {...{ relation, ...data }} />
}

const SQLValueInputCell =
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
    key:
      LS_PREFIX + `settings/streaming/insertion/${pipelineRevision.pipeline.name}/${getCaseIndependentName(relation)}`,
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
        columns={[
          ...relation.fields.map((col: Field) => {
            return {
              type: undefined,
              field: getCaseIndependentName(col),
              headerName: getCaseIndependentName(col),
              description: getCaseIndependentName(col),
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
              renderCell: (props: GridRenderCellParams) => (
                <SQLValueDisplay value={props.value} type={col.columntype} />
              ),
              renderEditCell: SQLValueInputCell(col.columntype)
            }
          }),
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
            valueFormatter: (params: GridValueFormatterParams<number>) => {
              return String(params.value) as string | null
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
        ]}
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
