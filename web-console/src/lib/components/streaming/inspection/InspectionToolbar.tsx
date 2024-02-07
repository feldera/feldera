import { useInsertDeleteRows } from '$lib/compositions/streaming/inspection/useDeleteRows'
import { Relation } from '$lib/services/manager'
import { PipelineStatus } from '$lib/types/pipeline'
import { ReactNode, useCallback } from 'react'

import {
  GridRowId,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
  GridToolbarExport,
  GridToolbarProps,
  GridValidRowModel,
  useGridApiContext
} from '@mui/x-data-grid-pro'

import { RowDeleteButton } from './RowDeleteButton'

export const InspectionToolbar = (
  props: GridToolbarProps & {
    pipelineName: string
    status: PipelineStatus
    relation: Relation
    isReadonly: boolean
    before: ReactNode
  }
) => {
  const { csvOptions, printOptions, excelOptions } = props
  const gridRef = useGridApiContext()
  const updateRows = useInsertDeleteRows()
  const onDeleteRows = useCallback(
    (rows: Map<GridRowId, GridValidRowModel>) => {
      updateRows(
        props.pipelineName,
        props.relation,
        props.status !== PipelineStatus.RUNNING,
        Array.from(rows.values()).map(row => ({ delete: row.record }))
      )
      gridRef.current.setRowSelectionModel([])
    },
    [props.pipelineName, props.relation, props.status, updateRows, gridRef]
  )
  return (
    <GridToolbarContainer>
      {props.before}
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
      <GridToolbarExport
        {...{
          csvOptions,
          printOptions,
          excelOptions
        }}
      />
      {!props.isReadonly && <RowDeleteButton onDeleteRows={onDeleteRows}></RowDeleteButton>}
      {props.children}
    </GridToolbarContainer>
  )
}
