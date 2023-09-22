import { useInsertDeleteRows } from '$lib/compositions/streaming/inspection/useDeleteRows'
import { PipelineStatus } from '$lib/services/manager'
import { useCallback } from 'react'

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
  props: GridToolbarProps & { pipelineId: string; status: PipelineStatus; relation: string; isReadonly: boolean }
) => {
  const { csvOptions, printOptions, excelOptions } = props
  const gridRef = useGridApiContext()
  const deleteRows = useInsertDeleteRows()
  const onDeleteRows = useCallback(
    (rows: Map<GridRowId, GridValidRowModel>) => {
      deleteRows(
        props.pipelineId,
        props.relation,
        props.status !== PipelineStatus.RUNNING,
        Array.from(rows.values()).map(row => ({ delete: row.record }))
      )
      gridRef.current.setRowSelectionModel([])
    },
    [props.pipelineId, props.relation, props.status, deleteRows, gridRef]
  )
  return (
    <GridToolbarContainer>
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
    </GridToolbarContainer>
  )
}
