import { useInsertDeleteRows } from '$lib/compositions/streaming/inspection/useDeleteRows'
import { useCallback } from 'react'

import {
  GridRowId,
  GridToolbarColumnsButton,
  GridToolbarContainer,
  GridToolbarDensitySelector,
  GridToolbarExport,
  GridToolbarProps,
  GridValidRowModel
} from '@mui/x-data-grid-pro'

import { RowDeleteButton } from './RowDeleteButton'

export const InspectionToolbar = (props: GridToolbarProps & { pipelineId: string; relation: string }) => {
  const { csvOptions, printOptions, excelOptions } = props
  const deleteRows = useInsertDeleteRows()
  const onDeleteRows = useCallback(
    (rows: Map<GridRowId, GridValidRowModel>) => {
      deleteRows(
        props.pipelineId,
        props.relation,
        Array.from(rows.values()).map(row => ({ delete: row.record }))
      )
    },
    [props.pipelineId, props.relation, deleteRows]
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
      <RowDeleteButton onDeleteRows={onDeleteRows}></RowDeleteButton>
    </GridToolbarContainer>
  )
}
