import { reorderElement, replaceElement } from '$lib/functions/common/array'
import { fullJoin } from 'array-join'

import { DataGridPro, DataGridProProps, GridColDef, GridValidRowModel } from '@mui/x-data-grid-pro'

export type DataGridColumnViewModel = { field: string; width?: number; flex?: number }[]

export type DataGridProMaxProps<Model extends GridValidRowModel = any> = DataGridProProps<Model> & {
  columnViewModel?: { field: string; width?: number; flex?: number }[]
  setColumnViewModel?: (model: NonNullable<DataGridProMaxProps<Model>['columnViewModel']>) => void
  setColumnVisibilityModel?: DataGridProMaxProps<Model>['onColumnVisibilityModelChange']
  setFilterModel?: DataGridProMaxProps<Model>['onFilterModelChange']
}

/**
 * At this time API for DataGridPro is somewhat inconsistent.
 * This wrapper is designed to provide a declarative API
 * and a better separation between Grid data and its presentation.
 * @param props
 * @returns
 */
function DataGridProMax<Model extends GridValidRowModel>({
  columnViewModel = [],
  setColumnViewModel = () => {},
  ...props
}: DataGridProMaxProps<Model>) {
  columnViewModel =
    columnViewModel.length === props.columns.length
      ? columnViewModel
      : props.columns.map(c => ({ field: c.field, width: c.width, flex: c.flex }))

  // Keeps the order of columnViewModel
  const columns: GridColDef<Model>[] = fullJoin(
    columnViewModel,
    props.columns,
    c => c.field,
    c => c.field,
    (v, d) => (d ? [{ ...d, ...v }] : [])
  ).flat()
  return (
    <DataGridPro
      initialState={{
        columns: {
          orderedFields: columnViewModel.map(c => c.field),
          dimensions: Object.fromEntries(columnViewModel.map(m => [m.field, { width: m.width, flex: m.flex }]))
        }
      }}
      onColumnOrderChange={change =>
        setColumnViewModel(reorderElement(columnViewModel, change.oldIndex, change.targetIndex))
      }
      onColumnWidthChange={change =>
        setColumnViewModel(
          replaceElement(columnViewModel, e => {
            return e.field !== change.colDef.field ? null : { ...e, width: change.width, flex: 0 }
          })
        )
      }
      {...{
        ...props,
        onFilterModelChange: props.setFilterModel,
        onColumnVisibilityModelChange: props.setColumnVisibilityModel,
        columns
      }}
    />
  )
}

export { DataGridProMax as DataGridPro, type DataGridProMaxProps as DataGridProProps }
