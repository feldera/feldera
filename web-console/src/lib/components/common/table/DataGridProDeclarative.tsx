import { reorderElement, replaceElement } from '$lib/functions/common/array'
import { fullJoin } from 'array-join'

import { DataGridPro, DataGridProProps, GridColDef, GridValidRowModel } from '@mui/x-data-grid-pro'

export type DataGridColumnViewModel = { field: string; width?: number; flex?: number }[]

export type DataGridProDeclarativeProps<Model extends GridValidRowModel = any> = Omit<
  DataGridProProps<Model>,
  'onFilterModelChange' | 'onColumnVisibilityModelChange'
> & {
  columnViewModel?: DataGridColumnViewModel
  setColumnViewModel?: (model: NonNullable<DataGridProDeclarativeProps<Model>['columnViewModel']>) => void
  setColumnVisibilityModel?: DataGridProProps<Model>['onColumnVisibilityModelChange']
  setFilterModel?: DataGridProProps<Model>['onFilterModelChange']
}

const deriveColumnViewModel = <Model extends GridValidRowModel>(
  cols: GridColDef<Model>[],
  viewModel: DataGridColumnViewModel
) =>
  viewModel.length === cols.length // check if columnViewModel is applicable to the columns in the table
    ? viewModel
    : cols.map(c => ({ field: c.field, width: c.width, flex: c.flex })) // derive initial columnViewModel from column definition

/**
 * At this time API for DataGridPro is somewhat inconsistent.
 * This wrapper is designed to provide a declarative API
 * for a better separation between Grid data and its presentation model.
 * @param props
 * @returns
 */
export function DataGridProDeclarative<Model extends GridValidRowModel>({
  columnViewModel = [],
  setColumnViewModel = () => {},
  columns: originalColumns,
  ...props
}: DataGridProDeclarativeProps<Model>) {
  columnViewModel = deriveColumnViewModel(originalColumns, columnViewModel)

  // Overwrite values of originalColumns, but preserve the order of columnViewModel
  const columns: GridColDef<Model>[] = fullJoin(
    columnViewModel,
    originalColumns,
    c => c.field,
    c => c.field,
    (v, d) => (d ? [{ ...d, ...(v ?? {}) }] : [])
  ).flat()
  return (
    <DataGridPro
      onColumnOrderChange={change =>
        setColumnViewModel(reorderElement(columnViewModel, change.oldIndex, change.targetIndex))
      }
      onColumnWidthChange={change =>
        setColumnViewModel(
          replaceElement(columnViewModel, e =>
            e.field === change.colDef.field
              ? {
                  ...e,
                  width: change.width,
                  flex: 0 // set flex to 0 to let the change in the width be visible
                }
              : null
          )
        )
      }
      {...{
        onFilterModelChange: props.setFilterModel,
        onColumnVisibilityModelChange: props.setColumnVisibilityModel,
        columns,
        ...props
      }}
    />
  )
}

export { DataGridProDeclarative as DataGridPro, type DataGridProDeclarativeProps as DataGridProProps }
