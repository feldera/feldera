import { reorderElement, replaceElement } from '$lib/functions/common/array'
import { fullJoin } from 'array-join'
import { MutableRefObject, useEffect, useRef } from 'react'

import { useLocalStorage } from '@mantine/hooks'
import {
  DataGridPro,
  DataGridProProps,
  GridColDef,
  GridColumnOrderChangeParams,
  GridColumnResizeParams,
  GridColumnVisibilityModel,
  GridFilterModel,
  GridValidRowModel,
  useGridApiRef
} from '@mui/x-data-grid-pro'
import { GridApiPro } from '@mui/x-data-grid-pro/models/gridApiPro'

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
            console.log('onColumnWidthChange', change.colDef)
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

function useFirstRender() {
  const ref = useRef(true);
  const firstRender = ref.current;
  ref.current = false;
  return firstRender;
}

export const useGridPersistence = ({path, defaultColumnVisibility = {}, apiRef = useGridApiRef()}: {path: string, defaultColumnVisibility?: GridColumnVisibilityModel, apiRef?: MutableRefObject<GridApiPro>}) => {
  const firstRender =  useFirstRender()
  const [columnVisibilityModel, setColumnVisibilityModel] = useLocalStorage<GridColumnVisibilityModel>({
    key: `${path}/visibility`,
    defaultValue: defaultColumnVisibility
  })
  const [filterModel, setFilterModel] = useLocalStorage<GridFilterModel>({
    key: `${path}/filters`
  })
  const [columnViewModel, setColumnViewModel] = useLocalStorage<DataGridColumnViewModel>({
    key: `${path}/columnView`,
    defaultValue: []
  })

  useEffect(() => {
    console.log('useEffect columnVisibilityModel', JSON.stringify(columnViewModel))
    apiRef.current.restoreState(
      {
        columns: {
          columnVisibilityModel
        }
      }
    )
  }, [apiRef, columnVisibilityModel])

  useEffect(() => {
    console.log('useEffect filterModel', JSON.stringify(columnViewModel))
    apiRef.current.restoreState(
      {
        filter: {
          filterModel
        }
      }
    )
  }, [apiRef, filterModel])

  useEffect(() => {
    if (firstRender) return
    console.log('useEffect columnViewModel', JSON.stringify(columnViewModel))
    const st = {
      columns: {
        orderedFields: columnViewModel.map(c => c.field),
        dimensions: Object.fromEntries(columnViewModel.map(m => [m.field, { width: m.width, flex: m.flex }]))
      }
    }
    console.log('st', JSON.stringify(st))
    apiRef.current.restoreState(st)
  }, [apiRef, columnViewModel])


  return {
    onColumnOrderChange: (change: GridColumnOrderChangeParams) => {
      console.log('onColumnOrderChange')
      setColumnViewModel(columnViewModel)
    },
    onColumnWidthChange: (change: GridColumnResizeParams) =>
      setColumnViewModel(
        replaceElement(columnViewModel, e => {
          return e.field !== change.colDef.field ? null : { ...e, width: change.width, flex: 0 }
        })
      ),
    onFilterModelChange: setFilterModel,
    onColumnVisibilityModelChange: setColumnVisibilityModel,
    setColumnViewModel,
    setColumnVisibilityModel,
    // initialState: {
    //   columns: {
    //     columnVisibilityModel,
    //     orderedFields: columnViewModel.map(c => c.field),
    //     dimensions: Object.fromEntries(columnViewModel.map(m => [m.field, { width: m.width, flex: m.flex }]))
    //   },
    //   filter: {
    //     filterModel
    //   }
    // },
  }
}

export { DataGridProMax as DataGridPro, type DataGridProMaxProps as DataGridProProps }
