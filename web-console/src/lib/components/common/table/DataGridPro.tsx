
import { MutableRefObject, useEffect, useState } from 'react'

import { useLocalStorage } from '@mantine/hooks'
import { GridColumnVisibilityModel } from '@mui/x-data-grid-pro'
import { GridApiPro } from '@mui/x-data-grid-pro/models/gridApiPro'
import { GridInitialStatePro } from '@mui/x-data-grid-pro/models/gridStatePro'

export const useGridPersistence = ({
  key,
  defaultColumnVisibility = {},
  apiRef
}: {
  key: string
  defaultColumnVisibility?: GridColumnVisibilityModel
  apiRef: MutableRefObject<GridApiPro>
}) => {

  const [state, setState] = useLocalStorage<GridInitialStatePro>({
    key,
    defaultValue: {
      columns: {
        columnVisibilityModel: defaultColumnVisibility
      }
    }
  })
  const [defaultState, setDefaultState] = useState<GridInitialStatePro>(undefined!)

  console.log('useGridPersistence init', state)
  // useEffect(() => {
  //   console.log('useGridPersistence init', state)
  //   setDefaultState(apiRef.current.exportState())
  //   apiRef.current.restoreState(state)
  // }, [apiRef])

  // const sx = JSON.stringify(state.columns?.columnVisibilityModel)
  // useEffect(() => {
  //   if (!state.columns?.columnVisibilityModel) {
  //     return
  //   }
  //   console.log('useEffect columnVisibilityModel', state)
  //   apiRef.current.restoreState({columns: { columnVisibilityModel: JSON.parse(sx) }})
  // }, [apiRef, sx])

  // const sx = JSON.stringify(state)
  // useEffect(() => {
  //   if (!state.columns?.columnVisibilityModel) {
  //     return
  //   }
  //   console.log('useEffect columnVisibilityModel', state)
  //   apiRef.current.restoreState(JSON.parse(sx))
  // }, [apiRef, sx])

  useEffect(() => {
    console.log('useEffect columnVisibilityModel', state)
    apiRef.current.restoreState(state)
  }, [apiRef, state])

  return {
    onColumnOrderChange: () => {
      console.log('onColumnOrderChange', apiRef.current.exportState())
      setState(apiRef.current.exportState())
    },
    onColumnWidthChange: () => {
      console.log('onColumnWidthChange', apiRef.current.exportState())
      setState(apiRef.current.exportState())
    },
    onFilterModelChange: () => {
      console.log('onFilterModelChange', apiRef.current.exportState())
      setState(apiRef.current.exportState())
    },
    onColumnVisibilityModelChange: (columnVisibilityModel: GridColumnVisibilityModel) => {
      console.log('onColumnVisibilityModelChange', state, columnVisibilityModel)
      // setState(merge(state, {columns: { columnVisibilityModel }}))
      setState(apiRef.current.exportState())
    },
    resetGridViewModel: () => {
      setState(defaultState)
    },
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
