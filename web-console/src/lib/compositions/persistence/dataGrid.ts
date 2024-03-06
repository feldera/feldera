import { DataGridColumnViewModel } from '$lib/components/common/table/DataGridProDeclarative'

import { useLocalStorage } from '@mantine/hooks'
import { GridColumnVisibilityModel, GridFilterModel } from '@mui/x-data-grid-pro'

/**
 * Persist DataGrid's column presentation and applied filters
 */
export function useDataGridPresentationLocalStorage({
  key,
  defaultColumnVisibility = {}
}: {
  key: string
  defaultColumnVisibility?: GridColumnVisibilityModel
}) {
  const [columnVisibilityModel, setColumnVisibilityModel] = useLocalStorage<GridColumnVisibilityModel>({
    key: key + '/visibility',
    defaultValue: defaultColumnVisibility
  })
  const [filterModel, setFilterModel] = useLocalStorage<GridFilterModel>({
    key: key + '/filters',
    defaultValue: {
      items: []
    }
  })

  const [columnViewModel, setColumnViewModel] = useLocalStorage<DataGridColumnViewModel>({
    key: key + '/columnView',
    defaultValue: []
  })
  return {
    columnVisibilityModel,
    setColumnVisibilityModel,
    filterModel,
    setFilterModel,
    columnViewModel,
    setColumnViewModel
  }
}
