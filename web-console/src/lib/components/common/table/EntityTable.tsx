// The entity table is a wrapper around the DataGridPro component.
//
// It is used to display a list of entities in a table. It's the generic version
// of a table we use to display programs, pipelines, etc.

import { ChangeEvent, useState, useEffect, Dispatch, MutableRefObject, ReactNode } from 'react'
import Card from '@mui/material/Card'
import {
  DataGridPro,
  GridValidRowModel,
  DataGridProProps,
  GridRenderCellParams,
  GridToolbarFilterButton
} from '@mui/x-data-grid-pro'
import { UseQueryResult } from '@tanstack/react-query'
import { GridApiPro } from '@mui/x-data-grid-pro/models/gridApiPro'
import { ErrorOverlay } from './ErrorOverlay'
import { escapeRegExp } from '$lib/functions/common/string'
import Tooltip from '@mui/material/Tooltip'
import IconButton from '@mui/material/IconButton'
import { Icon } from '@iconify/react'

import QuickSearch from '$lib/components/common/table/QuickSearch'
import { ErrorBoundary } from 'react-error-boundary'
import DataGridToolbar from './DataGridToolbar'
import { DataGridFooter } from './DataGridFooter'

// This is a workaround for the following issue:
// https://github.com/mui/mui-x/issues/5239
// https://github.com/mui/material-ui/issues/35287#issuecomment-1337250566
declare global {
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace React {
    interface DOMAttributes<T> {
      onResize?: ReactEventHandler<T> | undefined
      onResizeCapture?: ReactEventHandler<T> | undefined
      nonce?: string | undefined
    }
  }
}

export type EntityTableProps<TData extends GridValidRowModel> = {
  setRows: (rows: TData[]) => void
  fetchRows: UseQueryResult<TData[], unknown>
  onUpdateRow?: (newRow: TData, oldRow: TData) => TData
  onDeleteRow?: Dispatch<TData>
  onDuplicateClicked?: Dispatch<TData>
  onEditClicked?: Dispatch<TData>
  hasSearch?: boolean
  hasFilter?: boolean
  addActions?: boolean
  tableProps: DataGridProProps<TData>
  apiRef?: MutableRefObject<GridApiPro>
  footerChildren?: ReactNode
}

const EntityTable = <TData extends GridValidRowModel>(props: EntityTableProps<TData>) => {
  // By default 7 rows are displayed
  const ROWS_DISPLAYED = 7
  // We can choose between these options for the number of rows per page
  const ROWS_PER_PAGE_OPTIONS = [7, 10, 25, 50]
  const [paginationModel, setPaginationModel] = useState({
    pageSize: ROWS_DISPLAYED,
    page: 0
  })

  const { setRows, fetchRows, onUpdateRow, onDeleteRow, onEditClicked, tableProps, addActions } = props

  const [searchText, setSearchText] = useState<string>('')
  const [filteredData, setFilteredData] = useState<TData[]>([])

  const { isLoading, isError, data, error } = fetchRows

  if (addActions) {
    tableProps.columns.push({
      flex: 0.1,
      minWidth: 90,
      sortable: false,
      field: 'actions',
      headerName: 'Actions',
      renderCell: (params: GridRenderCellParams) => {
        return (
          <>
            {onEditClicked && (
              <Tooltip title='Edit'>
                <IconButton
                  size='small'
                  onClick={e => {
                    e.preventDefault()
                    onEditClicked(params.row)
                  }}
                >
                  <Icon icon='bx:pencil' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
            {onDeleteRow && (
              <Tooltip title='Delete'>
                <IconButton size='small' onClick={() => onDeleteRow(params.row)}>
                  <Icon icon='bx:trash-alt' fontSize={20} />
                </IconButton>
              </Tooltip>
            )}
          </>
        )
      }
    })
  }

  useEffect(() => {
    if (!isLoading && !isError) {
      setRows(data)
    }
    if (isError) {
      throw error
    }
  }, [isLoading, isError, data, setRows, error])

  const handleSearch = (searchValue: string) => {
    setSearchText(searchValue)
    if (searchValue.length == 0) {
      setFilteredData([])
      return
    }

    const searchRegex = new RegExp(escapeRegExp(searchValue), 'i')
    if (!isLoading && !isError) {
      const filteredRows = data.filter((row: any) => {
        return Object.keys(row).some(field => {
          // @ts-ignore
          if (row[field] !== null) {
            return searchRegex.test(row[field].toString())
          }
        })
      })
      setFilteredData(filteredRows)
    }
  }

  return (
    <Card>
      <DataGridPro
        {...tableProps}
        autoHeight
        apiRef={props.apiRef}
        slots={{
          toolbar: DataGridToolbar,
          footer: DataGridFooter
        }}
        rows={filteredData.length ? filteredData : tableProps.rows}
        pageSizeOptions={ROWS_PER_PAGE_OPTIONS}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
        processRowUpdate={onUpdateRow}
        loading={isLoading}
        slotProps={{
          baseButton: {
            variant: 'outlined'
          },
          toolbar: {
            children: [
              <GridToolbarFilterButton key='0' />,
              <QuickSearch
                key='1'
                value={searchText}
                clearSearch={() => handleSearch('')}
                onChange={(event: ChangeEvent<HTMLInputElement>) => handleSearch(event.target.value)}
              />
            ]
          },
          footer: {
            children: props.footerChildren
          }
        }}
      />
    </Card>
  )
}

const EntityTableWithErrorBoundary = <TData extends GridValidRowModel>(props: EntityTableProps<TData>) => {
  return (
    <ErrorBoundary FallbackComponent={ErrorOverlay}>
      <EntityTable {...props} />
    </ErrorBoundary>
  )
}

export default EntityTableWithErrorBoundary
