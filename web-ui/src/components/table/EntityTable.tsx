import { ChangeEvent, useState, useEffect, Dispatch, MutableRefObject } from 'react'

import Card from '@mui/material/Card'
import { DataGridPro, GridValidRowModel, DataGridProProps, GridRenderCellParams } from '@mui/x-data-grid-pro'
import { UseQueryResult } from '@tanstack/react-query'
import { GridApiPro } from '@mui/x-data-grid-pro/models/gridApiPro'
import { ErrorOverlay } from './ErrorOverlay'
import { escapeRegExp } from 'src/utils/escapeRegExp'
import Tooltip from '@mui/material/Tooltip'
import IconButton from '@mui/material/IconButton'
import { Icon } from '@iconify/react'

import QuickSearchToolbar from 'src/components/table/QuickSearchToolbar'

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
}

const EntityTable = <TData extends GridValidRowModel>(props: EntityTableProps<TData>) => {
  const { setRows, fetchRows, onUpdateRow, onDeleteRow, onEditClicked, tableProps, addActions } = props

  const [pageSize, setPageSize] = useState<number>(7)
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
                <IconButton size='small' href='#' onClick={() => onEditClicked(params.row)}>
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
  }, [isLoading, isError, data, setRows])

  const handleSearch = (searchValue: string) => {
    setSearchText(searchValue)
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
      if (searchValue.length) {
        setFilteredData(filteredRows)
      } else {
        setFilteredData([])
      }
    }
  }

  return (
    <Card>
      <DataGridPro
        {...tableProps}
        autoHeight
        experimentalFeatures={{ newEditingApi: true }}
        apiRef={props.apiRef}
        components={{
          Toolbar: QuickSearchToolbar,
          ErrorOverlay: ErrorOverlay
        }}
        rows={filteredData.length ? filteredData : tableProps.rows}
        pageSize={pageSize}
        rowsPerPageOptions={[7, 10, 25, 50]}
        onPageSizeChange={newPageSize => setPageSize(newPageSize)}
        processRowUpdate={onUpdateRow}
        error={error}
        loading={isLoading}
        componentsProps={{
          baseButton: {
            variant: 'outlined'
          },
          toolbar: {
            hasSearch: props.hasSearch,
            hasFilter: props.hasFilter,
            value: searchText,
            clearSearch: () => handleSearch(''),
            onChange: (event: ChangeEvent<HTMLInputElement>) => handleSearch(event.target.value)
          },
          errorOverlay: {
            isError: isError,
            error: error
          }
        }}
      />
    </Card>
  )
}

export default EntityTable
