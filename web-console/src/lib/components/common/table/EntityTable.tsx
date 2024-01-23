// The entity table is a wrapper around the DataGridPro component.
//
// It is used to display a list of entities in a table. It's the generic version
// of a table we use to display programs, pipelines, etc.
'use client'

import { DataGridFooter } from '$lib/components/common/table/DataGridFooter'
import { DataGridPro, DataGridProProps } from '$lib/components/common/table/DataGridProDeclarative'
import DataGridSearch from '$lib/components/common/table/DataGridSearch'
import DataGridToolbar from '$lib/components/common/table/DataGridToolbar'
import { ErrorOverlay } from '$lib/components/common/table/ErrorOverlay'
import { Children, Dispatch, MutableRefObject, ReactNode, useEffect, useState } from 'react'
import { ErrorBoundary } from 'react-error-boundary'

import Card from '@mui/material/Card'
import { GridRow, GridRowProps, GridValidRowModel } from '@mui/x-data-grid-pro'
import { GridApiPro } from '@mui/x-data-grid-pro/models/gridApiPro'
import { UseQueryResult } from '@tanstack/react-query'

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
  onDuplicateClicked?: Dispatch<TData>
  hasSearch?: boolean
  hasFilter?: boolean
  tableProps: DataGridProProps<TData>
  apiRef?: MutableRefObject<GridApiPro>
  toolbarChildren?: ReactNode
  footerChildren?: ReactNode
}

const DataGridRow = (props: GridRowProps) => {
  return <GridRow data-testid={`box-grid-row-${props.row!.name}`} {...props}></GridRow>
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

  const { setRows, fetchRows, onUpdateRow, tableProps } = props

  const [filteredData, setFilteredData] = useState<TData[]>([])

  const { isPending, isError, data, error } = fetchRows

  useEffect(() => {
    if (!isPending && !isError) {
      setRows(data)
    }
    if (isError) {
      throw error
    }
  }, [isPending, isError, data, setRows, error])

  return (
    <Card>
      <DataGridPro
        {...tableProps}
        autoHeight
        apiRef={props.apiRef}
        slots={{
          toolbar: DataGridToolbar,
          footer: DataGridFooter,
          row: DataGridRow
        }}
        rows={filteredData.length ? filteredData : tableProps.rows}
        pageSizeOptions={ROWS_PER_PAGE_OPTIONS}
        paginationModel={paginationModel}
        onPaginationModelChange={setPaginationModel}
        processRowUpdate={onUpdateRow}
        loading={isPending}
        slotProps={{
          baseButton: {
            variant: 'outlined'
          },
          toolbar: {
            children: [
              ...Children.toArray(props.toolbarChildren),
              <DataGridSearch fetchRows={fetchRows} setFilteredData={setFilteredData} key='99' />
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
