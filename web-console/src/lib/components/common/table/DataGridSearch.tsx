// An input field that can be used to search the table (on the client-side).

import QuickSearch from '$lib/components/common/table/QuickSearch'
import { escapeRegExp } from '$lib/functions/common/string'
import { ChangeEvent, Dispatch, SetStateAction, useState } from 'react'

import { GridValidRowModel } from '@mui/x-data-grid-pro'
import { UseQueryResult } from '@tanstack/react-query'

const DataGridSearch = <TData extends GridValidRowModel>(props: {
  fetchRows: UseQueryResult<TData[], unknown>
  setFilteredData: Dispatch<SetStateAction<TData[]>>
}) => {
  const { isPending, isError, data } = props.fetchRows

  const [searchText, setSearchText] = useState<string>('')

  const handleSearch = (searchValue: string) => {
    setSearchText(searchValue)
    if (searchValue.length == 0) {
      props.setFilteredData([])
      return
    }

    const searchRegex = new RegExp(escapeRegExp(searchValue), 'i')
    if (!isPending && !isError) {
      const filteredRows = data.filter((row: any) => {
        return Object.keys(row).some(field => {
          // @ts-ignore
          if (row[field] !== null) {
            return searchRegex.test(row[field].toString())
          }
        })
      })
      props.setFilteredData(filteredRows)
    }
  }
  return (
    <QuickSearch
      value={searchText}
      clearSearch={() => handleSearch('')}
      onChange={(event: ChangeEvent<HTMLInputElement>) => handleSearch(event.target.value)}
    />
  )
}

export default DataGridSearch
