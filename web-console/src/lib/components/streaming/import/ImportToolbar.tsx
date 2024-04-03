import useDefaultRows from '$lib/compositions/streaming/import/useDefaultRows'
import useGenerateRows from '$lib/compositions/streaming/import/useGenerateRows'
import useInsertRows from '$lib/compositions/streaming/import/useInsertRows'
import { usePipelineManagerQuery } from '$lib/compositions/usePipelineManagerQuery'
import { Row } from '$lib/functions/sqlValue'
import { PipelineRevision, Relation } from '$lib/services/manager'
import { LS_PREFIX } from '$lib/types/localStorage'
import { PipelineStatus } from '$lib/types/pipeline'
import { BigNumber } from 'bignumber.js/bignumber.js'
import dayjs from 'dayjs'
import Papa from 'papaparse'
import { ChangeEvent, Dispatch, ReactNode, SetStateAction, useCallback } from 'react'
import { getCaseIndependentName } from 'src/lib/functions/felderaRelation'
import JSONbig from 'true-json-bigint'

import { useLocalStorage } from '@mantine/hooks'
import { Button } from '@mui/material'
import { GridToolbarColumnsButton, GridToolbarContainer, GridToolbarDensitySelector } from '@mui/x-data-grid-pro'
import { useQuery } from '@tanstack/react-query'

import RngSettingsDialog, { StoredFieldSettings } from './RngSettingsDialog'

export type StoreSettingsFn = (
  val:
    | Map<string, StoredFieldSettings>
    | ((prevState: Map<string, StoredFieldSettings>) => Map<string, StoredFieldSettings>)
) => void

const ImportToolbar = ({
  relation,
  rows,
  setRows,
  pipelineRevision,
  setIsBusy,
  children
}: {
  relation: Relation
  rows: Row[]
  setRows: Dispatch<SetStateAction<any[]>>
  pipelineRevision: PipelineRevision
  setIsBusy: Dispatch<SetStateAction<boolean>>
  children?: ReactNode
}) => {
  const pipelineManagerQuery = usePipelineManagerQuery()
  const { data: pipeline } = useQuery(pipelineManagerQuery.pipelineStatus(pipelineRevision.pipeline.name))
  const isRunning = pipeline?.state.current_status === PipelineStatus.RUNNING

  const [settings, setSettings] = useLocalStorage<Map<string, StoredFieldSettings>>({
    key: [LS_PREFIX, 'rng-settings', pipelineRevision.program.program_id, getCaseIndependentName(relation)].join('-'),
    serialize: value => {
      return JSONbig.stringify(Array.from(value.entries()))
    },
    deserialize: localStorageValue => {
      if (!localStorageValue) {
        return new Map()
      }
      const parsed: [string, StoredFieldSettings][] = JSONbig.parse(localStorageValue)
      parsed.map(v => {
        v[1].config.time = dayjs(new Date(v[1].config.time))
        v[1].config.time2 = dayjs(new Date(v[1].config.time2))
        v[1].config.date_range = [
          dayjs(new Date(v[1].config.date_range[0])),
          dayjs(new Date(v[1].config.date_range[1]))
        ]
        for (const field of ['alpha', 'beta', 'lambda', 'mu', 'sigma', 'min', 'max', 'value'] as const) {
          if (field in v[1].config) {
            v[1].config[field] = new BigNumber(v[1].config[field])
          }
        }
      })
      const deserializedMap = new Map(parsed)
      return deserializedMap
    },
    defaultValue: new Map()
  })

  const handleClearData = () => {
    setRows([])
  }

  const insertDefaultRows = useDefaultRows(rows.length, setRows, relation)
  const insertRandomRows = useGenerateRows(rows.length, setRows, relation, settings)

  // Function to handle the CSV file import
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  const handleFileChange = useCallback(
    (event: ChangeEvent<HTMLInputElement>) => {
      setIsBusy(true)
      if (event.target.files && event.target.files.length > 0) {
        const file = event.target.files[0]
        const reader = new FileReader()

        reader.onload = e => {
          if (e.target) {
            const content = e.target.result
            if (typeof content === 'string') {
              const { data: parsedData } = Papa.parse<Record<string, any>>(content.trimEnd(), { header: true })
              const newRows: Row[] = []
              parsedData.forEach((record, index) => {
                const row: Row = { genId: index, weight: 1, record }
                newRows.push(row)
              })
              setIsBusy(false)
              setRows(newRows)
            }
          }
        }
        reader.readAsText(file)
      }
    },
    [setIsBusy, setRows]
  )

  // Sends new rows to the table
  const insertRows = useInsertRows()
  const handleInsertRows = useCallback(() => {
    if (relation && rows.length > 0) {
      insertRows(pipelineRevision.pipeline.name, relation, !isRunning, rows, setRows)
    }
  }, [insertRows, pipelineRevision, isRunning, relation, rows, setRows])

  return relation && pipelineRevision ? (
    <GridToolbarContainer>
      <GridToolbarColumnsButton />
      <GridToolbarDensitySelector />
      <Button size='small' onClick={handleClearData} startIcon={<i className={`bx bx-trash />}`} style={{}} />}>
        Clear
      </Button>
      <RngSettingsDialog relation={relation} settings={settings} setSettings={setSettings} />
      <Button
        size='small'
        onClick={() => insertDefaultRows(1)}
        startIcon={<i className={`bx bx-plus-circle`} style={{}} />}
        data-testid='button-add-1-empty'
      >
        Add Default Row
      </Button>
      <Button
        size='small'
        onClick={() => insertRandomRows(1)}
        startIcon={<i className={`bx bx-plus-circle`} style={{}} />}
        data-testid='button-add-1-random'
      >
        1 Random Row
      </Button>
      <Button
        size='small'
        onClick={() => insertRandomRows(5)}
        startIcon={<i className={`bx bx-duplicate`} style={{}} />}
        data-testid='button-add-5-random'
      >
        5 Random Rows
      </Button>
      <Button
        size='small'
        onClick={handleInsertRows}
        startIcon={<i className={`bx bx-upload`} style={{}} />}
        color='info'
        data-testid='button-insert-rows'
      >
        Insert Rows
      </Button>
      {children}
    </GridToolbarContainer>
  ) : (
    <></>
  )
}

export default ImportToolbar
